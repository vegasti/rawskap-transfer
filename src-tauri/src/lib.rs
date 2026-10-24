// Rawskap Transfer v0 (22/8-2026) — NEDLASTINGS-RETNINGEN først:
// hent en hel mappe (m/ undermapper) fra skapet til disk, parallelt, med
// Range-resume per fil. Bytene går portal → 302 → R2 direkte; appen følger
// redirecten selv så cookien IKKE sendes videre til R2.
//
// Auth: en langlivet «kunde_sesjon»-token (app-nøkkel fra portalen) sendes som
// Cookie-header — alle portal-ruter virker uendret.
// En metode som kaller seg selv, er en krasj ved første kall — aldri bare en advarsel. (0.3.0: en
// regex-erstatning gjorde Kanal::framdrift rekursiv, og det var bare advarselen som fanget den.)
#![deny(unconditional_recursion)]
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tauri::{AppHandle, Emitter, State};
use tauri_plugin_shell::ShellExt;
use base64::Engine;
use tokio::io::AsyncWriteExt;
use tokio::sync::Semaphore;
// Nett, opplasting, gjenopptak, nedlasting, kobling, mapper og delinger bor i craten skapflyt
// (crates/skapflyt, S1 24/10). Her er Tauri-kommandoene, køens jobber, ffmpeg og alt lokalt.
use skapflyt::innlesing::{self, Kildefil};
use skapflyt::{bare_ned, bare_opp, enkel_klient, last_ned_med_forsok, last_opp_en, mime_fra, nettfeil, pause_foer_forsok, put_proxy, resume_dir, sikre_mappe, sov, trygt_navn, urlenc, Etterarbeid, Framdrift, Melder, Stopp, Struper};
pub use skapflyt::{Fil, OppFil, SjekkFil};

/// Logg per jobb (valgfritt, Frame.io-stil): én fil i målmappa med start/ferdig
/// per fil + oppsummering. Størrelses-verifisert (ingen hasher lagret ennå).
struct Logg { fil: tokio::sync::Mutex<tokio::fs::File> }
impl Logg {
    async fn skriv(&self, linje: &str) {
        let ts = chrono_lite();
        let mut f = self.fil.lock().await;
        let _ = f.write_all(format!("{ts} {linje}
").as_bytes()).await;
    }
}
fn chrono_lite() -> String {
    // yyyy/mm/dd hh:mm:ss lokal tid uten chrono-avhengighet: bruk std + enkel UTC-offset fra OS er overkill — vi bruker UTC og merker det.
    let d = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
    let (dager, rest) = (d / 86400, d % 86400);
    let (h, m, s) = (rest / 3600, (rest % 3600) / 60, rest % 60);
    // sivil dato fra dagnummer (Howard Hinnant)
    let z = dager as i64 + 719468; let era = z.div_euclid(146097); let doe = z - era * 146097; let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365; let y = yoe + era * 400; let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); let mp = (5 * doy + 2) / 153; let dd = doy - (153 * mp + 2) / 5 + 1; let mm = if mp < 10 { mp + 3 } else { mp - 9 }; let yy = if mm <= 2 { y + 1 } else { y };
    format!("{yy:04}/{mm:02}/{dd:02} {h:02}:{m:02}:{s:02}Z")
}

#[derive(Default)]
pub struct Tilstand {
    avbryt: Arc<AtomicBool>,
    // Enkeltfiler brukeren har stoppet i den AKTIVE jobben (31/8, Vegard:
    // «kan vi avbryte enkeltfiler?»). Køede jobber trenger ikke serveren i
    // det hele tatt — de fjernes bare fra lista før de starter.
    // Nøkkelen er samme id som framdrift-hendelsene bruker: stien for
    // opplasting, asset-id for nedlasting.
    avbrutte: Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
    ned_mbps: Arc<AtomicU64>, // 0 = ubegrenset
    opp_mbps: Arc<AtomicU64>,
}

#[tauri::command]
fn sett_nettverk(tilstand: State<'_, Tilstand>, ned_mbps: u64, opp_mbps: u64) {
    tilstand.ned_mbps.store(ned_mbps, Ordering::Relaxed);
    tilstand.opp_mbps.store(opp_mbps, Ordering::Relaxed);
}

/// User-Agent mot portalen. Appens versjon, ikke cratens — portalloggene skal vise hvilken app som snakker.
const UA: &str = concat!("RawskapTransfer/", env!("CARGO_PKG_VERSION"));

fn klient(nokkel: &str) -> Result<reqwest::Client, String> { skapflyt::klient(nokkel, UA) }

/// Hent mappetre + filer fra portalen.
#[tauri::command]
async fn hent_liste(portal: String, nokkel: String, mappe: String) -> Result<serde_json::Value, String> {
    skapflyt::hent_liste(portal, nokkel, mappe, UA).await
}

/// Last ned et sett filer til en mappe — `parallell` samtidige strømmer.
#[tauri::command]
async fn last_ned(app: AppHandle, tilstand: State<'_, Tilstand>, portal: String, nokkel: String, filer: Vec<Fil>, maal: String, parallell: usize, logg: bool, jobbnavn: String, konflikt: String) -> Result<serde_json::Value, String> {
    let app = App(app);
    tilstand.avbryt.store(false, Ordering::Relaxed);
    let rot0 = PathBuf::from(&maal);
    tokio::fs::create_dir_all(&rot0).await.map_err(|e| format!("{e}"))?;
    let loggen: Option<Arc<Logg>> = if logg {
        let navn = format!("{} {}.log", trygt_navn(&jobbnavn), chrono_lite().replace(['/', ':', ' '], "-").trim_end_matches('Z'));
        match tokio::fs::OpenOptions::new().create(true).append(true).open(rot0.join(&navn)).await {
            Ok(f) => { let l = Arc::new(Logg { fil: tokio::sync::Mutex::new(f) }); l.skriv(&format!("Rawskap Transfer - {} ({})", env!("CARGO_PKG_VERSION"), std::env::consts::OS)).await; l.skriv(&format!("Jobb: {} | {} filer | mål: {}", jobbnavn, filer.len(), maal)).await; l.skriv("").await; Some(l) }
            Err(_) => None,
        }
    } else { None };
    let k = klient(&nokkel)?;
    let bare = bare_ned();
    let rot = PathBuf::from(&maal);
    tokio::fs::create_dir_all(&rot).await.map_err(|e| format!("{e}"))?;
    let sem = Arc::new(Semaphore::new(parallell.clamp(1, 8)));
    let avbryt = tilstand.avbryt.clone();
    // Ny jobb = blanke ark for enkeltfil-avbrudd. Bare den AKTIVE jobbens
    // filer havner her, så det er trygt å tømme når en ny jobb starter —
    // ellers ville en fil man stoppet i går blitt stoppet igjen i dag.
    let avbrutte = tilstand.avbrutte.clone();
    if let Ok(mut s) = avbrutte.lock() { s.clear(); }
    let struper = Struper::ny(tilstand.ned_mbps.clone());
    let mut jobber = Vec::new();
    for fil in filer {
        let (app, k, bare, portal, rot, sem, loggen, struper) = (app.clone(), k.clone(), bare.clone(), portal.clone(), rot.clone(), sem.clone(), loggen.clone(), struper.clone());
        let avbryt = Stopp { global: avbryt.clone(), sett: avbrutte.clone(), id: fil.id.clone() };
        let konflikt = konflikt.clone();
        jobber.push(tokio::spawn(async move {
            let _p = sem.acquire().await;
            let sti = if fil.sti.is_empty() { rot.join(trygt_navn(&fil.filnavn)) } else { rot.join(&fil.sti).join(trygt_navn(&fil.filnavn)) };
            if let Some(l) = &loggen { l.skriv(&format!("🚀 Startet                | ID: {} | {}", fil.id, sti.display())).await; }
            if avbryt.av() { return (fil.id.clone(), Err::<(), String>("Avbrutt".into())); }
            let _ = app.framdrift(Framdrift { id: fil.id.clone(), hentet: 0, total: fil.bytes, status: "laster".into(), feil: None });
            let res = last_ned_med_forsok(&app, &k, &bare, &portal, &fil, &rot, &avbryt, &struper, &konflikt).await;
            if let Err(e) = &res {
                let _ = app.framdrift(Framdrift { id: fil.id.clone(), hentet: 0, total: fil.bytes, status: "feil".into(), feil: Some(e.clone()) });
                if let Some(l) = &loggen { l.skriv(&format!("❌ {:<22} | ID: {} | {}", if e == "Avbrutt" { "Avbrutt" } else { "Feilet" }, fil.id, e)).await; }
            } else if let Some(l) = &loggen { if fil.xxh64.is_empty() { l.skriv(&format!("✅ Ferdig & størrelse ok  | {:>12} B | ID: {} | {}", fil.bytes, fil.id, sti.display())).await; } else { l.skriv(&format!("✅ Ferdig & verifisert   | xxHash: {} | ID: {} | {}", fil.xxh64, fil.id, sti.display())).await; } }
            (fil.id.clone(), res)
        }));
    }
    let mut ok = 0usize; let mut feil = Vec::new();
    for j in jobber {
        match j.await { Ok((_, Ok(()))) => ok += 1, Ok((id, Err(e))) => feil.push(serde_json::json!({ "id": id, "feil": e })), Err(e) => feil.push(serde_json::json!({ "feil": format!("{e}") })) }
    }
    if let Some(l) = &loggen {
        let avbrutt = feil.iter().filter(|f| f["feil"].as_str() == Some("Avbrutt")).count();
        l.skriv("🏁 ===== nedlasting ferdig =====").await;
        l.skriv(&format!("
	Totalt: {}
	Lastet ned: {}
	Feilet: {}
	Avbrutt: {}", ok + feil.len(), ok, feil.len() - avbrutt, avbrutt)).await;
    }
    Ok(serde_json::json!({ "ok": ok, "feil": feil }))
}

/// Maskinnavn til koblingssiden («Koblet til — VEGARD-PC»), ikke «Win32».
#[tauri::command]
fn maskinnavn() -> String {
    std::env::var("COMPUTERNAME").or_else(|_| std::env::var("HOSTNAME")).unwrap_or_else(|_| "denne maskinen".into())
}

/// Device-kobling (webviewen kan ikke fetch-e portalen — CORS): start → kode.
#[tauri::command]
async fn kobling_start(portal: String, maskin: String) -> Result<serde_json::Value, String> {
    skapflyt::kobling_start(portal, maskin).await
}

/// Device-kobling: poll til nøkkelen er godkjent i nettleseren. `hemmelighet` er den lange strengen
/// `kobling_start` fikk tilbake — se skapflyt::kobling_poll.
#[tauri::command]
async fn kobling_poll(portal: String, kode: String, hemmelighet: String) -> Result<serde_json::Value, String> {
    skapflyt::kobling_poll(portal, kode, hemmelighet).await
}

// ── VIDEO via ffmpeg-sidecar (22/8, Vegards valg): poster + scrubbe-sprite +
// varighet/mål — SAMME kontrakt som nettleseren sender i fullfør, så serveren
// er uendret. ffmpeg (LGPL-bygg) ligger som sidecar; ffprobe droppet (115 MB)
// — metadata leses fra `ffmpeg -i` sin stderr.
struct VideoInfo { bredde: u32, hoyde: u32, varighet: f64, poster: Option<String>, sprite: Option<String>, frames: u32 }

fn er_video(navn: &str) -> bool { mime_fra(navn).starts_with("video/") || navn.to_ascii_lowercase().ends_with(".mxf") }

/// Appen sett fra skapflyt: framdriften går ut som Tauri-hendelsen «framdrift», og etterarbeidet
/// før fullfør (avspillingskopi, plakat, stripe) kjøres med ffmpeg-sidecaren.
///
/// Egen type (S1): Rust lar ikke appen implementere cratens traits for Tauris `AppHandle` — begge
/// er fremmede for denne craten.
#[derive(Clone)]
struct App(AppHandle);

impl Melder for App {
    fn framdrift(&self, f: Framdrift) { let _ = self.0.emit("framdrift", f); }
}

/// Det videoløypa trenger av appen i tillegg til framdriften: å kjøre ffmpeg.
///
/// Egen trait (0.3.0) så løypa kan kjøres uten en ekte Tauri-app (se skapflyt::Melder for hvorfor
/// en falsk Tauri-app ikke går). Før S1 het framdrift + ffmpeg sammen `Kanal`; framdriften er nå
/// cratens `Melder`, og bare ffmpeg er igjen her.
trait Kanal: Melder {
    fn ffmpeg(&self, args: &[String]) -> impl std::future::Future<Output = Result<(Vec<u8>, String), String>> + Send;
}
impl Kanal for App {
    fn ffmpeg(&self, args: &[String]) -> impl std::future::Future<Output = Result<(Vec<u8>, String), String>> + Send {
        let cmd = self.0.shell().sidecar("ffmpeg").map(|c| c.args(args));
        async move {
            let cmd = cmd.map_err(|e| format!("ffmpeg mangler: {e}"))?;
            let out = cmd.output().await.map_err(|e| format!("ffmpeg: {e}"))?;
            Ok((out.stdout, String::from_utf8_lossy(&out.stderr).to_string()))
        }
    }
}

/// Etterarbeidet opplastingen i skapflyt kaller før fullfør. Samme løype og rekkefølge som før
/// uttrekket: avspillingskopien, så «ulesbar?» når det ikke ble noen, så (etter «fullfor») plakat,
/// stripe, mål og varighet.
impl Etterarbeid for App {
    fn avspillingskopi(&self, bare: &reqwest::Client, portal: &str, sti: &str, navn: &str, proxy_put: &str, bytes: u64) -> impl std::future::Future<Output = Option<u64>> + Send {
        lag_proxy_hvis_video(self, bare, portal, sti, navn, proxy_put, bytes)
    }

    /// Ingen proxy Å sende? Si HVORFOR når grunnen er at formatet ikke kan leses (ProRes RAW):
    /// serveren dropper da Stream-forsøket, og fila meldes «utenProxy». Proben er billig.
    fn ulesbar_video(&self, sti: &str, navn: &str) -> impl std::future::Future<Output = bool> + Send {
        async move {
            if !er_video(navn) { return false; }
            let (_, e) = ffmpeg_ut(self, &["-hide_banner".into(), "-i".into(), sti.to_string()]).await.unwrap_or_default();
            probe_ubrukelig(&e)
        }
    }

    fn videofelt(&self, sti: &str, navn: &str, bytes: u64, body: &mut serde_json::Value) -> impl std::future::Future<Output = ()> + Send {
        async move {
            if er_video(navn) {
                let _ = self.framdrift(Framdrift { id: sti.to_string(), hentet: bytes, total: bytes, status: "thumbs".into(), feil: None });
                let mut vi = video_info(self, sti).await;
                // Ingen plakat (ProRes RAW: strømmen kan ikke leses)? Den lokale
                // proxyen kan. Bildene hentes derfra; MÅL og VARIGHET beholdes fra
                // originalens container — kortet skal si 8192x4320, ikke 1024x540.
                if vi.poster.is_none() {
                    if let Some(lp) = finn_lokal_proxy(sti) {
                        let vp = video_info(self, &lp).await;
                        vi.poster = vp.poster;
                        vi.sprite = vp.sprite;
                        vi.frames = vp.frames;
                        if vi.varighet <= 0.0 { vi.varighet = vp.varighet; }
                        if vi.bredde == 0 { vi.bredde = vp.bredde; vi.hoyde = vp.hoyde; }
                    }
                }
                if vi.bredde > 0 { body["bredde"] = serde_json::json!(vi.bredde); body["hoyde"] = serde_json::json!(vi.hoyde); }
                if vi.varighet > 0.0 { body["varighet"] = serde_json::json!(vi.varighet); }
                if let Some(p) = vi.poster { body["posterBase64"] = serde_json::json!(p); }
                if let Some(sp) = vi.sprite { body["spriteBase64"] = serde_json::json!(sp); body["spriteFrames"] = serde_json::json!(vi.frames); }
            }
        }
    }
}

async fn ffmpeg_ut<K: Kanal>(app: &K, args: &[String]) -> Result<(Vec<u8>, String), String> { app.ffmpeg(args).await }

fn parse_varighet(stderr: &str) -> f64 {
    if let Some(i) = stderr.find("Duration: ") {
        let t = &stderr[i + 10..]; let t = t.split(',').next().unwrap_or("").trim();
        let d: Vec<f64> = t.split(':').filter_map(|x| x.trim().parse::<f64>().ok()).collect();
        if d.len() == 3 { return d[0] * 3600.0 + d[1] * 60.0 + d[2]; }
    }
    0.0
}
fn parse_dim(stderr: &str) -> (u32, u32) {
    for linje in stderr.lines().filter(|l| l.contains("Video:")) {
        for ord in linje.split(|c: char| c == ' ' || c == ',') {
            if let Some((a, b)) = ord.split_once('x') {
                if let (Ok(w), Ok(h)) = (a.parse::<u32>(), b.parse::<u32>()) { if w >= 16 && h >= 16 { return (w, h); } }
            }
        }
    }
    (0, 0)
}

/// Er den ferdige proxyen faktisk hele klippet — eller bare de første bildene?
///
/// ⚠ 31/8: ffmpeg dekoder DJIs ProRes RAW HQ (aprh) bare ett bilde inn i fila
/// («unspecified pixel format», gjelder både vår nightly og 8.1.1), og
/// AVSLUTTER MED KODE 0. Resultatet er en 25 kB mp4 på 0,08 sekunder — som
/// passerte den gamle vakta på 10 kB og ble lastet opp og merket «klar».
/// En proxy som lyver om at den er klippet er verre enn ingen proxy: da vet
/// serveren i det minste at fila trenger behandling.
/// Fikk ffmpeg i det hele tatt åpnet videostrømmen?
///
/// ⚠ 31/8: på DJIs ProRes RAW gir mov-demukseren bare 2 pakker — men den LESER
/// HELE FILA først (målt: 13,49 av 13,50 GB, 65 s). Appen kjørte fire slike
/// passeringer per klipp — probe, proxy, plakat, sprite — altså ~76 GB lesing
/// over nettverket for å produsere ingenting. Vegards A013C0036 (19 GB) sto
/// to timer i etterbehandling mens de andre opplastingene delte båndbredden.
///
/// Probe-linja sier det rett ut, og den er billig. Sier den dette, hopper vi
/// over ALT lokalt arbeid og lar serveren om fila. Bredde/høyde/varighet
/// leses fortsatt ut — de kommer fra containeren og er riktige.
fn probe_ubrukelig(stderr: &str) -> bool {
    stderr.contains("Could not find codec parameters")
}

async fn proxy_holder<K: Kanal>(app: &K, ut: &str, kilde: f64) -> bool {
    if tokio::fs::metadata(ut).await.map(|m| m.len() <= 10_000).unwrap_or(true) { return false; }
    if kilde <= 0.0 { return true; }  // ukjent kilde — da er størrelsen alt vi har, som før
    let Ok((_, err)) = ffmpeg_ut(app, &["-hide_banner".into(), "-i".into(), ut.into()]).await else { return true };
    // 90 %: et par bilder kan falle av i enden uten at proxyen er ubrukelig.
    parse_varighet(&err) >= kilde * 0.9
}

/// Finn en proxy brukeren allerede har laget for dette klippet (0.2.0, Vegard:
/// «jeg lager ofte proxies lokalt, så det er ofte unødvendig å lage disse på
/// nytt»). Ren filsystem-titting — billig nok til å kalles i hver løype.
/// Mønstrene, i prioritert rekkefølge:
///   1. DJI:      <mappe>/<stamme>.LRF (Mavic legger lavoppløst opptak ved
///                siden av masteren — det ER en proxy)
///   2. Enkelt:   <mappe>/Proxy|Proxies/<stamme>[_Proxy].mov|mp4 (Premiere &
///                håndlagde oppsett)
///   3. Resolve:  <forfar>/Proxy/ProxyMedia/<kildens sti fra volumroten>/
///                <stamme>.mov|mp4 — Resolve SPEILER hele kildestien under
///                ProxyMedia (verifisert mot Vegards Rakettnatt-oppsett 31/8)
fn finn_lokal_proxy(sti: &str) -> Option<String> {
    let p = std::path::Path::new(sti);
    let (dir, stem) = (p.parent()?, p.file_stem()?.to_str()?.to_string());
    let fins = |c: std::path::PathBuf| if c.is_file() { Some(c.to_string_lossy().to_string()) } else { None };
    for e in ["LRF", "lrf"] {
        if let Some(t) = fins(dir.join(format!("{stem}.{e}"))) { return Some(t); }
    }
    let navn: Vec<String> = ["mov", "mp4"].iter()
        .flat_map(|e| [format!("{stem}.{e}"), format!("{stem}_Proxy.{e}")]).collect();
    for m in ["Proxy", "Proxies"] {
        for n in &navn { if let Some(t) = fins(dir.join(m).join(n)) { return Some(t); } }
    }
    // Kildemappa uten stasjonsprefiks/rot — det Resolve speiler under ProxyMedia.
    let rel: std::path::PathBuf = dir.components()
        .filter(|c| matches!(c, std::path::Component::Normal(_))).collect();
    let mut a = Some(dir);
    for _ in 0..8 {
        let Some(base) = a else { break };
        let pm = base.join("Proxy").join("ProxyMedia");
        if pm.is_dir() {
            for n in &navn {
                if let Some(t) = fins(pm.join(&rel).join(n)) { return Some(t); }
                if let Some(t) = fins(pm.join(n)) { return Some(t); }
            }
        }
        a = base.parent();
    }
    None
}

async fn video_info<K: Kanal>(app: &K, sti: &str) -> VideoInfo {
    let mut v = VideoInfo { bredde: 0, hoyde: 0, varighet: 0.0, poster: None, sprite: None, frames: 0 };
    let (_, err) = match ffmpeg_ut(app, &["-hide_banner".into(), "-i".into(), sti.into()]).await { Ok(x) => x, Err(_) => return v };
    v.varighet = parse_varighet(&err);
    let (w, h) = parse_dim(&err); v.bredde = w; v.hoyde = h;
    // Målene og varigheten er gode (de kommer fra containeren) — men å be om
    // bilder ut av en strøm ffmpeg ikke fikk åpnet koster en full gjennomlesing
    // per forsøk og gir ingenting.
    if probe_ubrukelig(&err) { return v; }
    // Poster: midten, men maks 5 s inn (som nettleseren). Maks 1280 bred.
    let t = if v.varighet > 0.0 { (v.varighet / 2.0).min(5.0) } else { 1.0 };
    // Binær ut via stdout ødelegges av shell-pluginens tekstbehandling (22/8:
    // «glitch-poster») — skriv til temp-fil og les den.
    // ⚠ Unikt navn per KALL, ikke per prosess (0.3.0) — se rand_suffiks.
    let tmp = std::env::temp_dir().join(format!("rawskap-poster-{}.jpg", rand_suffiks()));
    if ffmpeg_ut(app, &["-hide_banner".into(), "-loglevel".into(), "error".into(), "-y".into(), "-ss".into(), format!("{t:.2}"), "-i".into(), sti.into(), "-frames:v".into(), "1".into(), "-vf".into(), "scale='min(1280,iw)':-2".into(), "-q:v".into(), "4".into(), tmp.to_string_lossy().to_string()]).await.is_ok() {
        if let Ok(jpg) = tokio::fs::read(&tmp).await { if jpg.len() > 1000 { v.poster = Some(format!("data:image/jpeg;base64,{}", base64::engine::general_purpose::STANDARD.encode(&jpg))); } }
        let _ = tokio::fs::remove_file(&tmp).await;
    }
    // Sprite: N = clamp(round(dur), 12, 48) frames jevnt fordelt, 200 px høye, vannrett stripe.
    if v.varighet > 0.5 {
        let n = (v.varighet.round() as u32).clamp(12, 48);
        let fps = n as f64 / v.varighet;
        let vf = format!("fps={fps:.6},scale=-2:200,tile={n}x1");
        let tmp = std::env::temp_dir().join(format!("rawskap-sprite-{}.jpg", rand_suffiks()));
        if ffmpeg_ut(app, &["-hide_banner".into(), "-loglevel".into(), "error".into(), "-y".into(), "-i".into(), sti.into(), "-vf".into(), vf, "-frames:v".into(), "1".into(), "-q:v".into(), "5".into(), tmp.to_string_lossy().to_string()]).await.is_ok() {
            if let Ok(jpg) = tokio::fs::read(&tmp).await { if jpg.len() > 1000 { v.sprite = Some(format!("data:image/jpeg;base64,{}", base64::engine::general_purpose::STANDARD.encode(&jpg))); v.frames = n; } }
            let _ = tokio::fs::remove_file(&tmp).await;
        }
    }
    v
}

/// Lager en H.264-avspillingsproxy lokalt og laster den opp til lageret.
///
/// HVORFOR HER (30/8, Vegard): uten dette blir en ProRes-master hentet av
/// Cloudflare Stream (som feiler på formatet) og DERETTER lastet ned igjen av
/// Rawcode for lokal enkoding — for en 8K ProRes RAW-fil er det to ganger 8 GB
/// for én klipp. Appen har allerede ffmpeg OG fila på disk; den lager jo
/// plakaten og spriten fra den. Da er det her proxyen hører hjemme.
///
/// Returnerer størrelsen på proxyen, eller None om noe røk — en manglende
/// proxy skal ALDRI velte selve opplastingen. Da faller vi tilbake til den
/// gamle løypa (Stream/Rawcode), som fortsatt virker.
async fn lag_og_last_opp_proxy<K: Kanal>(app: &K, bare: &reqwest::Client, portal: &str, sti: &str, put_url: &str, id: &str, total: u64, kilde_varighet: f64, prov_remux: bool) -> Option<u64> {
    // `sti` ≠ `id` betyr at kilden er en LOKAL proxy (Resolve/LRF) — si det i
    // fasen, så brukeren ser hvorfor dette steget plutselig går på sekunder.
    let lokal = sti != id;
    let _ = app.framdrift(Framdrift { id: id.to_string(), hentet: total, total, status: if lokal { "proxyLokal".into() } else { "proxy".into() }, feil: None });
    let ut = std::env::temp_dir().join(format!("rawskap-proxy-{}.mp4", rand_suffiks()));
    let ut_s = ut.to_string_lossy().to_string();
    // REMUX-VEIEN: kilden er alt H.264 ≤1920 — bare bytt container og legg
    // faststart-flagget. Feiler den (rart lydspor, korrupt fil), faller vi
    // videre til full enkoding under.
    if prov_remux {
        let args: Vec<String> = ["-hide_banner", "-loglevel", "error", "-y", "-i", sti,
            "-c:v", "copy", "-c:a", "aac", "-b:a", "128k", "-movflags", "+faststart", &ut_s]
            .iter().map(|s| s.to_string()).collect();
        if ffmpeg_ut(app, &args).await.is_ok() && proxy_holder(app, &ut_s, kilde_varighet).await {
            let svar = put_proxy(bare, portal, put_url, &ut).await;
            let _ = tokio::fs::remove_file(&ut).await;
            return svar;
        }
        let _ = tokio::fs::remove_file(&ut).await;
    }

    // MASKINVARE-ENKODING FØRST. Å skalere 8K ned til 1080p på CPU tar minutter
    // per klipp; NVENC/VideoToolbox gjør det i sanntid eller raskere. libx264
    // står sist som reservevei — den finnes ALLTID, og maskinvare-enkoderen kan
    // mangle (Mac uten VideoToolbox-støtte for kilden, driverfeil, virtuell
    // maskin). Vi prøver etter tur og tar den første som gir en fil.
    // ⚠ IKKE libx264: den innebygde ffmpeg-en er en LGPL-build, og x264 er GPL —
    // «Unknown encoder 'libx264'» (målt 30/8). Reserveveien er libopenh264, som
    // ER med i LGPL-builder. Den tar ikke -crf, bare bitrate.
    #[cfg(target_os = "macos")]
    let enkodere: &[(&str, &[&str])] = &[
        ("h264_videotoolbox", &["-q:v", "55"]),
        ("libopenh264", &["-b:v", "8M"]),
    ];
    #[cfg(not(target_os = "macos"))]
    let enkodere: &[(&str, &[&str])] = &[
        ("h264_nvenc", &["-preset", "p4", "-rc", "vbr", "-cq", "26", "-b:v", "0"]),
        ("libopenh264", &["-b:v", "8M"]),
    ];

    let mut ok = false;
    for (enk, kvalitet) in enkodere {
        let mut args: Vec<String> = vec![
            "-hide_banner".into(), "-loglevel".into(), "error".into(), "-y".into(),
            "-i".into(), sti.into(),
            // 1080p-tak. `-2` runder høyden til partall — H.264 krever det.
            "-vf".into(), "scale='min(1920,iw)':-2".into(),
            "-c:v".into(), (*enk).into(),
        ];
        args.extend(kvalitet.iter().map(|s| (*s).into()));
        args.extend([
            "-pix_fmt".into(), "yuv420p".into(), "-movflags".into(), "+faststart".into(),
            "-c:a".into(), "aac".into(), "-b:a".into(), "128k".into(),
            ut_s.clone(),
        ]);
        if ffmpeg_ut(app, &args).await.is_ok() && proxy_holder(app, &ut_s, kilde_varighet).await { ok = true; break; }
        let _ = tokio::fs::remove_file(&ut).await;
    }
    if !ok { let _ = tokio::fs::remove_file(&ut).await; return None; }
    let svar = put_proxy(bare, portal, put_url, &ut).await;
    let _ = tokio::fs::remove_file(&ut).await;
    svar
}

/// Unikt suffiks for temp-filer.
///
/// ⚠ Prosess-id alene er IKKE unikt (fikset 0.3.0): opptil seks filer lastes opp samtidig i samme
/// prosess, og to videoer som ble ferdige samtidig skrev plakaten og spriten sin til SAMME temp-fil
/// — med feil plakat på feil video som resultat. Telleren gjør hvert kall unikt.
fn rand_suffiks() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    static TELLER: AtomicU64 = AtomicU64::new(0);
    let ns = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0) % 1_000_000;
    format!("{}-{ns}-{}", std::process::id(), TELLER.fetch_add(1, Ordering::Relaxed))
}

/// Hvilke av disse filene finnes IKKE i nedlastingsmappa? Grunnlaget for «N nye» og «Last ned nye»
/// — reglene (endelig navn = finnes, tomme filer teller ikke) står i skapflyt::mangler_lokalt.
#[tauri::command]
async fn mangler_lokalt(rot: String, filer: Vec<SjekkFil>) -> Result<Vec<String>, String> {
    skapflyt::mangler_lokalt(rot, filer).await
}

/// Bare video, og bare når serveren faktisk tilbød en proxy-URL.
async fn lag_proxy_hvis_video<K: Kanal>(app: &K, bare: &reqwest::Client, portal: &str, sti: &str, navn: &str, put_url: &str, total: u64) -> Option<u64> {
    if put_url.is_empty() || !er_video(navn) { return None; }
    // Original-proben er billig (bare container-hodene) og gir fasiten:
    // varighet + om strømmen i det hele tatt kan leses.
    let (_, e) = ffmpeg_ut(app, &["-hide_banner".into(), "-i".into(), sti.into()]).await.unwrap_or_default();
    let orig_lesbar = !probe_ubrukelig(&e);
    let orig_var = parse_varighet(&e);
    // LOKAL PROXY FØRST: har brukeren alt laget en (Resolve/LRF/Premiere), er
    // det bortkastet å enkode masteren på nytt — og for ProRes RAW er det
    // ENESTE vei til en avspillingskopi i det hele tatt. Kandidaten må dekke
    // klippet (≥90 % av varigheten), ellers later vi som den ikke finnes.
    if let Some(lp) = finn_lokal_proxy(sti) {
        let (_, pe) = ffmpeg_ut(app, &["-hide_banner".into(), "-i".into(), lp.clone()]).await.unwrap_or_default();
        let p_var = parse_varighet(&pe);
        if !probe_ubrukelig(&pe) && (orig_var <= 0.0 || p_var >= orig_var * 0.9) {
            // Alt H.264 i spillbar størrelse? Da holder en remux — sekunder,
            // ikke minutter, og ingen generasjon tapt kvalitet.
            let (w, _) = parse_dim(&pe);
            let remux = pe.contains("Video: h264") && w > 0 && w <= 1920;
            let fasit = if orig_var > 0.0 { orig_var } else { p_var };
            if let Some(n) = lag_og_last_opp_proxy(app, bare, portal, &lp, put_url, sti, total, fasit, remux).await { return Some(n); }
        }
    }
    if !orig_lesbar { return None; }
    lag_og_last_opp_proxy(app, bare, portal, sti, put_url, sti, total, orig_var, false).await
}

#[tauri::command]
async fn last_opp(app: AppHandle, tilstand: State<'_, Tilstand>, portal: String, nokkel: String, filer: Vec<OppFil>, mappe_id: String, parallell: usize) -> Result<serde_json::Value, String> {
    let app = App(app);
    tilstand.avbryt.store(false, Ordering::Relaxed);
    // DJI legger X.LRF (lavoppløst opptak) ved siden av X.MOV — det ER en
    // proxy. Ligger BEGGE i jobben, blir LRF-en avspillingskopi for MOV-en
    // (finn_lokal_proxy finner den i proxy-steget) i stedet for å bli et eget,
    // uspillbart kort i skapet. En jobb med bare LRF-er røres ikke: da har
    // brukeren valgt dem med vilje.
    let video_stammer: std::collections::HashSet<String> = filer.iter()
        .filter(|f| er_video(&f.sti) && !f.sti.to_ascii_lowercase().ends_with(".lrf"))
        .filter_map(|f| std::path::Path::new(&f.sti).with_extension("").to_str().map(|s| s.to_ascii_lowercase()))
        .collect();
    let (som_proxy, filer): (Vec<OppFil>, Vec<OppFil>) = filer.into_iter().partition(|f| {
        f.sti.to_ascii_lowercase().ends_with(".lrf")
            && std::path::Path::new(&f.sti).with_extension("").to_str()
                .map(|s| video_stammer.contains(&s.to_ascii_lowercase())).unwrap_or(false)
    });
    for f in &som_proxy {
        let _ = app.framdrift(Framdrift { id: f.sti.clone(), hentet: f.bytes, total: f.bytes, status: "somProxy".into(), feil: None });
    }
    let portal = portal.trim_end_matches('/').to_string();
    let k = klient(&nokkel)?;
    let bare = bare_opp();
    let cache = Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::<String, String>::new()));
    let sem = Arc::new(Semaphore::new(parallell.clamp(1, 6)));
    let avbryt = tilstand.avbryt.clone();
    let avbrutte = tilstand.avbrutte.clone();
    if let Ok(mut s) = avbrutte.lock() { s.clear(); }
    let struper = Struper::ny(tilstand.opp_mbps.clone());
    let mut jobber = Vec::new();
    for fil in filer {
        let (app, k, bare, portal, cache, sem, mappe_id, struper) = (app.clone(), k.clone(), bare.clone(), portal.clone(), cache.clone(), sem.clone(), mappe_id.clone(), struper.clone());
        let avbryt = Stopp { global: avbryt.clone(), sett: avbrutte.clone(), id: fil.sti.clone() };
        jobber.push(tokio::spawn(async move {
            let _p = sem.acquire().await;
            if avbryt.av() { return (fil.sti.clone(), Err::<(), String>("Avbrutt".into())); }
            let _ = app.framdrift(Framdrift { id: fil.sti.clone(), hentet: 0, total: fil.bytes, status: "laster".into(), feil: None });
            let rel_dir = std::path::Path::new(&fil.relativ).parent().map(|p| p.to_string_lossy().replace('\\', "/")).unwrap_or_default();
            log::info!("opp start: {} ({} B)", fil.relativ, fil.bytes);
            let res = match sikre_mappe(&k, &portal, &mappe_id, &rel_dir, &cache).await {
                Ok(mid) => {
                    // To filforsøk. Hvert har egne omforsøk per del/PUT; dette laget tar det som krever
                    // å begynne på nytt (utløpt signatur på en enkel PUT, sammensetting som feilet).
                    let mut r = Err(String::new());
                    for forsok in 0..2u32 {
                        r = last_opp_en(&app, &k, &bare, &portal, &fil, &mid, &avbryt, struper.clone()).await;
                        match &r {
                            Ok(()) => break,
                            Err(e) if avbryt.av() || e.starts_with("Ikke tilgang") => break,
                            Err(e) => {
                                log::warn!("opp {} filforsøk {} feilet: {e}", fil.relativ, forsok + 1);
                                if forsok == 0 { sov(pause_foer_forsok(1), Some(&avbryt)).await; }
                            }
                        }
                    }
                    r
                }
                Err(e) => Err(e),
            };
            match &res {
                Ok(()) => log::info!("opp ferdig: {}", fil.relativ),
                Err(e) if e != "Avbrutt" => log::warn!("opp feilet: {}: {e}", fil.relativ),
                Err(_) => {}
            }
            if let Err(e) = &res { let _ = app.framdrift(Framdrift { id: fil.sti.clone(), hentet: 0, total: fil.bytes, status: "feil".into(), feil: Some(e.clone()) }); }
            (fil.sti.clone(), res)
        }));
    }
    let mut ok = 0usize; let mut feil = Vec::new();
    for j in jobber { match j.await { Ok((_, Ok(()))) => ok += 1, Ok((id, Err(e))) => feil.push(serde_json::json!({ "id": id, "feil": e })), Err(e) => feil.push(serde_json::json!({ "feil": format!("{e}") })) } }
    Ok(serde_json::json!({ "ok": ok, "feil": feil }))
}

/// Lokale filer under en mappe (rekursivt) → [{sti, relativ, bytes}].
#[tauri::command]
async fn les_mappe(sti: String) -> Result<Vec<OppFil>, String> {
    let rot = PathBuf::from(&sti);
    let mut ut = Vec::new(); let mut stakk = vec![rot.clone()];
    while let Some(d) = stakk.pop() {
        let mut rd = tokio::fs::read_dir(&d).await.map_err(|e| format!("{e}"))?;
        while let Some(e) = rd.next_entry().await.map_err(|e| format!("{e}"))? {
            let p = e.path(); let navn = e.file_name().to_string_lossy().to_string();
            if navn.starts_with('.') || navn.ends_with(".part") || navn.ends_with(".log") { continue; }
            let m = e.metadata().await.map_err(|e| format!("{e}"))?;
            if m.is_dir() { stakk.push(p); } else { ut.push(OppFil { relativ: p.strip_prefix(&rot).map(|r| r.to_string_lossy().replace('\\', "/")).unwrap_or(navn), sti: p.to_string_lossy().to_string(), bytes: m.len() }); }
        }
    }
    ut.sort_by(|a, b| a.relativ.cmp(&b.relativ));
    Ok(ut)
}

/// Ny mappe i skapet (høyreklikk / verktøylinja).
#[tauri::command]
async fn ny_mappe(portal: String, nokkel: String, navn: String, forelder: String) -> Result<serde_json::Value, String> {
    skapflyt::ny_mappe(portal, nokkel, navn, forelder, UA).await
}

/// Legg filer i papirkurven (brukes av «Erstatt» ved opplasting).
#[tauri::command]
async fn slett_filer(portal: String, nokkel: String, ids: Vec<String>) -> Result<(), String> {
    let k = klient(&nokkel)?;
    let r = k.post(format!("{}/api/rawskap/slett", portal.trim_end_matches('/'))).json(&serde_json::json!({ "assetIds": ids })).send().await.map_err(nettfeil)?;
    if !r.status().is_success() { return Err(format!("Kunne ikke slette ({})", r.status())); }
    Ok(())
}

/// «Vis i Utforsker» (23/8): shell.open nekter lokale stier (scope = URL-skjemaer),
/// så vi åpner selv. Fil → Explorer med fila markert; mappe → mappa.
/// Versjonssjekk (25/8): henter manifestet fra media-domenet — Rust-siden
/// fordi webviewen ikke får CORS mot det. Returnerer rå JSON; JS sammenligner.
/// Levende tray-status (0.1.2): JS speiler statuslinja inn i kurv-tooltipen —
/// hold musa over ikonet og se «⤒ 3 aktive · 45 MB/s» uten å åpne vinduet.
#[tauri::command]
fn sett_tray_tekst(app: tauri::AppHandle, tekst: String) {
    if let Some(t) = app.tray_by_id("hoved") {
        let kort: String = tekst.chars().take(120).collect();
        let _ = t.set_tooltip(Some(if kort.is_empty() { "Rawskap Transfer".into() } else { format!("Rawskap Transfer — {kort}") }));
    }
}

// Framdrift på app-ikonet (23/10): docken på Mac, oppgavelinja på Windows. -1 = skjul, -2 = ubestemt, 0–100 = prosent.
#[tauri::command]
fn sett_framdrift(app: tauri::AppHandle, prosent: i64) {
    use tauri::window::{ProgressBarState, ProgressBarStatus};
    use tauri::Manager;
    let Some(w) = app.get_webview_window("main") else { return };
    let (status, progress) = match prosent {
        -1 => (ProgressBarStatus::None, None),
        p if p < 0 => (ProgressBarStatus::Indeterminate, None),
        p => (ProgressBarStatus::Normal, Some(p.min(100) as u64)),
    };
    let _ = w.set_progress_bar(ProgressBarState { status: Some(status), progress });
}

#[tauri::command]
async fn sjekk_versjon() -> Result<String, String> {
    let r = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build().map_err(|e| format!("{e}"))?
        .get("https://rawskap.no/api/transfer/versjon")
        .send().await.map_err(nettfeil)?;
    r.text().await.map_err(nettfeil)
}

/// Alders-rydding av .part (kø 25/8): .part MÅ ligge ved målet (samme volum =
/// gratis rename; cache på C: ville tvunget alt innom systemdisken) — men
/// avbrutte jobber som aldri gjenopptas skal ikke rote til mappa for alltid.
/// Feier ETT nivå av en kjent nedlastingsmappe; sletter kun *.part eldre enn
/// `dager`. Ferske beholdes — de er gjenopptaks-data.
#[tauri::command]
async fn rydd_part_i_mappe(mappe: String, dager: u64) -> u32 {
    let mut slettet = 0u32;
    let grense = std::time::SystemTime::now() - std::time::Duration::from_secs(dager.max(1) * 86_400);
    if let Ok(mut rd) = tokio::fs::read_dir(&mappe).await {
        while let Ok(Some(e)) = rd.next_entry().await {
            let sti = e.path();
            if sti.extension().and_then(|x| x.to_str()) != Some("part") { continue; }
            if let Ok(m) = e.metadata().await {
                if m.is_file() && m.modified().map(|t| t < grense).unwrap_or(false) {
                    if tokio::fs::remove_file(&sti).await.is_ok() { slettet += 1; }
                }
            }
        }
    }
    slettet
}

// OMDØP-MODULEN (0.2.3): regelbasert bulk rename av lokale filer. Alt går via
// temp-navn i to hopp — det gjør case-only-endringer (NTFS ser a.jpg og A.jpg
// som samme fil), bytter og kjeder (A→B mens B→C) til samme enkle sak.
// Validering skjer FØR noe røres; låste filer feiler per fil og resten fortsetter.
#[derive(serde::Deserialize)]
struct OmdoepPar { fra: String, til: String }

#[tauri::command]
async fn omdoep(mappe: String, par: Vec<OmdoepPar>) -> Result<serde_json::Value, String> {
    let rot = std::path::Path::new(&mappe);
    if !rot.is_dir() { return Err("mappa finnes ikke".into()); }
    // Fase 0: valider hele lista før noe som helst røres.
    let mut nye = std::collections::HashSet::new();
    let gamle: std::collections::HashSet<String> = par.iter().map(|p| p.fra.to_lowercase()).collect();
    for p in &par {
        if p.til.is_empty() || p.til.contains(['/', '\\', '<', '>', ':', '"', '|', '?', '*']) || p.til.ends_with(['.', ' ']) {
            return Err(format!("ugyldig navn: «{}»", p.til));
        }
        if !nye.insert(p.til.to_lowercase()) { return Err(format!("to filer får samme navn: «{}»", p.til)); }
        if !rot.join(&p.fra).exists() { return Err(format!("finnes ikke lenger: «{}»", p.fra)); }
        // Målet er opptatt — med mindre det er en av kildene (flyttes vekk i fase 1).
        if rot.join(&p.til).exists() && !gamle.contains(&p.til.to_lowercase()) {
            return Err(format!("«{}» finnes fra før", p.til));
        }
    }
    // Fase 1: kilde → temp. Låst fil = per-fil-feil, resten fortsetter.
    let mut temp: Vec<(usize, std::path::PathBuf)> = Vec::new();
    let mut feil: Vec<(String, String)> = Vec::new();
    for (i, p) in par.iter().enumerate() {
        let t = rot.join(format!("~omd{}_{}", i, rand_suffiks()));
        match std::fs::rename(rot.join(&p.fra), &t) {
            Ok(()) => temp.push((i, t)),
            Err(e) => feil.push((p.fra.clone(), format!("{e}"))),
        }
    }
    // Fase 2: temp → mål. Feiler dette (skal ikke skje etter valideringen),
    // settes fila tilbake til gammelt navn.
    let mut omdopt: Vec<(String, String)> = Vec::new();
    for (i, t) in temp {
        match std::fs::rename(&t, rot.join(&par[i].til)) {
            Ok(()) => omdopt.push((par[i].fra.clone(), par[i].til.clone())),
            Err(e) => { let _ = std::fs::rename(&t, rot.join(&par[i].fra)); feil.push((par[i].fra.clone(), format!("{e}"))); }
        }
    }
    Ok(serde_json::json!({ "omdopt": omdopt, "feil": feil }))
}

#[tauri::command]
fn vis_i_utforsker(sti: String) -> Result<(), String> {
    let p = std::path::Path::new(&sti);
    if !p.exists() { return Err("finnes ikke".into()); }
    #[cfg(windows)]
    {
        // Explorer-fella (kø 25/8): /select tåler verken skråstreker (stien
        // bygges som «mappe/fil» i JS) eller std-quotingen rundt komma-argumentet
        // — begge deler får Explorer til å gi opp og åpne Dokumenter i stedet.
        // Normaliser til backslash og send argumentet RÅTT med egne fnutter.
        use std::os::windows::process::CommandExt;
        let vsti = sti.replace('/', "\\").replace('"', "");
        let mut c = std::process::Command::new("explorer.exe");
        if p.is_dir() { c.raw_arg(format!("\"{vsti}\"")); } else { c.raw_arg(format!("/select,\"{vsti}\"")); }
        c.spawn().map_err(|e| format!("{e}"))?;
    }
    #[cfg(target_os = "macos")]
    {
        let mut c = std::process::Command::new("open");
        if p.is_dir() { c.arg(&sti); } else { c.arg("-R").arg(&sti); }
        c.spawn().map_err(|e| format!("{e}"))?;
    }
    #[cfg(target_os = "linux")]
    { std::process::Command::new("xdg-open").arg(if p.is_dir() { p } else { p.parent().unwrap_or(p) }).spawn().map_err(|e| format!("{e}"))?; }
    Ok(())
}

#[tauri::command]
async fn er_mappe(sti: String) -> bool { tokio::fs::metadata(&sti).await.map(|m| m.is_dir()).unwrap_or(false) }

/// Ledig plass i bytes på disken stien ligger på.
///
/// HVORFOR (19/9, Vegard): han startet en nedlasting mot en disk som var full,
/// og fikk ikke vite det før jobben hadde rotet seg bort i halvskrevne filer.
/// Bekreftelsesmodalen spør om dette FØR den starter, og kan da si «trenger
/// 45 GB, har 12 ledig» i stedet for å feile på fil nummer 300.
///
/// ⚠ MÅLMAPPA FINNES OFTE IKKE ENNÅ — den lages først når jobben starter
/// (`{maal}/{delingsnavn}`). Derfor går vi oppover til nærmeste forelder som
/// faktisk finnes; ellers ville hver eneste sjekk svart med en feil i stedet
/// for tallet vi er ute etter.
#[tauri::command]
fn ledig_plass(sti: String) -> Result<u64, String> {
    let mut p = std::path::PathBuf::from(&sti);
    loop {
        if p.exists() { break; }
        match p.parent() {
            Some(f) if !f.as_os_str().is_empty() => p = f.to_path_buf(),
            _ => return Err(format!("fant ingen eksisterende mappe i «{sti}»")),
        }
    }
    fs2::available_space(&p).map_err(|e| format!("{e}"))
}

// ── SYNK-MAPPER (22/8): en lokal mappe speiles til en mappe i skapet.
// Polling hvert 20. s (virker på NAS/nettverksdisk der fil-hendelser er
// upålitelige). En fil er «ferdig skrevet» når størrelse+mtime er uendret
// mellom to skann. Ferdige (sti|bytes|mtime) huskes per synk i en JSON i
// datamappa, så ingenting lastes opp dobbelt — heller ikke etter omstart.
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Synk { pub id: String, pub lokal: String, pub mappe_id: String, pub navn: String, pub aktiv: bool }

#[derive(Default)]
pub struct SynkTilstand {
    lopere: tokio::sync::Mutex<std::collections::HashMap<String, tokio::task::JoinHandle<()>>>,
    meldt: Arc<tokio::sync::Mutex<std::collections::HashSet<String>>>, // stier som ligger i kø/jobb nå
}

fn synk_ferdig_sti(id: &str) -> PathBuf { resume_dir().join(format!("synk-{}.json", trygt_navn(id))) }
fn synk_ferdig_les(id: &str) -> std::collections::HashSet<String> { std::fs::read(synk_ferdig_sti(id)).ok().and_then(|b| serde_json::from_slice(&b).ok()).unwrap_or_default() }
/// Atomisk (0.3.0): skriv til .tmp og bytt navn. Døde appen midt i en vanlig skriving, ble fila
/// halv, leste tilbake som TOM — og hele synk-mappa ble lastet opp på nytt.
fn synk_ferdig_skriv(id: &str, sett: &std::collections::HashSet<String>) {
    if let Ok(b) = serde_json::to_vec(sett) {
        let p = synk_ferdig_sti(id);
        let tmp = p.with_extension("tmp");
        if std::fs::write(&tmp, b).is_ok() { let _ = std::fs::rename(&tmp, &p); }
    }
}
fn nokkel(f: &OppFil, mtime: u64) -> String { format!("{}|{}|{}", f.sti, f.bytes, mtime) }

async fn skann(sti: &str) -> Vec<(OppFil, u64)> {
    let mut ut = Vec::new();
    if let Ok(liste) = les_mappe(sti.to_string()).await {
        for f in liste {
            let mt = tokio::fs::metadata(&f.sti).await.ok().and_then(|m| m.modified().ok()).and_then(|m| m.duration_since(std::time::UNIX_EPOCH).ok()).map(|d| d.as_secs()).unwrap_or(0);
            ut.push((f, mt));
        }
    }
    ut
}

#[derive(Clone, Serialize)]
struct SynkFunn { id: String, mappe_id: String, navn: String, filer: Vec<OppFil> }

async fn synk_loper(app: AppHandle, synk: Synk, meldt: Arc<tokio::sync::Mutex<std::collections::HashSet<String>>>) {
    let mut forrige: std::collections::HashMap<String, (u64, u64)> = std::collections::HashMap::new();
    loop {
        let ferdige = synk_ferdig_les(&synk.id);
        let naa = skann(&synk.lokal).await;
        let mut nye = Vec::new();
        let mut denne = std::collections::HashMap::new();
        for (f, mt) in &naa {
            denne.insert(f.sti.clone(), (f.bytes, *mt));
            if ferdige.contains(&nokkel(f, *mt)) { continue; }
            if f.bytes == 0 { continue; }
            // Stabil = samme størrelse og mtime som forrige skann, og mtime minst 10 s gammel.
            let stabil = forrige.get(&f.sti).map(|(b, m)| *b == f.bytes && *m == *mt).unwrap_or(false)
                && std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0).saturating_sub(*mt) >= 10;
            if !stabil { continue; }
            let mut m = meldt.lock().await;
            if m.contains(&f.sti) { continue; }
            m.insert(f.sti.clone());
            nye.push(f.clone());
        }
        forrige = denne;
        if !nye.is_empty() { let _ = app.emit("synk-funn", SynkFunn { id: synk.id.clone(), mappe_id: synk.mappe_id.clone(), navn: synk.navn.clone(), filer: nye }); }
        tokio::time::sleep(std::time::Duration::from_secs(20)).await;
    }
}

/// Sett hele lista av synk-mapper (erstatter): stopper gamle løpere, starter aktive.
#[tauri::command]
async fn synk_sett(app: AppHandle, st: State<'_, SynkTilstand>, liste: Vec<Synk>) -> Result<(), String> {
    let mut l = st.lopere.lock().await;
    for (_, h) in l.drain() { h.abort(); }
    for synk in liste.into_iter().filter(|s| s.aktiv && !s.lokal.is_empty()) {
        let id = synk.id.clone();
        l.insert(id, tokio::spawn(synk_loper(app.clone(), synk, st.meldt.clone())));
    }
    Ok(())
}

/// Jobben for disse filene er ferdig (ok) eller feilet — oppdater minnet.
#[tauri::command]
async fn synk_merk(st: State<'_, SynkTilstand>, id: String, filer: Vec<OppFil>, ok: bool) -> Result<(), String> {
    let mut m = st.meldt.lock().await;
    let mut ferdige = synk_ferdig_les(&id);
    for f in &filer {
        m.remove(&f.sti);
        if ok { let mt = tokio::fs::metadata(&f.sti).await.ok().and_then(|x| x.modified().ok()).and_then(|x| x.duration_since(std::time::UNIX_EPOCH).ok()).map(|d| d.as_secs()).unwrap_or(0); ferdige.insert(nokkel(f, mt)); }
    }
    synk_ferdig_skriv(&id, &ferdige);
    Ok(())
}

/// Deling → fil-liste (token = auth). Brukes av rawskap://deling/<token>. `nokkel` er den kortlevde
/// adgangsnøkkelen for passord-/inviterte delinger (tom for åpne) — se skapflyt::hent_deling.
#[tauri::command]
async fn hent_deling(portal: String, token: String, nokkel: Option<String>) -> Result<serde_json::Value, String> {
    skapflyt::hent_deling(portal, token, nokkel).await
}

/// Passord → kortlevd adgangsnøkkel for en passordbeskyttet deling (19/9). ⚠ Passordet sendes ÉN
/// gang og lagres ingen steder — verken her eller i frontenden.
#[tauri::command]
async fn deling_passord(portal: String, token: String, passord: String) -> Result<String, String> {
    skapflyt::deling_passord(portal, token, passord).await
}

/// Semantisk søk i skapet (samme motor som nettsøket) → [{id, score}].
#[tauri::command]
async fn sok(portal: String, nokkel: String, q: String) -> Result<serde_json::Value, String> {
    let k = klient(&nokkel)?;
    let r = k.get(format!("{}/api/rawskap/sok-semantisk?q={}", portal.trim_end_matches('/'), urlenc(&q))).send().await.map_err(nettfeil)?;
    let st = r.status().as_u16();
    let d: serde_json::Value = r.json().await.map_err(nettfeil)?;
    if st == 503 { return Err("Søk i skapet er ikke satt opp på kontoen".into()); }
    if !(200..300).contains(&st) { return Err(d["error"].as_str().unwrap_or("søket feilet").to_string()); }
    Ok(d)
}

#[tauri::command]
fn avbryt(tilstand: State<'_, Tilstand>) {
    tilstand.avbryt.store(true, Ordering::Relaxed);
}

/// MANUELL PROXY-LENKING (0.2.0): brukeren peker på en lokal fil som skal bli
/// avspillingskopi for et asset som alt ligger i skapet — veien for videoene
/// automatikken bommer på (egne mappenavn, proxy på annen disk, ProRes RAW
/// lastet opp før proxyen fantes). Samme enkoder/remux-løype som ved
/// opplasting; serveren eier nøkkelen og «fyll bare tomt»-regelen for plakat.
#[tauri::command]
async fn lenk_proxy(app: AppHandle, portal: String, nokkel: String, asset_id: String, sti: String) -> Result<serde_json::Value, String> {
    let portal = portal.trim_end_matches('/').to_string();
    let k = klient(&nokkel)?;
    let bare = bare_opp();
    lenk_en(&App(app), &k, &bare, &portal, &asset_id, &sti).await?;
    Ok(serde_json::json!({ "ok": true }))
}

async fn lenk_en(app: &App, k: &reqwest::Client, bare: &reqwest::Client, portal: &str, asset_id: &str, sti: &str) -> Result<(), String> {
    let r = k.post(format!("{}/api/rawskap/opplasting", portal))
        .json(&serde_json::json!({ "action": "proxy-lenk-start", "id": asset_id })).send().await.map_err(nettfeil)?;

    let d: serde_json::Value = r.json().await.map_err(nettfeil)?;
    let put = d["proxyPutUrl"].as_str().ok_or_else(|| d["error"].as_str().unwrap_or("fikk ikke signert opplasting").to_string())?.to_string();
    let fasit = d["varighet"].as_f64().unwrap_or(0.0);
    let har_plakat = d["harPlakat"].as_bool().unwrap_or(true);
    // Den valgte fila må kunne leses og dekke klippet — samme krav som
    // automatikken stiller til sine kandidater.
    let (_, pe) = ffmpeg_ut(app, &["-hide_banner".into(), "-i".into(), sti.to_string()]).await.unwrap_or_default();
    if probe_ubrukelig(&pe) { return Err("Fila kan ikke leses som video".into()); }
    let p_var = parse_varighet(&pe);
    if fasit > 0.0 && p_var > 0.0 && p_var < fasit * 0.9 {
        return Err(format!("Proxyen dekker bare {:.0} av {:.0} sekunder — er det riktig fil?", p_var, fasit));
    }
    let (w, _) = parse_dim(&pe);
    let remux = pe.contains("Video: h264") && w > 0 && w <= 1920;
    let n = lag_og_last_opp_proxy(app, bare, portal, sti, &put, sti, 0, if fasit > 0.0 { fasit } else { p_var }, remux).await
        .ok_or("Fikk ikke laget avspillingskopi av fila")?;
    let mut body = serde_json::json!({ "action": "proxy-lenk-fullfor", "id": asset_id, "proxyStorrelse": n });
    // Mangler kortet plakat, kan den lokale proxyen levere den også.
    if !har_plakat {
        let vi = video_info(app, sti).await;
        if let Some(po) = vi.poster { body["posterBase64"] = serde_json::json!(po); }
        if let Some(sp) = vi.sprite { body["spriteBase64"] = serde_json::json!(sp); body["spriteFrames"] = serde_json::json!(vi.frames); }
    }
    let r = k.post(format!("{}/api/rawskap/opplasting", portal)).json(&body).send().await.map_err(nettfeil)?;
    let d: serde_json::Value = r.json().await.unwrap_or(serde_json::json!({}));
    if !d["ok"].as_bool().unwrap_or(false) { return Err(d["error"].as_str().unwrap_or("Serveren avviste proxyen").to_string()); }
    Ok(())
}

/// MAPPE MOT MAPPE (0.2.0, Vegard: «kan man lenke en mappe mot en mappe også
/// finner appen match basert på navn?»): hent skap-mappas videoer uten
/// avspillingskopi, indekser den lokale mappa på filstamme (rekursivt), og
/// lenk hvert treff. 69 høyreklikk blir ett.
#[tauri::command]
async fn lenk_proxy_mappe(app: AppHandle, portal: String, nokkel: String, mappe_id: String, lokal: String) -> Result<serde_json::Value, String> {
    let portal = portal.trim_end_matches('/').to_string();
    let k = klient(&nokkel)?;
    let bare = bare_opp();
    let app = App(app);
    let r = k.post(format!("{}/api/rawskap/opplasting", portal))
        .json(&serde_json::json!({ "action": "proxy-lenk-kandidater", "mappeId": mappe_id })).send().await.map_err(nettfeil)?;
    let d: serde_json::Value = r.json().await.map_err(nettfeil)?;
    let kandidater: Vec<(String, String)> = d["kandidater"].as_array().map(|a| a.iter()
        .filter_map(|x| Some((x["id"].as_str()?.to_string(), x["filnavn"].as_str()?.to_string()))).collect()).unwrap_or_default();
    if kandidater.is_empty() { return Ok(serde_json::json!({ "lenket": 0, "mangler": 0, "feilet": [] })); }
    // Stamme → lokal sti. Grunneste treff vinner (Resolve legger dypt; en
    // proxy rett i mappa er mer presis enn en fem nivåer ned).
    let mut kart: std::collections::HashMap<String, (usize, String)> = std::collections::HashMap::new();
    for f in les_mappe(lokal.clone()).await? {
        if !er_video(&f.sti) && !f.sti.to_ascii_lowercase().ends_with(".lrf") { continue; }
        let p = std::path::Path::new(&f.sti);
        let Some(stem) = p.file_stem().and_then(|s| s.to_str()) else { continue };
        let dybde = f.relativ.matches('/').count() + f.relativ.matches('\\').count();
        let e = kart.entry(stem.to_ascii_lowercase()).or_insert((usize::MAX, String::new()));
        if dybde < e.0 { *e = (dybde, f.sti.clone()); }
    }
    let total = kandidater.len();
    let (mut lenket, mut mangler) = (0usize, 0usize);
    let mut feilet: Vec<serde_json::Value> = Vec::new();
    for (i, (id, navn)) in kandidater.iter().enumerate() {
        let stem = std::path::Path::new(navn).file_stem().and_then(|s| s.to_str()).unwrap_or(navn).to_ascii_lowercase();
        let _ = app.0.emit("lenk-framdrift", serde_json::json!({ "navn": navn, "n": i + 1, "total": total }));
        match kart.get(&stem) {
            None => { mangler += 1; }
            Some((_, sti)) => match lenk_en(&app, &k, &bare, &portal, id, sti).await {
                Ok(()) => { lenket += 1; }
                Err(e) => { feilet.push(serde_json::json!({ "navn": navn, "feil": e })); }
            },
        }
    }
    Ok(serde_json::json!({ "lenket": lenket, "mangler": mangler, "feilet": feilet }))
}

/// OPPLASTINGSØKT (5/10): meld portalen fasiten for en opplastingsjobb, fremdrift underveis og
/// slutten. Bare de tre okt-handlingene slippes gjennom — se skapflyt::opplasting_okt.
#[tauri::command]
async fn opplasting_okt(portal: String, nokkel: String, data: serde_json::Value) -> Result<serde_json::Value, String> {
    skapflyt::opplasting_okt(portal, nokkel, data, UA).await
}

/// DEL FRA APPEN (0.2.0): opprett en LEVENDE mappe-deling med kundens egne delings-defaults —
/// se skapflyt::del_mappe.
#[tauri::command]
async fn del_mappe(portal: String, nokkel: String, mappe_id: String, navn: String) -> Result<serde_json::Value, String> {
    skapflyt::del_mappe(portal, nokkel, mappe_id, navn, UA).await
}

/// MINNEKORT-INNLASTING (0.2.0, DIT): kopier fra kort til arkiv/NAS med
/// VERIFISERING — xxh64 regnes mens kilden leses, målet leses TILBAKE og
/// hashes på nytt, og først når de to er like meldes fila «verifisert».
/// Det er hele forskjellen på dette og en Explorer-kopi: etterpå VET du at
/// arkivkopien er bit for bit lik kortet, og kortet kan formateres.
/// Sletter ALDRI fra kortet — det er fotografens handling, ikke vår.
///
/// T4 (24/10): selve kopieringen er `skapflyt::innlesing`, den samme som rawLight skal bruke. Den
/// overskriver aldri (to kameraer med samme DSC0001.ARW gir DSC0001-1.ARW), skriver hver kopi helt
/// til disk, tar med kortets tid (F2) og leser kopien tilbake forbi Windows' filbuffer. `reserve` er
/// en valgfri andre kopi. `trygt` er sann bare når hver fil er kontrollert i alle målene.
#[tauri::command]
async fn last_inn(app: AppHandle, tilstand: State<'_, Tilstand>, filer: Vec<OppFil>, maal: String, reserve: Option<String>) -> Result<serde_json::Value, String> {
    let app = App(app);
    tilstand.avbryt.store(false, Ordering::Relaxed);
    if let Ok(mut s) = tilstand.avbrutte.lock() { s.clear(); }
    let arkiv = PathBuf::from(&maal);
    let mut maalene = vec![arkiv.clone()];
    if let Some(r) = reserve.filter(|r| !r.trim().is_empty()) { maalene.push(PathBuf::from(r)); }
    // Kortet kan være tatt ut siden lista ble lest: da feiler fila i innlesingen, ikke her.
    let kilder: Vec<Kildefil> = filer.iter().map(|f| Kildefil::fra_sti(&f.sti, f.relativ.clone())
        .unwrap_or_else(|_| Kildefil { sti: PathBuf::from(&f.sti), relativ: f.relativ.clone(), bytes: f.bytes, endret: std::time::UNIX_EPOCH })).collect();
    let stopp = Stopp { global: tilstand.avbryt.clone(), sett: tilstand.avbrutte.clone(), id: String::new() };
    let rapport = tokio::task::spawn_blocking(move || innlesing::last_inn(&kilder, &innlesing::Valg::ny(maalene, UA), &app, &stopp))
        .await.map_err(|e| format!("{e}"))??;
    Ok(inn_svar(&rapport, &arkiv))
}

/// Svaret grensesnittet har fått siden 0.2.0 (`ok`, `feil`, `kopiert`), pluss `trygt`. `kopiert` er
/// arkivkopiene som opplastingen etterpå leser: navnet og undermappa er kopiens, som kan ha fått
/// `-1` når navnet var tatt.
fn inn_svar(r: &innlesing::Rapport, arkiv: &Path) -> serde_json::Value {
    let mut ok = 0usize; let mut feil: Vec<serde_json::Value> = Vec::new(); let mut kopiert: Vec<serde_json::Value> = Vec::new();
    for f in &r.filer {
        if f.kontrollert() {
            ok += 1;
            if let Some(sti) = f.kopier.first().and_then(|k| k.sti.as_ref()) {
                let relativ = sti.strip_prefix(arkiv).map(|p| p.to_string_lossy().replace('\\', "/")).unwrap_or_else(|_| f.relativ.clone());
                kopiert.push(serde_json::json!({ "sti": sti.to_string_lossy(), "relativ": relativ, "bytes": f.bytes, "xxh64": f.xxh64 }));
            }
        } else {
            feil.push(serde_json::json!({ "id": f.kilde.to_string_lossy(), "feil": f.feil().unwrap_or_else(|| "Ukjent feil".into()) }));
        }
    }
    serde_json::json!({ "ok": ok, "feil": feil, "kopiert": kopiert, "trygt": r.trygt_aa_formatere, "ikkeTrygt": r.ikke_trygt, "manifest": r.manifest })
}

/// LOKAL BROWSER (0.2.1, Vegard: «en lokal browser ved siden av skapet man
/// kan drag+drop?»): ETT nivå av en lokal mappe — panelet skal føles som
/// Utforsker, ikke som en skanning. les_mappe er rekursiv og ville lest hele
/// treet ved hver klikk; denne leser bare det du ser.
#[tauri::command]
async fn les_lokal(sti: String) -> Result<serde_json::Value, String> {
    // Tom sti = «Denne maskinen»: stasjonene (mac: / + /Volumes), så panelet
    // er en ekte disk-browser fra første klikk — ikke et påtvunget mappevalg.
    if sti.is_empty() {
        let mut mapper: Vec<String> = Vec::new();
        #[cfg(target_os = "windows")]
        for b in b'A'..=b'Z' {
            let rot = format!("{}:\\", b as char);
            if std::path::Path::new(&rot).exists() { mapper.push(rot); }
        }
        #[cfg(not(target_os = "windows"))]
        {
            mapper.push("/".to_string());
            if let Ok(mut rd) = tokio::fs::read_dir("/Volumes").await {
                while let Ok(Some(e)) = rd.next_entry().await {
                    if let Some(s) = e.path().to_str() { mapper.push(s.to_string()); }
                }
            }
        }
        return Ok(serde_json::json!({ "mapper": mapper, "filer": [], "rot": true }));
    }
    let mut mapper: Vec<String> = Vec::new();
    let mut filer: Vec<serde_json::Value> = Vec::new();
    let mut rd = tokio::fs::read_dir(&sti).await.map_err(|e| format!("{e}"))?;
    while let Some(e) = rd.next_entry().await.map_err(|e| format!("{e}"))? {
        let navn = e.file_name().to_string_lossy().to_string();
        if navn.starts_with('.') || navn.starts_with('$') { continue; }
        let Ok(md) = e.metadata().await else { continue };
        if md.is_dir() { mapper.push(navn); }
        else { filer.push(serde_json::json!({ "navn": navn, "bytes": md.len() })); }
    }
    mapper.sort_by_key(|a| a.to_lowercase());
    filer.sort_by(|a, b| a["navn"].as_str().unwrap_or("").to_lowercase().cmp(&b["navn"].as_str().unwrap_or("").to_lowercase()));
    Ok(serde_json::json!({ "mapper": mapper, "filer": filer }))
}

// ── LIGHTROOM-PLUGINEN (0.3.1, 27/9) ─────────────────────────────────────────────────────────
// Transfer installerer og oppdaterer Rawskap-pluginen for Lightroom Classic. Lightroom laster plugins
// som ligger i Modules-mappa automatisk når det starter — ingen Plugin-behandler, ingen «Legg til».
//   Windows: %APPDATA%\Adobe\Lightroom\Modules    Mac: ~/Library/Application Support/Adobe/Lightroom/Modules
// Hvorfor HER og ikke en egen installer (Vegards valg 27/9): Mac-appen er alt signert og notarisert,
// så ingen nye sertifikater trengs, og samme løype blir selvoppdateringen senere.
// Pakken og sjekksummen kommer fra manifestet (portal: /api/lightroom/versjon, skrevet av
// verktoy/publiser-lightroom.mjs). ⚠ Uten riktig sha256 installeres ingenting — det er kode som
// Lightroom kjører.
const LR_MAPPE: &str = "rawskap.lrdevplugin";

fn lightroom_moduler() -> Option<PathBuf> {
    dirs::config_dir().map(|d| d.join("Adobe").join("Lightroom").join("Modules"))
}

/// VERSION fra Info.lua («major = 1, minor = 2, revision = 0» → "1.2.0").
fn les_lua_versjon(info: &str) -> Option<String> {
    let resten = &info[info.find("VERSION =")?..];
    let tall = |navn: &str| -> Option<u64> {
        let i = resten.find(navn)? + navn.len();
        let s = resten[i..].trim_start().strip_prefix('=')?.trim_start();
        s.chars().take_while(|c| c.is_ascii_digit()).collect::<String>().parse().ok()
    };
    Some(format!("{}.{}.{}", tall("major")?, tall("minor")?, tall("revision")?))
}

fn sha256_hex(b: &[u8]) -> String {
    use sha2::Digest;
    sha2::Sha256::digest(b).iter().map(|x| format!("{x:02x}")).collect()
}

/// Pakker ut plugin-zip-en til `<moduler>/rawskap.lrdevplugin`.
///
/// ⚠ Aldri en halv plugin: alt pakkes ut i en midlertidig mappe ved siden av, den gamle flyttes unna,
/// den nye inn, og først da slettes den gamle. Feiler et steg, står den gamle urørt.
/// ⚠ Bare filer under rawskap.lrdevplugin/ godtas, og ingen stier som klatrer ut (zip-slip).
fn pakk_ut_lightroom(zip_bytes: &[u8], moduler: &Path) -> Result<PathBuf, String> {
    std::fs::create_dir_all(moduler).map_err(|e| format!("Kunne ikke lage {}: {e}", moduler.display()))?;
    let suffiks = rand_suffiks();
    let tmp = moduler.join(format!(".rawskap-ny-{suffiks}"));
    let utpakket = (|| -> Result<(), String> {
        let mut arkiv = zip::ZipArchive::new(std::io::Cursor::new(zip_bytes)).map_err(|e| format!("Pakken er ødelagt: {e}"))?;
        for i in 0..arkiv.len() {
            let mut fil = arkiv.by_index(i).map_err(|e| format!("Pakken er ødelagt: {e}"))?;
            let navn = fil.enclosed_name().ok_or("Pakken har en ugyldig sti")?;
            let rot = navn.components().next().and_then(|c| match c { std::path::Component::Normal(s) => s.to_str(), _ => None });
            if rot != Some(LR_MAPPE) { return Err("Pakken har filer utenfor plugin-mappa".into()); }
            let mal = tmp.join(&navn);
            if fil.is_dir() { std::fs::create_dir_all(&mal).map_err(|e| format!("{e}"))?; continue; }
            if let Some(d) = mal.parent() { std::fs::create_dir_all(d).map_err(|e| format!("{e}"))?; }
            let mut ut = std::fs::File::create(&mal).map_err(|e| format!("{e}"))?;
            std::io::copy(&mut fil, &mut ut).map_err(|e| format!("{e}"))?;
        }
        if !tmp.join(LR_MAPPE).join("Info.lua").is_file() { return Err("Pakken mangler Info.lua".into()); }
        Ok(())
    })();
    if let Err(e) = utpakket { let _ = std::fs::remove_dir_all(&tmp); return Err(e); }
    let endelig = moduler.join(LR_MAPPE);
    let gammel = moduler.join(format!(".rawskap-gammel-{suffiks}"));
    let hadde = endelig.exists();
    if hadde {
        if let Err(e) = std::fs::rename(&endelig, &gammel) {
            let _ = std::fs::remove_dir_all(&tmp);
            return Err(format!("Fikk ikke byttet ut den gamle pluginen — er Lightroom åpent? Lukk det og prøv igjen ({e})"));
        }
    }
    if let Err(e) = std::fs::rename(tmp.join(LR_MAPPE), &endelig) {
        if hadde { let _ = std::fs::rename(&gammel, &endelig); }
        let _ = std::fs::remove_dir_all(&tmp);
        return Err(format!("Fikk ikke lagt pluginen på plass: {e}"));
    }
    let _ = std::fs::remove_dir_all(&tmp);
    if hadde { let _ = std::fs::remove_dir_all(&gammel); }
    Ok(endelig)
}

/// Hva er installert, og hva er ute? `tilgjengelig` er tom hvis manifestet ikke kunne leses.
#[tauri::command]
async fn lightroom_status(portal: String) -> serde_json::Value {
    let moduler = lightroom_moduler();
    // Adobe/Lightroom-mappa finnes når Lightroom har kjørt på maskinen minst én gang.
    let lightroom = moduler.as_ref().and_then(|m| m.parent()).map(|p| p.is_dir()).unwrap_or(false);
    let installert = moduler.as_ref()
        .and_then(|m| std::fs::read_to_string(m.join(LR_MAPPE).join("Info.lua")).ok())
        .and_then(|s| les_lua_versjon(&s));
    let url = format!("{}/api/lightroom/versjon", portal.trim_end_matches('/'));
    let tilgjengelig = match enkel_klient().get(url).send().await {
        Ok(r) => r.json::<serde_json::Value>().await.ok().and_then(|m| m["versjon"].as_str().map(|s| s.to_string())).unwrap_or_default(),
        Err(_) => String::new(),
    };
    serde_json::json!({
        "lightroom": lightroom, "installert": installert, "tilgjengelig": tilgjengelig,
        "sti": moduler.map(|m| m.join(LR_MAPPE).to_string_lossy().to_string()).unwrap_or_default(),
    })
}

/// Last ned pluginen fra manifestet, sjekk sha256, og legg den i Lightrooms Modules-mappe.
#[tauri::command]
async fn installer_lightroom(portal: String) -> Result<serde_json::Value, String> {
    let moduler = lightroom_moduler().ok_or("Fant ikke brukermappa")?;
    installer_lightroom_til(&portal, &moduler).await
}

/// Selve installasjonen, med målmappa som parameter — så testen kan kjøre hele løypa mot den ekte
/// pakken i prod uten å røre Lightroom på maskina den kjøres på.
async fn installer_lightroom_til(portal: &str, moduler: &Path) -> Result<serde_json::Value, String> {
    let portal = portal.trim_end_matches('/');
    let k = enkel_klient();
    let m: serde_json::Value = k.get(format!("{portal}/api/lightroom/versjon")).send().await.map_err(nettfeil)?.json().await.map_err(nettfeil)?;
    let versjon = m["versjon"].as_str().unwrap_or("").to_string();
    let url = m["zip"].as_str().unwrap_or("").to_string();
    let fasit = m["sha256"].as_str().unwrap_or("").to_ascii_lowercase();
    if versjon.is_empty() || !url.starts_with("https://") { return Err("Fant ingen Lightroom-plugin å installere".into()); }
    if fasit.len() != 64 { return Err("Manifestet mangler sjekksum — installerer ikke".into()); }
    let bytes = k.get(&url).send().await.map_err(nettfeil)?.error_for_status().map_err(nettfeil)?.bytes().await.map_err(nettfeil)?;
    if bytes.len() > 20 << 20 { return Err("Pakken er uventet stor — installerer ikke".into()); }
    if sha256_hex(&bytes) != fasit { return Err("Pakken stemmer ikke med sjekksummen — installerer ikke".into()); }
    let sti = pakk_ut_lightroom(&bytes, moduler)?;
    log::info!("Lightroom-pluginen {versjon} installert i {}", sti.display());
    Ok(serde_json::json!({ "versjon": versjon, "sti": sti.to_string_lossy() }))
}

/// «Fjern» på en opplasting som ikke ble ferdig (0.3.0): serveren får beskjed, og resume-fila
/// slettes. ⚠ Pause og «Avbryt» skal IKKE kalle dette — se skapflyt::forkast_opplasting.
#[tauri::command]
async fn forkast_opplasting(portal: String, nokkel: String, filer: Vec<OppFil>) -> Result<u32, String> {
    skapflyt::forkast_opplasting(portal, nokkel, filer, UA).await
}

/// Om → «Åpne loggmappa» (0.3.0).
#[tauri::command]
fn apne_loggmappe(app: AppHandle) -> Result<(), String> {
    use tauri::Manager;
    let d = app.path().app_log_dir().map_err(|e| format!("{e}"))?;
    let _ = std::fs::create_dir_all(&d);
    vis_i_utforsker(d.to_string_lossy().to_string())
}

/// Stopp ÉN fil i jobben som kjører; resten går videre.
#[tauri::command]
fn avbryt_fil(tilstand: State<'_, Tilstand>, id: String) {
    if let Ok(mut s) = tilstand.avbrutte.lock() { s.insert(id); }
}

/// Lukk-til-systemkurv (22/8): når valget er på, skjules vinduet i stedet for å
/// avslutte — synk-mapper og kø lever videre. «Avslutt» i kurv-menyen avslutter.
static TIL_KURV: AtomicBool = AtomicBool::new(false);
#[tauri::command]
fn sett_til_kurv(paa: bool) { TIL_KURV.store(paa, Ordering::Relaxed); }

fn vis_vindu(app: &AppHandle) {
    use tauri::Manager;
    if let Some(w) = app.get_webview_window("main") { let _ = w.show(); let _ = w.unminimize(); let _ = w.set_focus(); }
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    use tauri::Manager;
    use tauri::menu::{Menu, MenuItem};
    use tauri::tray::{TrayIconBuilder, TrayIconEvent, MouseButton, MouseButtonState};
    tauri::Builder::default()
        .plugin(tauri_plugin_single_instance::init(|app, _args, _cwd| { vis_vindu(app); }))
        .plugin(tauri_plugin_deep_link::init())
        .plugin(tauri_plugin_autostart::init(tauri_plugin_autostart::MacosLauncher::LaunchAgent, Some(vec!["--skjult"])))
        .setup(|app| {
            log::info!("Rawskap Transfer {} startet ({})", env!("CARGO_PKG_VERSION"), std::env::consts::OS);
            // rawskap:// — i dev er ikke skjemaet registrert av en installer; gjør det her.
            #[cfg(any(windows, target_os = "linux"))]
            { use tauri_plugin_deep_link::DeepLinkExt; let _ = app.deep_link().register_all(); }
            // Systemkurv: venstreklikk = vis, meny = Åpne / Avslutt.
            let apne = MenuItem::with_id(app, "apne", "Åpne Rawskap Transfer", true, None::<&str>)?;
            let avslutt = MenuItem::with_id(app, "avslutt", "Avslutt", true, None::<&str>)?;
            let meny = Menu::with_items(app, &[&apne, &avslutt])?;
            let mut tb = TrayIconBuilder::with_id("hoved").menu(&meny).show_menu_on_left_click(false).tooltip("Rawskap Transfer");
            if let Some(ikon) = app.default_window_icon().cloned() { tb = tb.icon(ikon); }
            tb.on_menu_event(|app, ev| match ev.id.as_ref() { "apne" => vis_vindu(app), "avslutt" => app.exit(0), _ => {} })
              .on_tray_icon_event(|tray, ev| { if let TrayIconEvent::Click { button: MouseButton::Left, button_state: MouseButtonState::Up, .. } = ev { vis_vindu(tray.app_handle()); } })
              .build(app)?;
            // Startet av autostart («--skjult»): ligg i kurven til noen åpner.
            if std::env::args().any(|a| a == "--skjult") { if let Some(w) = app.get_webview_window("main") { let _ = w.hide(); } }
            Ok(())
        })
        .on_window_event(|w, ev| {
            if let tauri::WindowEvent::CloseRequested { api, .. } = ev {
                if TIL_KURV.load(Ordering::Relaxed) { api.prevent_close(); let _ = w.hide(); }
            }
        })
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_shell::init())
        .plugin(tauri_plugin_store::Builder::default().build())
        .plugin(tauri_plugin_notification::init())
        .plugin(tauri_plugin_updater::Builder::new().build())
        .plugin(tauri_plugin_process::init())
        // LOGG (0.3.0): pluginen lå i Cargo.toml fra første dag, men ble aldri slått på — når noe
        // feilet, fantes det ingenting å se på. Nå: fil i appens loggmappe (Om → «Åpne loggmappa»),
        // 5 MB × 3. ⚠ Aldri URL-er i loggen: nettfeil stripper dem, og all nettfeil går gjennom den.
        .plugin(tauri_plugin_log::Builder::new()
            .clear_targets()
            .target(tauri_plugin_log::Target::new(tauri_plugin_log::TargetKind::LogDir { file_name: Some("transfer".into()) }))
            .level(log::LevelFilter::Warn)
            .level_for("app_lib", log::LevelFilter::Info)
            .level_for("skapflyt", log::LevelFilter::Info)
            .max_file_size(5_000_000)
            .rotation_strategy(tauri_plugin_log::RotationStrategy::KeepSome(3))
            .timezone_strategy(tauri_plugin_log::TimezoneStrategy::UseLocal)
            .build())
        .manage(Tilstand::default())
        .manage(SynkTilstand::default())
        .invoke_handler(tauri::generate_handler![avbryt_fil, les_lokal,
            mangler_lokalt, omdoep, lenk_proxy, lenk_proxy_mappe, del_mappe, opplasting_okt, last_inn, hent_liste, hent_deling, deling_passord, sok, last_ned, last_opp, les_mappe, ny_mappe, er_mappe, ledig_plass, vis_i_utforsker, sjekk_versjon, sett_tray_tekst, sett_framdrift, rydd_part_i_mappe, slett_filer, sett_nettverk, synk_sett, synk_merk, sett_til_kurv, avbryt, kobling_start, kobling_poll, maskinnavn, forkast_opplasting, apne_loggmappe, lightroom_status, installer_lightroom])
        .run(tauri::generate_context!())
        .expect("Rawskap Transfer kunne ikke starte");
}

// ─────────────────────────────────────────────────────────────────────────────
// TESTER (17/9). Repoet hadde ingen. Fire feil ble funnet på én ettermiddag, og tre av dem satt i
// logikk som så åpenbart riktig ut ved lesing — så dekningen starter på det som faktisk ØDELEGGER
// DATA hvis det er galt, ikke på det som er lett å teste.
// (S1: nett-, opplastings- og nedlastingstestene ligger i crates/skapflyt/src/tester.rs, sammen med koden.)
//   cargo test
#[cfg(test)]
mod tester {
    use super::*;

    /// `ledig_plass` må tåle at målmappa ikke finnes ennå — den lages først når
    /// jobben starter. Uten oppoverklatringen ville bekreftelsesmodalen aldri
    /// fått et tall å vise, og plass-sjekken (hele grunnen til at den finnes)
    /// hadde vært stille i det vanligste tilfellet.
    #[test]
    fn ledig_plass_klatrer_til_mappe_som_finnes() {
        let rot = std::env::temp_dir();
        let dyp = rot.join("rawskap-finnes-ikke").join("heller-ikke-denne");
        assert!(!dyp.exists(), "testen forutsetter at stien ikke finnes");

        let svar = ledig_plass(dyp.to_string_lossy().to_string());
        assert!(svar.is_ok(), "skulle funnet en forelder som finnes: {svar:?}");
        assert!(svar.unwrap() > 0, "en disk med 0 ledig er mistenkelig — trolig feil sti");

        // Samme disk, eksisterende mappe: skal gi omtrent samme tall.
        let direkte = ledig_plass(rot.to_string_lossy().to_string()).unwrap();
        let via_klatring = ledig_plass(dyp.to_string_lossy().to_string()).unwrap();
        let avvik = direkte.abs_diff(via_klatring);
        assert!(avvik < 5 << 30, "samme disk skal gi samme svar (avvik {avvik} bytes)");
    }

    /// En sti uten forelder i det hele tatt skal gi feil, ikke panikk.
    #[test]
    fn ledig_plass_paa_tom_sti_feiler_pent() {
        assert!(ledig_plass(String::new()).is_err());
    }

    #[test]
    fn temp_navn_er_unike() {
        let navn: std::collections::HashSet<String> = (0..10_000).map(|_| rand_suffiks()).collect();
        assert_eq!(navn.len(), 10_000);
    }

    // ── MINNEKORT (T4) ──
    // Svaret til grensesnittet: opplastingen etterpå leser arkivkopien, så navnet og undermappa
    // skal være kopiens (DSC0001-1.ARW når navnet var tatt), ikke kortets.
    #[test]
    fn innlesingens_svar_til_grensesnittet() {
        use innlesing::{Innlest, Kopi, Rapport, Utfall};
        let arkiv = PathBuf::from("D:/Arkiv");
        let kopi = |sti: Option<&str>, utfall: Utfall| Kopi { maal: arkiv.clone(), sti: sti.map(|s| arkiv.join(s)), utfall, kontroll: None };
        let fil = |kilde: &str, relativ: &str, kopier: Vec<Kopi>| Innlest { kilde: PathBuf::from(kilde), relativ: relativ.into(), bytes: 10, xxh64: Some("00000000000000aa".into()), kopier };
        let r = Rapport {
            filer: vec![
                fil("F:/DCIM/100MSDCF/DSC0001.ARW", "DCIM/100MSDCF/DSC0001.ARW", vec![kopi(Some("DCIM/100MSDCF/DSC0001-1.ARW"), Utfall::Kopiert)]),
                fil("F:/DCIM/100MSDCF/DSC0002.ARW", "DCIM/100MSDCF/DSC0002.ARW", vec![kopi(Some("DCIM/100MSDCF/DSC0002.ARW"), Utfall::Fantes)]),
                fil("F:/DCIM/100MSDCF/DSC0003.ARW", "DCIM/100MSDCF/DSC0003.ARW", vec![kopi(None, Utfall::Avbrutt)]),
                fil("F:/DCIM/100MSDCF/DSC0004.ARW", "DCIM/100MSDCF/DSC0004.ARW", vec![kopi(Some("DCIM/100MSDCF/DSC0004.ARW"), Utfall::Kopiert), kopi(None, Utfall::Feil("Disken er full".into()))]),
            ],
            manifest: vec![arkiv.join(".rawskap-innlesing/x.json")],
            trygt_aa_formatere: false,
            ikke_trygt: None,
        };
        let s = inn_svar(&r, &arkiv);
        assert_eq!(s["ok"], 2);
        assert_eq!(s["kopiert"][0]["relativ"], "DCIM/100MSDCF/DSC0001-1.ARW");
        assert_eq!(s["kopiert"][0]["xxh64"], "00000000000000aa");
        assert_eq!(s["kopiert"][1]["relativ"], "DCIM/100MSDCF/DSC0002.ARW", "Fantes er også en kontrollert arkivkopi");
        // «Avbrutt» er signalet grensesnittet kjenner igjen (jobben blir «avbrutt», ikke rød).
        assert_eq!(s["feil"][0], serde_json::json!({ "id": "F:/DCIM/100MSDCF/DSC0003.ARW", "feil": "Avbrutt" }));
        assert_eq!(s["feil"][1]["feil"], "Disken er full", "en fil som mangler i reservekopien, er ikke ferdig");
        assert_eq!(s["trygt"], false);
        assert_eq!(inn_svar(&Rapport { trygt_aa_formatere: true, ..r }, &arkiv)["trygt"], true);
    }

    // ── LIGHTROOM-INSTALLASJONEN (0.3.1) ──
    fn lr_zip(filer: &[(&str, &str)]) -> Vec<u8> {
        use std::io::Write;
        let mut w = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
        let opt = zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Deflated);
        for (navn, innhold) in filer { w.start_file(*navn, opt).unwrap(); w.write_all(innhold.as_bytes()).unwrap(); }
        w.finish().unwrap().into_inner()
    }
    fn lr_moduler(navn: &str) -> PathBuf {
        let d = std::env::temp_dir().join("rawskap-transfer-test").join("lr").join(navn).join("Modules");
        let _ = std::fs::remove_dir_all(&d);
        d
    }
    const INFO: &str = "return {\n  LrToolkitIdentifier = 'no.rawstudios.rawskap',\n  -- ⚠ Må være lik Api.VERSJON\n  VERSION = { major = 1, minor = 2, revision = 3 },\n}\n";

    #[test]
    fn lua_versjonen_leses_fra_info() {
        assert_eq!(les_lua_versjon(INFO).as_deref(), Some("1.2.3"));
        assert_eq!(les_lua_versjon("return {}"), None);
    }

    #[test]
    fn sha256_er_riktig() {
        assert_eq!(sha256_hex(b"abc"), "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
    }

    /// Ny installasjon, og så en oppdatering som bytter ut den gamle helt — uten rester.
    #[test]
    fn lightroom_installeres_og_byttes_ut() {
        let m = lr_moduler("bytt");
        let v1 = lr_zip(&[("rawskap.lrdevplugin/Info.lua", INFO), ("rawskap.lrdevplugin/Gammel.lua", "-- skal forsvinne")]);
        let sti = pakk_ut_lightroom(&v1, &m).unwrap();
        assert!(sti.join("Info.lua").is_file() && sti.join("Gammel.lua").is_file());
        let v2 = lr_zip(&[("rawskap.lrdevplugin/Info.lua", &INFO.replace("revision = 3", "revision = 4"))]);
        pakk_ut_lightroom(&v2, &m).unwrap();
        let info = std::fs::read_to_string(m.join(LR_MAPPE).join("Info.lua")).unwrap();
        assert_eq!(les_lua_versjon(&info).as_deref(), Some("1.2.4"));
        assert!(!m.join(LR_MAPPE).join("Gammel.lua").exists(), "filer fra forrige versjon skal ikke henge igjen");
        let rester: Vec<_> = std::fs::read_dir(&m).unwrap().filter_map(|e| e.ok()).map(|e| e.file_name().to_string_lossy().to_string()).collect();
        assert_eq!(rester, vec![LR_MAPPE.to_string()], "ingen midlertidige mapper skal bli liggende");
    }

    /// Hele løypa mot PROD: manifest → pakke → sha256 → utpakking, i en midlertidig mappe.
    /// Går mot nettet, så den kjøres bare på forespørsel:  cargo test --lib -- --ignored lightroom_fra_prod
    #[test]
    #[ignore]
    fn lightroom_fra_prod() {
        let m = lr_moduler("prod");
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let svar = rt.block_on(installer_lightroom_til("https://rawskap.no", &m)).expect("installasjonen feilet");
        let info = std::fs::read_to_string(m.join(LR_MAPPE).join("Info.lua")).unwrap();
        assert_eq!(les_lua_versjon(&info).as_deref(), svar["versjon"].as_str(), "installert versjon = manifestets");
        for f in ["RawskapApi.lua", "RawskapPublish.lua", "RawskapJson.lua"] { assert!(m.join(LR_MAPPE).join(f).is_file(), "{f} mangler"); }
        println!("installert {} i {}", svar["versjon"], m.display());
    }

    /// Farlige eller feil pakker installeres ikke — og den som alt er installert, står urørt.
    #[test]
    fn lightroom_avviser_farlige_pakker() {
        let m = lr_moduler("farlig");
        pakk_ut_lightroom(&lr_zip(&[("rawskap.lrdevplugin/Info.lua", INFO)]), &m).unwrap();
        for (hvorfor, pakke) in [
            ("sti som klatrer ut", lr_zip(&[("rawskap.lrdevplugin/Info.lua", INFO), ("rawskap.lrdevplugin/../../ond.lua", "x")])),
            ("fil utenfor plugin-mappa", lr_zip(&[("rawskap.lrdevplugin/Info.lua", INFO), ("annen/ond.lua", "x")])),
            ("mangler Info.lua", lr_zip(&[("rawskap.lrdevplugin/Api.lua", "x")])),
            ("ikke en zip", b"dette er ikke en zip".to_vec()),
        ] {
            assert!(pakk_ut_lightroom(&pakke, &m).is_err(), "{hvorfor} skulle blitt avvist");
        }
        assert!(!m.parent().unwrap().join("ond.lua").exists() && !m.join("ond.lua").exists());
        let info = std::fs::read_to_string(m.join(LR_MAPPE).join("Info.lua")).unwrap();
        assert_eq!(les_lua_versjon(&info).as_deref(), Some("1.2.3"), "den installerte skal stå urørt");
        let rester: Vec<_> = std::fs::read_dir(&m).unwrap().filter_map(|e| e.ok()).map(|e| e.file_name().to_string_lossy().to_string()).collect();
        assert_eq!(rester, vec![LR_MAPPE.to_string()]);
    }
}
