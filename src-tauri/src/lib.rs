// Rawskap Transfer v0 (22/8-2026) — NEDLASTINGS-RETNINGEN først:
// hent en hel mappe (m/ undermapper) fra skapet til disk, parallelt, med
// Range-resume per fil. Bytene går portal → 302 → R2 direkte; appen følger
// redirecten selv så cookien IKKE sendes videre til R2.
//
// Auth: en langlivet «kunde_sesjon»-token (app-nøkkel fra portalen) sendes som
// Cookie-header — alle portal-ruter virker uendret.
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tauri::{AppHandle, Emitter, State};
use tauri_plugin_shell::ShellExt;
use base64::Engine;
use tokio::io::AsyncWriteExt;
use tokio::sync::Semaphore;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Fil {
    pub id: String,
    pub filnavn: String,
    pub bytes: u64,
    #[serde(default)]
    pub sti: String,
    #[serde(default)]
    pub xxh64: String,
    #[serde(default)]
    pub url: String, // deling: nedlastings-URL m/ token (302 → R2), uten innlogging
}

#[derive(Clone, Serialize)]
struct Framdrift {
    id: String,
    hentet: u64,
    total: u64,
    status: String, // venter | laster | ferdig | feil | hoppet
    feil: Option<String>,
}

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
    ned_mbps: Arc<AtomicU64>, // 0 = ubegrenset
    opp_mbps: Arc<AtomicU64>,
}

/// Enkel struping: hold gjennomsnittsfarten under taket ved å sove når vi
/// ligger foran skjema (token-bucket-lite, målt fra jobbstart).
struct Struper { start: std::time::Instant, bytes: AtomicU64, mbps: Arc<AtomicU64> }
impl Struper {
    fn ny(mbps: Arc<AtomicU64>) -> Arc<Self> { Arc::new(Self { start: std::time::Instant::now(), bytes: AtomicU64::new(0), mbps }) }
    async fn tell(&self, n: u64) {
        let tak = self.mbps.load(Ordering::Relaxed);
        let sum = self.bytes.fetch_add(n, Ordering::Relaxed) + n;
        if tak == 0 { return; }
        let skal_ha_brukt = (sum as f64 * 8.0) / (tak as f64 * 1_000_000.0); // sekunder
        let brukt = self.start.elapsed().as_secs_f64();
        if skal_ha_brukt > brukt { tokio::time::sleep(std::time::Duration::from_secs_f64((skal_ha_brukt - brukt).min(2.0))).await; }
    }
}

#[tauri::command]
fn sett_nettverk(tilstand: State<'_, Tilstand>, ned_mbps: u64, opp_mbps: u64) {
    tilstand.ned_mbps.store(ned_mbps, Ordering::Relaxed);
    tilstand.opp_mbps.store(opp_mbps, Ordering::Relaxed);
}

fn klient(nokkel: &str) -> Result<reqwest::Client, String> {
    let mut h = reqwest::header::HeaderMap::new();
    // API-nøkkel (rsk_live_…) = Bearer; app-nøkkel (signert sesjon) = cookie.
    if nokkel.starts_with("rsk_live_") {
        h.insert(reqwest::header::AUTHORIZATION, format!("Bearer {}", nokkel).parse().map_err(|e| format!("{e}"))?);
    } else {
        let cookie = format!("kunde_sesjon={}", nokkel);
        h.insert(reqwest::header::COOKIE, cookie.parse().map_err(|e| format!("{e}"))?);
    }
    h.insert(reqwest::header::USER_AGENT, "RawskapTransfer/0.1".parse().unwrap());
    reqwest::Client::builder()
        .default_headers(h)
        .redirect(reqwest::redirect::Policy::none()) // vi følger 302 selv (uten cookie)
        .build()
        .map_err(|e| format!("{e}"))
}

/// Hent mappetre + filer fra portalen.
#[tauri::command]
async fn hent_liste(portal: String, nokkel: String, mappe: String) -> Result<serde_json::Value, String> {
    let k = klient(&nokkel)?;
    let url = format!("{}/api/rawskap/transfer/liste?mappe={}", portal.trim_end_matches('/'), mappe);
    let r = k.get(&url).send().await.map_err(|e| format!("{e}"))?;
    if r.status() == 401 || r.status() == 403 {
        return Err("Nøkkelen er ugyldig eller utløpt — lag en ny i portalen.".into());
    }
    if !r.status().is_success() {
        return Err(format!("Portalen svarte {}", r.status()));
    }
    r.json::<serde_json::Value>().await.map_err(|e| format!("{e}"))
}

fn bare_uten_redirect() -> reqwest::Client { reqwest::Client::builder().redirect(reqwest::redirect::Policy::none()).build().unwrap_or_default() }

fn trygt_navn(s: &str) -> String {
    s.chars().map(|c| if "\\/:*?\"<>|".contains(c) { '_' } else { c }).collect()
}

/// Last ned én fil med Range-resume. Skriver til «<navn>.part», flytter til
/// endelig navn når alt er nede. Finnes endelig fil med riktig størrelse → hopp.
async fn last_ned_en(
    app: &AppHandle,
    k: &reqwest::Client,
    bare: &reqwest::Client,
    portal: &str,
    fil: &Fil,
    rot: &Path,
    avbryt: &AtomicBool,
    struper: &Struper,
    konflikt: &str,
) -> Result<(), String> {
    // SIKRING (sweep 25/8): fil.sti kommer fra server-/delingsdata — vask hver
    // komponent (trygt_navn) og kast «..»/tomme, så en fiendtlig deling aldri
    // kan skrive utenfor nedlastingsmappa (`..\..\Startup`-klassikeren).
    let trygg_sti: std::path::PathBuf = fil.sti
        .split(['/', '\\'])
        .filter(|d| !d.is_empty() && *d != "." && *d != "..")
        .map(trygt_navn)
        .collect();
    let mappe = if trygg_sti.as_os_str().is_empty() { rot.to_path_buf() } else { rot.join(trygg_sti) };
    tokio::fs::create_dir_all(&mappe).await.map_err(|e| format!("{e}"))?;
    let mut maal = mappe.join(trygt_navn(&fil.filnavn));
    // Når fila finnes: 'hopp' (lik størrelse = ferdig, ellers overskriv — standard),
    // 'begge' (nytt navn «navn (2).ext»), 'overskriv' (alltid på nytt).
    if tokio::fs::metadata(&maal).await.is_ok() {
        match konflikt {
            "begge" => { let (stamme, ext) = match trygt_navn(&fil.filnavn).rsplit_once('.') { Some((a, b)) => (a.to_string(), format!(".{b}")), None => (trygt_navn(&fil.filnavn), String::new()) }; let mut n = 2; loop { let k = mappe.join(format!("{stamme} ({n}){ext}")); if tokio::fs::metadata(&k).await.is_err() { maal = k; break; } n += 1; } }
            "overskriv" => { let _ = tokio::fs::remove_file(&maal).await; }
            _ => {}
        }
    }
    let part = mappe.join(format!("{}.part", trygt_navn(&fil.filnavn)));

    if let Ok(m) = tokio::fs::metadata(&maal).await {
        if fil.bytes > 0 && m.len() == fil.bytes {
            // Død .part ved siden av komplett fil (kø 25/8): en avbrutt runde
            // etterlot resten — fila ER her, så resume-dataene er verdiløse.
            let _ = tokio::fs::remove_file(&part).await;
            let _ = app.emit("framdrift", Framdrift { id: fil.id.clone(), hentet: fil.bytes, total: fil.bytes, status: "hoppet".into(), feil: None });
            return Ok(());
        }
    }
    let allerede = tokio::fs::metadata(&part).await.map(|m| m.len()).unwrap_or(0);
    let hentet = Arc::new(AtomicU64::new(allerede));

    // 1) portalen → 302 (signert R2-URL). Cookien følger KUN hit.
    let url = if fil.url.is_empty() { format!("{}/api/rawskap/original/{}?last=1", portal.trim_end_matches('/'), fil.id) } else { fil.url.clone() };
    let r = if fil.url.is_empty() { k.get(&url) } else { bare_uten_redirect().get(&url) }.send().await.map_err(|e| format!("{e}"))?;
    let r2 = match r.status().as_u16() {
        301 | 302 | 303 | 307 | 308 => r
            .headers()
            .get(reqwest::header::LOCATION)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
            .ok_or("302 uten Location")?,
        403 => return Err("Blokkert (samtykke trukket/utløpt)".into()),
        401 => return Err("Nøkkelen er ugyldig — lag en ny".into()),
        s if (200..300).contains(&s) => url.clone(), // ikke R2 (Drive) → strøm fra portalen
        s => return Err(format!("Portalen svarte {s}")),
    };
    // 2) R2 med Range fra der vi slapp.
    let mut req = if r2 == url { k.get(&r2) } else { bare.get(&r2) };
    if allerede > 0 {
        req = req.header(reqwest::header::RANGE, format!("bytes={}-", allerede));
    }
    let resp = req.send().await.map_err(|e| format!("{e}"))?;
    let status = resp.status().as_u16();
    let (append, start) = match status {
        206 => (true, allerede),
        200 => (false, 0u64), // serveren ignorerte Range → start på nytt
        416 => { // alt er alt nede
            tokio::fs::rename(&part, &maal).await.map_err(|e| format!("{e}"))?;
            let _ = app.emit("framdrift", Framdrift { id: fil.id.clone(), hentet: fil.bytes, total: fil.bytes, status: "ferdig".into(), feil: None });
            return Ok(());
        }
        s => return Err(format!("Lageret svarte {s}")),
    };
    let total = if fil.bytes > 0 { fil.bytes } else { start + resp.content_length().unwrap_or(0) };
    hentet.store(start, Ordering::Relaxed);
    let mut f = tokio::fs::OpenOptions::new().create(true).write(true).append(append).truncate(!append).open(&part).await.map_err(|e| format!("{e}"))?;
    let mut strom = resp.bytes_stream();
    let mut sist_meldt = std::time::Instant::now();
    while let Some(bit) = strom.next().await {
        if avbryt.load(Ordering::Relaxed) { return Err("Avbrutt".into()); }
        let bit = bit.map_err(|e| format!("{e}"))?;
        f.write_all(&bit).await.map_err(|e| format!("{e}"))?;
        struper.tell(bit.len() as u64).await;
        let n = hentet.fetch_add(bit.len() as u64, Ordering::Relaxed) + bit.len() as u64;
        if sist_meldt.elapsed().as_millis() > 150 {
            sist_meldt = std::time::Instant::now();
            let _ = app.emit("framdrift", Framdrift { id: fil.id.clone(), hentet: n, total, status: "laster".into(), feil: None });
        }
    }
    f.flush().await.map_err(|e| format!("{e}"))?;
    drop(f);
    let n = hentet.load(Ordering::Relaxed);
    if fil.bytes > 0 && n != fil.bytes {
        return Err(format!("Ufullstendig: {} av {} bytes — prøv igjen (fortsetter der den slapp)", n, fil.bytes));
    }
    // Verifisering (22/8): har serveren xxh64, sjekkes fila før den får endelig navn.
    if !fil.xxh64.is_empty() {
        let _ = app.emit("framdrift", Framdrift { id: fil.id.clone(), hentet: n, total, status: "hash".into(), feil: None });
        let h = fil_xxh64(&part).await.unwrap_or_default();
        if h != fil.xxh64.to_lowercase() { let _ = tokio::fs::remove_file(&part).await; return Err("Verifisering feilet (xxHash) — lastet ned på nytt".into()); }
    }
    tokio::fs::rename(&part, &maal).await.map_err(|e| format!("{e}"))?;
    let _ = app.emit("framdrift", Framdrift { id: fil.id.clone(), hentet: n, total, status: if fil.xxh64.is_empty() { "ferdig".into() } else { "verifisert".into() }, feil: None });
    Ok(())
}

/// Last ned et sett filer til en mappe — `parallell` samtidige strømmer.
#[tauri::command]
async fn last_ned(app: AppHandle, tilstand: State<'_, Tilstand>, portal: String, nokkel: String, filer: Vec<Fil>, maal: String, parallell: usize, logg: bool, jobbnavn: String, konflikt: String) -> Result<serde_json::Value, String> {
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
    let bare = reqwest::Client::builder().build().map_err(|e| format!("{e}"))?;
    let rot = PathBuf::from(&maal);
    tokio::fs::create_dir_all(&rot).await.map_err(|e| format!("{e}"))?;
    let sem = Arc::new(Semaphore::new(parallell.clamp(1, 8)));
    let avbryt = tilstand.avbryt.clone();
    let struper = Struper::ny(tilstand.ned_mbps.clone());
    let mut jobber = Vec::new();
    for fil in filer {
        let (app, k, bare, portal, rot, sem, avbryt, loggen, struper) = (app.clone(), k.clone(), bare.clone(), portal.clone(), rot.clone(), sem.clone(), avbryt.clone(), loggen.clone(), struper.clone());
        let konflikt = konflikt.clone();
        jobber.push(tokio::spawn(async move {
            let _p = sem.acquire().await;
            let sti = if fil.sti.is_empty() { rot.join(trygt_navn(&fil.filnavn)) } else { rot.join(&fil.sti).join(trygt_navn(&fil.filnavn)) };
            if let Some(l) = &loggen { l.skriv(&format!("🚀 Startet                | ID: {} | {}", fil.id, sti.display())).await; }
            if avbryt.load(Ordering::Relaxed) { return (fil.id.clone(), Err::<(), String>("Avbrutt".into())); }
            let _ = app.emit("framdrift", Framdrift { id: fil.id.clone(), hentet: 0, total: fil.bytes, status: "laster".into(), feil: None });
            // Inntil 3 forsøk per fil — resume gjør hvert forsøk billig.
            let mut res = Err("".into());
            for _ in 0..3 {
                res = last_ned_en(&app, &k, &bare, &portal, &fil, &rot, &avbryt, &struper, &konflikt).await;
                if res.is_ok() || avbryt.load(Ordering::Relaxed) { break; }
                tokio::time::sleep(std::time::Duration::from_millis(800)).await;
            }
            if let Err(e) = &res {
                let _ = app.emit("framdrift", Framdrift { id: fil.id.clone(), hentet: 0, total: fil.bytes, status: "feil".into(), feil: Some(e.clone()) });
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
    let k = reqwest::Client::new();
    let r = k.post(format!("{}/api/rawskap/transfer/kobling", portal.trim_end_matches('/')))
        .json(&serde_json::json!({ "maskin": maskin })).send().await.map_err(|e| format!("{e}"))?;
    if !r.status().is_success() { return Err(format!("Portalen svarte {}", r.status())); }
    r.json::<serde_json::Value>().await.map_err(|e| format!("{e}"))
}

/// Device-kobling: poll til nøkkelen er godkjent i nettleseren.
#[tauri::command]
async fn kobling_poll(portal: String, kode: String) -> Result<serde_json::Value, String> {
    let k = reqwest::Client::new();
    let r = k.get(format!("{}/api/rawskap/transfer/kobling?kode={}", portal.trim_end_matches('/'), kode)).send().await.map_err(|e| format!("{e}"))?;
    if !r.status().is_success() { return Err(format!("Portalen svarte {}", r.status())); }
    r.json::<serde_json::Value>().await.map_err(|e| format!("{e}"))
}

// ── VIDEO via ffmpeg-sidecar (22/8, Vegards valg): poster + scrubbe-sprite +
// varighet/mål — SAMME kontrakt som nettleseren sender i fullfør, så serveren
// er uendret. ffmpeg (LGPL-bygg) ligger som sidecar; ffprobe droppet (115 MB)
// — metadata leses fra `ffmpeg -i` sin stderr.
struct VideoInfo { bredde: u32, hoyde: u32, varighet: f64, poster: Option<String>, sprite: Option<String>, frames: u32 }

fn er_video(navn: &str) -> bool { mime_fra(navn).starts_with("video/") || navn.to_ascii_lowercase().ends_with(".mxf") }

async fn ffmpeg_ut(app: &AppHandle, args: &[String]) -> Result<(Vec<u8>, String), String> {
    let cmd = app.shell().sidecar("ffmpeg").map_err(|e| format!("ffmpeg mangler: {e}"))?.args(args);
    let out = cmd.output().await.map_err(|e| format!("ffmpeg: {e}"))?;
    Ok((out.stdout, String::from_utf8_lossy(&out.stderr).to_string()))
}

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
async fn proxy_holder(app: &AppHandle, ut: &str, kilde: f64) -> bool {
    if tokio::fs::metadata(ut).await.map(|m| m.len() <= 10_000).unwrap_or(true) { return false; }
    if kilde <= 0.0 { return true; }  // ukjent kilde — da er størrelsen alt vi har, som før
    let Ok((_, err)) = ffmpeg_ut(app, &["-hide_banner".into(), "-i".into(), ut.into()]).await else { return true };
    // 90 %: et par bilder kan falle av i enden uten at proxyen er ubrukelig.
    parse_varighet(&err) >= kilde * 0.9
}

async fn video_info(app: &AppHandle, sti: &str) -> VideoInfo {
    let mut v = VideoInfo { bredde: 0, hoyde: 0, varighet: 0.0, poster: None, sprite: None, frames: 0 };
    let (_, err) = match ffmpeg_ut(app, &["-hide_banner".into(), "-i".into(), sti.into()]).await { Ok(x) => x, Err(_) => return v };
    v.varighet = parse_varighet(&err);
    let (w, h) = parse_dim(&err); v.bredde = w; v.hoyde = h;
    // Poster: midten, men maks 5 s inn (som nettleseren). Maks 1280 bred.
    let t = if v.varighet > 0.0 { (v.varighet / 2.0).min(5.0) } else { 1.0 };
    // Binær ut via stdout ødelegges av shell-pluginens tekstbehandling (22/8:
    // «glitch-poster») — skriv til temp-fil og les den.
    let tmp = std::env::temp_dir().join(format!("rawskap-poster-{}.jpg", std::process::id()));
    if ffmpeg_ut(app, &["-hide_banner".into(), "-loglevel".into(), "error".into(), "-y".into(), "-ss".into(), format!("{t:.2}"), "-i".into(), sti.into(), "-frames:v".into(), "1".into(), "-vf".into(), "scale='min(1280,iw)':-2".into(), "-q:v".into(), "4".into(), tmp.to_string_lossy().to_string()]).await.is_ok() {
        if let Ok(jpg) = tokio::fs::read(&tmp).await { if jpg.len() > 1000 { v.poster = Some(format!("data:image/jpeg;base64,{}", base64::engine::general_purpose::STANDARD.encode(&jpg))); } }
        let _ = tokio::fs::remove_file(&tmp).await;
    }
    // Sprite: N = clamp(round(dur), 12, 48) frames jevnt fordelt, 200 px høye, vannrett stripe.
    if v.varighet > 0.5 {
        let n = (v.varighet.round() as u32).clamp(12, 48);
        let fps = n as f64 / v.varighet;
        let vf = format!("fps={fps:.6},scale=-2:200,tile={n}x1");
        let tmp = std::env::temp_dir().join(format!("rawskap-sprite-{}.jpg", std::process::id()));
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
async fn lag_og_last_opp_proxy(app: &AppHandle, bare: &reqwest::Client, sti: &str, put_url: &str, id: &str, total: u64) -> Option<u64> {
    let _ = app.emit("framdrift", Framdrift { id: id.to_string(), hentet: total, total, status: "proxy".into(), feil: None });
    let ut = std::env::temp_dir().join(format!("rawskap-proxy-{}-{}.mp4", std::process::id(), rand_suffiks()));
    let ut_s = ut.to_string_lossy().to_string();
    // Fasiten vi måler resultatet mot.
    let kilde_varighet = {
        let (_, e) = ffmpeg_ut(app, &["-hide_banner".into(), "-i".into(), sti.into()]).await.unwrap_or_default();
        parse_varighet(&e)
    };

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
    let bytes = match tokio::fs::read(&ut).await { Ok(b) if b.len() > 10_000 => b, _ => { let _ = tokio::fs::remove_file(&ut).await; return None; } };
    let n = bytes.len() as u64;
    let svar = bare.put(put_url).header(reqwest::header::CONTENT_TYPE, "video/mp4").header(reqwest::header::CONTENT_LENGTH, n).body(bytes).send().await;
    let _ = tokio::fs::remove_file(&ut).await;
    match svar { Ok(r) if r.status().is_success() => Some(n), _ => None }
}

fn rand_suffiks() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    format!("{}", SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0) % 1_000_000)
}

// ── MULTIPART + RESUME (22/8): filer over DEL_GRENSE går i 64 MB-deler.
// Tilstand per fil ligger i en liten JSON i appens datamappe (nøkkel =
// xxh64 av sti|størrelse|mtime) — uploadId, originalKey, ferdige deler m/
// ETag. Starter man på nytt (nettbrudd, lukket app) fortsettes fra siste
// ferdige del. Rådes til å ha lifecycle-regel i R2 for forlatte multiparts.
const DEL_BYTES: u64 = 64 * 1024 * 1024;
const DEL_GRENSE: u64 = 96 * 1024 * 1024;

#[derive(Clone, Serialize, Deserialize, Default)]
struct Resume { upload_id: String, original_key: String, mappe_id: String, bytes: u64, deler: Vec<(u32, String)>, #[serde(default)] proxy_put: String }

fn resume_dir() -> PathBuf {
    let d = dirs::data_local_dir().unwrap_or(std::env::temp_dir()).join("RawskapTransfer").join("opplasting");
    let _ = std::fs::create_dir_all(&d); d
}
fn resume_sti(sti: &str, bytes: u64, mtime: u64) -> PathBuf {
    let h = xxhash_rust::xxh64::xxh64(format!("{sti}|{bytes}|{mtime}").as_bytes(), 0);
    resume_dir().join(format!("{h:016x}.json"))
}
fn resume_les(p: &Path) -> Option<Resume> { std::fs::read(p).ok().and_then(|b| serde_json::from_slice(&b).ok()) }
fn resume_skriv(p: &Path, r: &Resume) { if let Ok(b) = serde_json::to_vec(r) { let tmp = p.with_extension("tmp"); if std::fs::write(&tmp, b).is_ok() { let _ = std::fs::rename(&tmp, p); } } }

/// xxh64 av hele fila (1 MB-blokker). Brukes ved opplasting (lagres på raden)
/// og ved nedlasting (verifisering mot serverens verdi).
async fn fil_xxh64(sti: &Path) -> Result<String, String> { fil_xxh64_meld(sti, None).await }

/// xxh64 over hele fila. `meld` gir framdrift underveis (app, id, total) —
/// STORE filer bruker flere titalls sekunder her FØR første byte er sendt, og
/// uten melding ser appen ut som den henger på 0 kB/s (Vegard 28/8).
async fn fil_xxh64_meld(sti: &Path, meld: Option<(&AppHandle, &str, u64)>) -> Result<String, String> {
    use tokio::io::AsyncReadExt;
    let mut f = tokio::fs::File::open(sti).await.map_err(|e| format!("{e}"))?;
    let mut h = xxhash_rust::xxh64::Xxh64::new(0);
    let mut buf = vec![0u8; 1 << 20];
    let mut lest: u64 = 0;
    let mut sist_meldt = std::time::Instant::now();
    loop {
        let n = f.read(&mut buf).await.map_err(|e| format!("{e}"))?;
        if n == 0 { break; }
        h.update(&buf[..n]);
        lest += n as u64;
        if let Some((app, id, total)) = meld {
            if sist_meldt.elapsed().as_millis() > 200 {
                sist_meldt = std::time::Instant::now();
                let _ = app.emit("framdrift", Framdrift { id: id.to_string(), hentet: lest, total, status: "hash".into(), feil: None });
            }
        }
    }
    Ok(format!("{:016x}", h.digest()))
}

/// Starter en fersk multipart og gir resume-tilstanden tilbake.
/// Egen funksjon fordi den kalles to steder: ved ny opplasting, og når en
/// gjenopptatt opplasting viser seg å være død på serveren (30/8).
async fn multipart_start(k: &reqwest::Client, portal: &str, navn: &str, mime: &str, bytes: u64, sist: u64, mappe_id: &str) -> Result<Resume, String> {
    let resp = k.post(format!("{}/api/rawskap/opplasting", portal)).json(&serde_json::json!({ "action": "multipart-start", "filnavn": navn, "mimeType": mime, "filstorrelse": bytes, "sistEndret": sist, "mappeId": mappe_json(mappe_id) })).send().await.map_err(|e| format!("{e}"))?;
    let st = resp.status().as_u16();
    let d: serde_json::Value = resp.json().await.map_err(|e| format!("{e}"))?;
    if st == 401 || st == 403 { return Err("Ikke tilgang — logg inn på nytt".into()); }
    let (uid, key) = match (d["uploadId"].as_str(), d["originalKey"].as_str()) { (Some(u), Some(kk)) => (u.to_string(), kk.to_string()), _ => return Err(d["error"].as_str().unwrap_or("multipart-start feilet").to_string()) };
    // Serveren tilbyr en presignert URL for avspillingsproxyen (kun video).
    // Den lagres i resume-fila så en gjenopptatt opplasting ikke mister den.
    Ok(Resume { upload_id: uid, original_key: key, mappe_id: mappe_id.to_string(), bytes, deler: vec![], proxy_put: d["proxyPutUrl"].as_str().unwrap_or("").to_string() })
}

async fn last_opp_multipart(app: &AppHandle, k: &reqwest::Client, bare: &reqwest::Client, portal: &str, fil: &OppFil, mappe_id: &str, avbryt: &Arc<AtomicBool>, struper: Arc<Struper>, navn: &str, mime: &str, bytes: u64, sist: u64) -> Result<(String, u64, String), String> {
    use tokio::io::{AsyncReadExt, AsyncSeekExt};
    let rsti = resume_sti(&fil.sti, bytes, sist);
    let mut r = resume_les(&rsti).filter(|r| r.bytes == bytes && r.mappe_id == mappe_id).unwrap_or_default();
    // Rundturen til portalen (multipart-start, og siden signering av deler) tar
    // et lite oyeblikk der ingen bytes gaar. Si det, i stedet for a staa pa 0.
    let _ = app.emit("framdrift", Framdrift { id: fil.sti.clone(), hentet: 0, total: bytes, status: "kobler".into(), feil: None });
    if r.upload_id.is_empty() {
        r = multipart_start(k, portal, navn, mime, bytes, sist, mappe_id).await?;
        resume_skriv(&rsti, &r);
    }
    let antall = ((bytes + DEL_BYTES - 1) / DEL_BYTES) as u32;
    let mut f = tokio::fs::File::open(&fil.sti).await.map_err(|e| format!("{e}"))?;
    let mut hentet;
    // ⚠ DØD OPPLASTING (30/8): kjenner ikke serveren opplastingen igjen, er
    // resume-tilstanden verdiløs — og før dette var fila da PERMANENT brekt:
    // hvert nytt forsøk leste den samme døde nøkkelen fra resume-fila og ga
    // opp med en gang. Nå starter vi friskt i stedet. Én gang, så en server
    // som avviser alt ikke blir en evig løkke.
    let mut omstart_brukt = false;
    'omstart: loop {
    let ferdige: std::collections::HashSet<u32> = r.deler.iter().map(|(n, _)| *n).collect();
    hentet = ((ferdige.len() as u64) * DEL_BYTES).min(bytes);
    let _ = app.emit("framdrift", Framdrift { id: fil.sti.clone(), hentet, total: bytes, status: "laster".into(), feil: None });
    // Presigner deler i bolker på 20 (URL-ene lever 1 t).
    let mangler: Vec<u32> = (1..=antall).filter(|n| !ferdige.contains(n)).collect();
    for bolk in mangler.chunks(20) {
        if hentet == 0 { let _ = app.emit("framdrift", Framdrift { id: fil.sti.clone(), hentet, total: bytes, status: "kobler".into(), feil: None }); }
        let resp = k.post(format!("{}/api/rawskap/opplasting", portal)).json(&serde_json::json!({ "action": "multipart-deler", "originalKey": r.original_key, "uploadId": r.upload_id, "deler": bolk })).send().await.map_err(|e| format!("{e}"))?;
        let d: serde_json::Value = resp.json().await.map_err(|e| format!("{e}"))?;
        let urler: std::collections::HashMap<u32, String> = d["deler"].as_array().map(|a| a.iter().filter_map(|x| Some((x["nr"].as_u64()? as u32, x["url"].as_str()?.to_string()))).collect()).unwrap_or_default();
        if urler.is_empty() {
            if !omstart_brukt {
                omstart_brukt = true;
                let _ = app.emit("framdrift", Framdrift { id: fil.sti.clone(), hentet: 0, total: bytes, status: "kobler".into(), feil: None });
                r = multipart_start(k, portal, navn, mime, bytes, sist, mappe_id).await?;
                resume_skriv(&rsti, &r);
                continue 'omstart;
            }
            return Err(d["error"].as_str().unwrap_or("Kunne ikke signere deler — opplastingen kan være utløpt; prøv igjen").to_string());
        }
        for nr in bolk {
            if avbryt.load(Ordering::Relaxed) { return Err("Avbrutt".into()); }
            let url = urler.get(nr).ok_or("mangler URL for del")?;
            let start = (*nr as u64 - 1) * DEL_BYTES;
            let len = (bytes - start).min(DEL_BYTES);
            f.seek(std::io::SeekFrom::Start(start)).await.map_err(|e| format!("{e}"))?;
            let mut buf = vec![0u8; len as usize];
            f.read_exact(&mut buf).await.map_err(|e| format!("{e}"))?;
            struper.tell(len).await;
            // Én del = ett forsøk × 3 (ekte resume: ferdige deler røres aldri).
            //
            // ⚠ FRAMDRIFT UNDERVEIS (28/8, Vegard: «går fra 3.8 MB/s til 22 MB/s
            // hele tiden»): før meldte vi først NÅR en 64 MB-del var ferdig. Ved
            // ~10 MB/s betyr det at telleren står bom stille i ~6 sekunder og så
            // hopper 64 MB — og en fartsmåler som sampler hvert sekund ser
            // vekselvis 0 og 64 MB/s. Nå strømmes delen i 256 kB-biter med
            // teller, akkurat som én-PUT-løypa, så tallet blir ekte.
            let buf = Arc::new(buf);
            let mut etag = None;
            for _ in 0..3 {
                let app2 = app.clone();
                let id2 = fil.sti.clone();
                let base = hentet;
                let mut sist_meldt = std::time::Instant::now();
                let sendt = Arc::new(AtomicU64::new(0));
                let sendt2 = sendt.clone();
                let b = buf.clone();
                // Ved omforsøk starter delen på nytt fra `base` — ærlig, for
                // bytene MÅ sendes om igjen.
                let strom = futures_util::stream::unfold((0usize, b), |(pos, b)| async move {
                    if pos >= b.len() { return None; }
                    let slutt = (pos + (1 << 18)).min(b.len());
                    let bit = bytes::Bytes::copy_from_slice(&b[pos..slutt]);
                    Some((Ok::<bytes::Bytes, std::io::Error>(bit), (slutt, b)))
                }).inspect(move |r: &Result<bytes::Bytes, std::io::Error>| {
                    if let Ok(bit) = r {
                        let sendt_na = sendt2.fetch_add(bit.len() as u64, Ordering::Relaxed) + bit.len() as u64;
                        if sist_meldt.elapsed().as_millis() > 150 {
                            sist_meldt = std::time::Instant::now();
                            let _ = app2.emit("framdrift", Framdrift { id: id2.clone(), hentet: (base + sendt_na).min(bytes), total: bytes, status: "laster".into(), feil: None });
                        }
                    }
                });
                match bare.put(url).header(reqwest::header::CONTENT_LENGTH, len).body(reqwest::Body::wrap_stream(strom)).send().await {
                    Ok(resp) if resp.status().is_success() => { etag = resp.headers().get("etag").and_then(|v| v.to_str().ok()).map(|s| s.trim_matches('"').to_string()); break; }
                    Ok(resp) => { if resp.status().as_u16() == 403 { break; } }
                    Err(_) => {}
                }
                tokio::time::sleep(std::time::Duration::from_millis(1200)).await;
            }
            let etag = etag.ok_or_else(|| format!("Del {nr} feilet — prøv igjen (fortsetter fra del {nr})"))?;
            r.deler.push((*nr, etag)); resume_skriv(&rsti, &r);
            hentet = (hentet + len).min(bytes);
            let _ = app.emit("framdrift", Framdrift { id: fil.sti.clone(), hentet, total: bytes, status: "laster".into(), feil: None });
        }
    }
    break;
    }
    // Sett sammen. R2 bruker maalbar tid pa a lime sammen 100+ deler — det er
    // her «star lenge pa 100 %» begynner.
    let _ = app.emit("framdrift", Framdrift { id: fil.sti.clone(), hentet: bytes, total: bytes, status: "setter".into(), feil: None });
    let deler: Vec<serde_json::Value> = r.deler.iter().map(|(n, e)| serde_json::json!({ "nr": n, "etag": e })).collect();
    let resp = k.post(format!("{}/api/rawskap/opplasting", portal)).json(&serde_json::json!({ "action": "multipart-fullfor", "originalKey": r.original_key, "uploadId": r.upload_id, "deler": deler })).send().await.map_err(|e| format!("{e}"))?;
    let d: serde_json::Value = resp.json().await.unwrap_or(serde_json::json!({}));
    if !d["ok"].as_bool().unwrap_or(false) { return Err(d["error"].as_str().unwrap_or("Kunne ikke sette sammen fila").to_string()); }
    let _ = std::fs::remove_file(&rsti);
    Ok((r.original_key.clone(), hentet, r.proxy_put.clone()))
}

// ── OPPLASTING (22/8): samme løype som nettleseren — presign → PUT rett til
// R2 → fullfør (serveren lager thumb/EXIF for bilder). Undermapper gjenskapes
// i skapet (mapper-API, cache per sti). Video får ingen poster i v0.
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct OppFil { pub sti: String, pub relativ: String, pub bytes: u64 }

fn mime_fra(navn: &str) -> &'static str {
    let e = navn.rsplit('.').next().unwrap_or("").to_ascii_lowercase();
    match e.as_str() {
        "jpg" | "jpeg" => "image/jpeg", "png" => "image/png", "webp" => "image/webp", "gif" => "image/gif", "heic" => "image/heic", "tif" | "tiff" => "image/tiff",
        "dng" => "image/x-adobe-dng", "arw" => "image/x-sony-arw", "cr2" => "image/x-canon-cr2", "cr3" => "image/x-canon-cr3", "nef" => "image/x-nikon-nef", "raf" => "image/x-fuji-raf",
        "mp4" => "video/mp4", "mov" => "video/quicktime", "mxf" => "application/mxf", "pdf" => "application/pdf", _ => "application/octet-stream",
    }
}

fn mappe_json(id: &str) -> serde_json::Value { if id.is_empty() { serde_json::Value::Null } else { serde_json::Value::String(id.to_string()) } }

async fn sikre_mappe(k: &reqwest::Client, portal: &str, rot: &str, relativ_dir: &str, cache: &tokio::sync::Mutex<std::collections::HashMap<String, String>>) -> Result<String, String> {
    if relativ_dir.is_empty() { return Ok(rot.to_string()); }
    let mut forelder = rot.to_string(); let mut sti = String::new();
    for del in relativ_dir.split('/').filter(|d| !d.is_empty()) {
        sti = if sti.is_empty() { del.to_string() } else { format!("{sti}/{del}") };
        let mut c = cache.lock().await;
        if let Some(id) = c.get(&sti) { forelder = id.clone(); continue; }
        let r = k.post(format!("{}/api/rawskap/mapper", portal)).json(&serde_json::json!({ "navn": del, "forelderId": mappe_json(&forelder) })).send().await.map_err(|e| format!("{e}"))?;
        let d: serde_json::Value = r.json().await.map_err(|e| format!("{e}"))?;
        let id = d["id"].as_str().ok_or_else(|| format!("Kunne ikke lage mappe «{del}»: {}", d["error"].as_str().unwrap_or("?")))?.to_string();
        c.insert(sti.clone(), id.clone()); forelder = id;
    }
    Ok(forelder)
}

/// Fil → byte-strøm m/ teller (PUT-framdrift).
fn fil_strom(f: tokio::fs::File, struper: Arc<Struper>, avbryt: Arc<AtomicBool>, mut tell: impl FnMut(u64) + Send + 'static) -> impl futures_util::Stream<Item = Result<bytes::Bytes, std::io::Error>> + Send + 'static {
    use tokio::io::AsyncReadExt;
    futures_util::stream::unfold((f, struper, avbryt), |(mut f, struper, avbryt)| async move {
        // Avbryt MIDT i en PUT (22/8-bug: en gjenglemt opplasting levde videre etter
        // reload og rapporterte til samme rad som den nye) — kutt strømmen, så
        // feiler PUT-en og opprydding (action avbryt) kjører.
        if avbryt.load(Ordering::Relaxed) { return Some((Err(std::io::Error::other("Avbrutt")), (f, struper, avbryt))); }
        let mut buf = vec![0u8; 1 << 20];
        match f.read(&mut buf).await {
            Ok(0) => None,
            Ok(n) => { buf.truncate(n); struper.tell(n as u64).await; Some((Ok(bytes::Bytes::from(buf)), (f, struper, avbryt))) }
            Err(e) => Some((Err(e), (f, struper, avbryt))),
        }
    }).inspect(move |r| { if let Ok(b) = r { tell(b.len() as u64); } })
}

async fn last_opp_en(app: &AppHandle, k: &reqwest::Client, bare: &reqwest::Client, portal: &str, fil: &OppFil, mappe_id: &str, avbryt: &Arc<AtomicBool>, struper: Arc<Struper>) -> Result<(), String> {
    let navn = std::path::Path::new(&fil.sti).file_name().and_then(|n| n.to_str()).unwrap_or("fil").to_string();
    let mime = mime_fra(&navn);
    let meta = tokio::fs::metadata(&fil.sti).await.map_err(|e| format!("{e}"))?;
    let bytes = meta.len();
    let sist = meta.modified().ok().and_then(|m| m.duration_since(std::time::UNIX_EPOCH).ok()).map(|d| d.as_millis() as u64).unwrap_or(0);
    // xxh64 mens vi likevel leser — lagres på raden, verifiseres ved nedlasting.
    let _ = app.emit("framdrift", Framdrift { id: fil.sti.clone(), hentet: 0, total: bytes, status: "hash".into(), feil: None });
    let xxh = fil_xxh64_meld(std::path::Path::new(&fil.sti), Some((app, &fil.sti, bytes))).await.ok();
    // Store filer: multipart m/ resume. Små: én PUT som før.
    if bytes > DEL_GRENSE {
        let (key, _, proxy_put) = last_opp_multipart(app, k, bare, portal, fil, mappe_id, avbryt, struper.clone(), &navn, mime, bytes, sist).await?;
        let proxy = lag_proxy_hvis_video(app, bare, &fil.sti, &navn, &proxy_put, bytes).await;
        return fullfor_opplasting(app, k, portal, fil, &key, &navn, mime, bytes, sist, mappe_id, xxh, proxy).await;
    }
    // 1) presign
    let _ = app.emit("framdrift", Framdrift { id: fil.sti.clone(), hentet: 0, total: bytes, status: "kobler".into(), feil: None });
    let r = k.post(format!("{}/api/rawskap/opplasting", portal)).json(&serde_json::json!({ "action": "presign", "filnavn": navn, "mimeType": mime, "filstorrelse": bytes, "sistEndret": sist, "mappeId": mappe_json(mappe_id) })).send().await.map_err(|e| format!("{e}"))?;
    let st = r.status().as_u16();
    if st == 401 || st == 403 { return Err("Ikke tilgang — logg inn på nytt (API-nøkler kan ikke laste opp)".into()); }
    let d: serde_json::Value = r.json().await.map_err(|e| format!("{e}"))?;
    if d["kvoteSperre"].as_bool().unwrap_or(false) { return Err(d["error"].as_str().unwrap_or("Lagringen er full").to_string()); }
    let (url, key) = match (d["uploadUrl"].as_str(), d["originalKey"].as_str()) { (Some(u), Some(k)) => (u.to_string(), k.to_string()), _ => return Err(d["error"].as_str().unwrap_or("presign feilet").to_string()) };
    let proxy_put = d["proxyPutUrl"].as_str().unwrap_or("").to_string();
    // 2) PUT rett til R2 m/ framdrift
    let f = tokio::fs::File::open(&fil.sti).await.map_err(|e| format!("{e}"))?;
    let id = fil.sti.clone(); let app2 = app.clone();
    let sendt = Arc::new(AtomicU64::new(0)); let sendt2 = sendt.clone();
    let mut sist_meldt = std::time::Instant::now();
    let strom = fil_strom(f, struper, avbryt.clone(), move |n| {
        let t = sendt2.fetch_add(n, Ordering::Relaxed) + n;
        if sist_meldt.elapsed().as_millis() > 150 || t == bytes { sist_meldt = std::time::Instant::now(); let _ = app2.emit("framdrift", Framdrift { id: id.clone(), hentet: t, total: bytes, status: "laster".into(), feil: None }); }
    });
    let resp = bare.put(&url).header(reqwest::header::CONTENT_TYPE, mime).header(reqwest::header::CONTENT_LENGTH, bytes).body(reqwest::Body::wrap_stream(strom)).send().await;
    if avbryt.load(Ordering::Relaxed) || resp.is_err() && avbryt.load(Ordering::Relaxed) {
        let _ = k.post(format!("{}/api/rawskap/opplasting", portal)).json(&serde_json::json!({ "action": "avbryt", "originalKeys": [key] })).send().await;
        return Err("Avbrutt".into());
    }
    let resp = resp.map_err(|e| format!("{e}"))?;
    if !resp.status().is_success() { return Err(format!("Lageret svarte {}", resp.status())); }
    if false {
        let _ = k.post(format!("{}/api/rawskap/opplasting", portal)).json(&serde_json::json!({ "action": "avbryt", "originalKeys": [key] })).send().await;
        return Err("Avbrutt".into());
    }
    let proxy = lag_proxy_hvis_video(app, bare, &fil.sti, &navn, &proxy_put, bytes).await;
    fullfor_opplasting(app, k, portal, fil, &key, &navn, mime, bytes, sist, mappe_id, xxh, proxy).await
}

/// Bare video, og bare når serveren faktisk tilbød en proxy-URL.
async fn lag_proxy_hvis_video(app: &AppHandle, bare: &reqwest::Client, sti: &str, navn: &str, put_url: &str, total: u64) -> Option<u64> {
    if put_url.is_empty() || !er_video(navn) { return None; }
    lag_og_last_opp_proxy(app, bare, sti, put_url, sti, total).await
}

/// Fullfør-steget (server: thumb/EXIF/dedup for bilder; video: poster/sprite fra ffmpeg her).
async fn fullfor_opplasting(app: &AppHandle, k: &reqwest::Client, portal: &str, fil: &OppFil, key: &str, navn: &str, mime: &str, bytes: u64, sist: u64, mappe_id: &str, xxh: Option<String>, proxy: Option<u64>) -> Result<(), String> {
    let mut body = serde_json::json!({ "action": "fullfor", "originalKey": key, "filnavn": navn, "mimeType": mime, "filstorrelse": bytes, "sistEndret": sist, "mappeId": mappe_json(mappe_id) });
    if let Some(x) = xxh { body["xxh64"] = serde_json::json!(x); }
    // Proxyen ligger alt i lageret — si fra, så slipper serveren å be Cloudflare
    // Stream (som feiler på ProRes) og Rawcode å laste ned originalen på nytt.
    if let Some(n) = proxy {
        // ⚠ MÅ være bit for bit lik serverens `proxyNokkel` — den avviser en
        // nøkkel den ikke selv ville laget. Reservefallet bruker det TRIMMEDE
        // navnet; `unwrap_or(key)` ville sneket «originals/» inn igjen.
        let basis = key.trim_start_matches("originals/");
        let basis = basis.rsplit_once('.').map(|(a, _)| a).unwrap_or(basis);
        body["proxyKey"] = serde_json::json!(format!("proxy/{basis}.mp4"));
        body["proxyStorrelse"] = serde_json::json!(n);
    }
    // Serveren lager thumb/EXIF/dedup her; for video kjorer ffmpeg LOKALT forst
    // (poster + sprite). Begge deler skjer ETTER at siste byte er sendt — uten
    // status sto appen bare stille pa 100 % (Vegard 28/8).
    let _ = app.emit("framdrift", Framdrift { id: fil.sti.clone(), hentet: bytes, total: bytes, status: "fullfor".into(), feil: None });
    if er_video(navn) {
        let _ = app.emit("framdrift", Framdrift { id: fil.sti.clone(), hentet: bytes, total: bytes, status: "thumbs".into(), feil: None });
        let vi = video_info(app, &fil.sti).await;
        if vi.bredde > 0 { body["bredde"] = serde_json::json!(vi.bredde); body["hoyde"] = serde_json::json!(vi.hoyde); }
        if vi.varighet > 0.0 { body["varighet"] = serde_json::json!(vi.varighet); }
        if let Some(p) = vi.poster { body["posterBase64"] = serde_json::json!(p); }
        if let Some(sp) = vi.sprite { body["spriteBase64"] = serde_json::json!(sp); body["spriteFrames"] = serde_json::json!(vi.frames); }
    }
    let r = k.post(format!("{}/api/rawskap/opplasting", portal)).json(&body).send().await.map_err(|e| format!("{e}"))?;
    let st = r.status().as_u16();
    let d: serde_json::Value = r.json().await.unwrap_or(serde_json::json!({}));
    if !(200..300).contains(&st) { return Err(format!("fullfør: {}", d["error"].as_str().unwrap_or("feilet"))); }
    let status = if d["allerede"].as_bool().unwrap_or(false) { "hoppet" } else { "ferdig" };
    let _ = app.emit("framdrift", Framdrift { id: fil.sti.clone(), hentet: bytes, total: bytes, status: status.into(), feil: None });
    Ok(())
}

#[tauri::command]
async fn last_opp(app: AppHandle, tilstand: State<'_, Tilstand>, portal: String, nokkel: String, filer: Vec<OppFil>, mappe_id: String, parallell: usize) -> Result<serde_json::Value, String> {
    tilstand.avbryt.store(false, Ordering::Relaxed);
    let portal = portal.trim_end_matches('/').to_string();
    let k = klient(&nokkel)?;
    let bare = reqwest::Client::builder().build().map_err(|e| format!("{e}"))?;
    let cache = Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::<String, String>::new()));
    let sem = Arc::new(Semaphore::new(parallell.clamp(1, 6)));
    let avbryt = tilstand.avbryt.clone();
    let struper = Struper::ny(tilstand.opp_mbps.clone());
    let mut jobber = Vec::new();
    for fil in filer {
        let (app, k, bare, portal, cache, sem, avbryt, mappe_id, struper) = (app.clone(), k.clone(), bare.clone(), portal.clone(), cache.clone(), sem.clone(), avbryt.clone(), mappe_id.clone(), struper.clone());
        jobber.push(tokio::spawn(async move {
            let _p = sem.acquire().await;
            if avbryt.load(Ordering::Relaxed) { return (fil.sti.clone(), Err::<(), String>("Avbrutt".into())); }
            let _ = app.emit("framdrift", Framdrift { id: fil.sti.clone(), hentet: 0, total: fil.bytes, status: "laster".into(), feil: None });
            let rel_dir = std::path::Path::new(&fil.relativ).parent().map(|p| p.to_string_lossy().replace('\\', "/")).unwrap_or_default();
            let res = match sikre_mappe(&k, &portal, &mappe_id, &rel_dir, &cache).await {
                Ok(mid) => { let mut r = Err(String::new()); for _ in 0..2 { r = last_opp_en(&app, &k, &bare, &portal, &fil, &mid, &avbryt, struper.clone()).await; if r.is_ok() || avbryt.load(Ordering::Relaxed) { break; } } r }
                Err(e) => Err(e),
            };
            if let Err(e) = &res { let _ = app.emit("framdrift", Framdrift { id: fil.sti.clone(), hentet: 0, total: fil.bytes, status: "feil".into(), feil: Some(e.clone()) }); }
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
    let k = klient(&nokkel)?;
    let r = k.post(format!("{}/api/rawskap/mapper", portal.trim_end_matches('/'))).json(&serde_json::json!({ "navn": navn, "forelderId": mappe_json(&forelder) })).send().await.map_err(|e| format!("{e}"))?;
    if r.status() == 401 || r.status() == 403 { return Err("Ikke tilgang — logg inn på nytt".into()); }
    let d: serde_json::Value = r.json().await.map_err(|e| format!("{e}"))?;
    if d["id"].as_str().is_none() { return Err(d["error"].as_str().unwrap_or("Kunne ikke lage mappe").to_string()); }
    Ok(d)
}

/// Legg filer i papirkurven (brukes av «Erstatt» ved opplasting).
#[tauri::command]
async fn slett_filer(portal: String, nokkel: String, ids: Vec<String>) -> Result<(), String> {
    let k = klient(&nokkel)?;
    let r = k.post(format!("{}/api/rawskap/slett", portal.trim_end_matches('/'))).json(&serde_json::json!({ "assetIds": ids })).send().await.map_err(|e| format!("{e}"))?;
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

#[tauri::command]
async fn sjekk_versjon() -> Result<String, String> {
    let r = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build().map_err(|e| format!("{e}"))?
        .get("https://rawskap.no/api/transfer/versjon")
        .send().await.map_err(|e| format!("{e}"))?;
    r.text().await.map_err(|e| format!("{e}"))
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
fn synk_ferdig_skriv(id: &str, sett: &std::collections::HashSet<String>) { if let Ok(b) = serde_json::to_vec(sett) { let _ = std::fs::write(synk_ferdig_sti(id), b); } }
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

/// Deling → fil-liste (token = auth). Brukes av rawskap://deling/<token>.
#[tauri::command]
async fn hent_deling(portal: String, token: String) -> Result<serde_json::Value, String> {
    let k = reqwest::Client::new();
    let r = k.get(format!("{}/api/bildebank/samling/transfer-liste?token={}", portal.trim_end_matches('/'), token)).send().await.map_err(|e| format!("{e}"))?;
    let st = r.status().as_u16();
    let d: serde_json::Value = r.json().await.map_err(|e| format!("{e}"))?;
    if !(200..300).contains(&st) { return Err(d["error"].as_str().unwrap_or("Kunne ikke hente delingen").to_string()); }
    Ok(d)
}

/// Semantisk søk i skapet (samme motor som nettsøket) → [{id, score}].
#[tauri::command]
async fn sok(portal: String, nokkel: String, q: String) -> Result<serde_json::Value, String> {
    let k = klient(&nokkel)?;
    let r = k.get(format!("{}/api/rawskap/sok-semantisk?q={}", portal.trim_end_matches('/'), urlenc(&q))).send().await.map_err(|e| format!("{e}"))?;
    let st = r.status().as_u16();
    let d: serde_json::Value = r.json().await.map_err(|e| format!("{e}"))?;
    if st == 503 { return Err("Søk i skapet er ikke satt opp på kontoen".into()); }
    if !(200..300).contains(&st) { return Err(d["error"].as_str().unwrap_or("søket feilet").to_string()); }
    Ok(d)
}
fn urlenc(s: &str) -> String { s.bytes().map(|b| match b { b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => (b as char).to_string(), _ => format!("%{b:02X}") }).collect() }

#[tauri::command]
fn avbryt(tilstand: State<'_, Tilstand>) {
    tilstand.avbryt.store(true, Ordering::Relaxed);
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
        .manage(Tilstand::default())
        .manage(SynkTilstand::default())
        .invoke_handler(tauri::generate_handler![hent_liste, hent_deling, sok, last_ned, last_opp, les_mappe, ny_mappe, er_mappe, vis_i_utforsker, sjekk_versjon, sett_tray_tekst, rydd_part_i_mappe, slett_filer, sett_nettverk, synk_sett, synk_merk, sett_til_kurv, avbryt, kobling_start, kobling_poll, maskinnavn])
        .run(tauri::generate_context!())
        .expect("Rawskap Transfer kunne ikke starte");
}
