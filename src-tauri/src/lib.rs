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
    let mappe = if fil.sti.is_empty() { rot.to_path_buf() } else { rot.join(&fil.sti) };
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
            let _ = app.emit("framdrift", Framdrift { id: fil.id.clone(), hentet: fil.bytes, total: fil.bytes, status: "hoppet".into(), feil: None });
            return Ok(());
        }
    }
    let allerede = tokio::fs::metadata(&part).await.map(|m| m.len()).unwrap_or(0);
    let hentet = Arc::new(AtomicU64::new(allerede));

    // 1) portalen → 302 (signert R2-URL). Cookien følger KUN hit.
    let url = format!("{}/api/rawskap/original/{}?last=1", portal.trim_end_matches('/'), fil.id);
    let r = k.get(&url).send().await.map_err(|e| format!("{e}"))?;
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

async fn video_info(app: &AppHandle, sti: &str) -> VideoInfo {
    let mut v = VideoInfo { bredde: 0, hoyde: 0, varighet: 0.0, poster: None, sprite: None, frames: 0 };
    let (_, err) = match ffmpeg_ut(app, &["-hide_banner".into(), "-i".into(), sti.into()]).await { Ok(x) => x, Err(_) => return v };
    v.varighet = parse_varighet(&err);
    let (w, h) = parse_dim(&err); v.bredde = w; v.hoyde = h;
    // Poster: midten, men maks 5 s inn (som nettleseren). Maks 1280 bred.
    let t = if v.varighet > 0.0 { (v.varighet / 2.0).min(5.0) } else { 1.0 };
    if let Ok((png, _)) = ffmpeg_ut(app, &["-hide_banner".into(), "-loglevel".into(), "error".into(), "-ss".into(), format!("{t:.2}"), "-i".into(), sti.into(), "-frames:v".into(), "1".into(), "-vf".into(), "scale='min(1280,iw)':-2".into(), "-q:v".into(), "4".into(), "-f".into(), "image2".into(), "-c:v".into(), "mjpeg".into(), "-".into()]).await {
        if png.len() > 1000 { v.poster = Some(format!("data:image/jpeg;base64,{}", base64::engine::general_purpose::STANDARD.encode(&png))); }
    }
    // Sprite: N = clamp(round(dur), 12, 48) frames jevnt fordelt, 200 px høye, vannrett stripe.
    if v.varighet > 0.5 {
        let n = (v.varighet.round() as u32).clamp(12, 48);
        let fps = n as f64 / v.varighet;
        let vf = format!("fps={fps:.6},scale=-2:200,tile={n}x1");
        if let Ok((jpg, _)) = ffmpeg_ut(app, &["-hide_banner".into(), "-loglevel".into(), "error".into(), "-i".into(), sti.into(), "-vf".into(), vf, "-frames:v".into(), "1".into(), "-q:v".into(), "5".into(), "-f".into(), "image2".into(), "-c:v".into(), "mjpeg".into(), "-".into()]).await {
            if jpg.len() > 1000 { v.sprite = Some(format!("data:image/jpeg;base64,{}", base64::engine::general_purpose::STANDARD.encode(&jpg))); v.frames = n; }
        }
    }
    v
}

// ── MULTIPART + RESUME (22/8): filer over DEL_GRENSE går i 64 MB-deler.
// Tilstand per fil ligger i en liten JSON i appens datamappe (nøkkel =
// xxh64 av sti|størrelse|mtime) — uploadId, originalKey, ferdige deler m/
// ETag. Starter man på nytt (nettbrudd, lukket app) fortsettes fra siste
// ferdige del. Rådes til å ha lifecycle-regel i R2 for forlatte multiparts.
const DEL_BYTES: u64 = 64 * 1024 * 1024;
const DEL_GRENSE: u64 = 96 * 1024 * 1024;

#[derive(Clone, Serialize, Deserialize, Default)]
struct Resume { upload_id: String, original_key: String, mappe_id: String, bytes: u64, deler: Vec<(u32, String)> }

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
async fn fil_xxh64(sti: &Path) -> Result<String, String> {
    use tokio::io::AsyncReadExt;
    let mut f = tokio::fs::File::open(sti).await.map_err(|e| format!("{e}"))?;
    let mut h = xxhash_rust::xxh64::Xxh64::new(0);
    let mut buf = vec![0u8; 1 << 20];
    loop { let n = f.read(&mut buf).await.map_err(|e| format!("{e}"))?; if n == 0 { break; } h.update(&buf[..n]); }
    Ok(format!("{:016x}", h.digest()))
}

async fn last_opp_multipart(app: &AppHandle, k: &reqwest::Client, bare: &reqwest::Client, portal: &str, fil: &OppFil, mappe_id: &str, avbryt: &Arc<AtomicBool>, struper: Arc<Struper>, navn: &str, mime: &str, bytes: u64, sist: u64) -> Result<(String, u64), String> {
    use tokio::io::{AsyncReadExt, AsyncSeekExt};
    let rsti = resume_sti(&fil.sti, bytes, sist);
    let mut r = resume_les(&rsti).filter(|r| r.bytes == bytes && r.mappe_id == mappe_id).unwrap_or_default();
    if r.upload_id.is_empty() {
        let resp = k.post(format!("{}/api/rawskap/opplasting", portal)).json(&serde_json::json!({ "action": "multipart-start", "filnavn": navn, "mimeType": mime, "filstorrelse": bytes, "sistEndret": sist, "mappeId": mappe_json(mappe_id) })).send().await.map_err(|e| format!("{e}"))?;
        let st = resp.status().as_u16();
        let d: serde_json::Value = resp.json().await.map_err(|e| format!("{e}"))?;
        if st == 401 || st == 403 { return Err("Ikke tilgang — logg inn på nytt".into()); }
        let (uid, key) = match (d["uploadId"].as_str(), d["originalKey"].as_str()) { (Some(u), Some(kk)) => (u.to_string(), kk.to_string()), _ => return Err(d["error"].as_str().unwrap_or("multipart-start feilet").to_string()) };
        r = Resume { upload_id: uid, original_key: key, mappe_id: mappe_id.to_string(), bytes, deler: vec![] };
        resume_skriv(&rsti, &r);
    }
    let antall = ((bytes + DEL_BYTES - 1) / DEL_BYTES) as u32;
    let ferdige: std::collections::HashSet<u32> = r.deler.iter().map(|(n, _)| *n).collect();
    let mut hentet = ferdige.len() as u64 * DEL_BYTES;
    if hentet > bytes { hentet = bytes; }
    let _ = app.emit("framdrift", Framdrift { id: fil.sti.clone(), hentet, total: bytes, status: "laster".into(), feil: None });
    let mut f = tokio::fs::File::open(&fil.sti).await.map_err(|e| format!("{e}"))?;
    // Presigner deler i bolker på 20 (URL-ene lever 1 t).
    let mangler: Vec<u32> = (1..=antall).filter(|n| !ferdige.contains(n)).collect();
    for bolk in mangler.chunks(20) {
        let resp = k.post(format!("{}/api/rawskap/opplasting", portal)).json(&serde_json::json!({ "action": "multipart-deler", "originalKey": r.original_key, "uploadId": r.upload_id, "deler": bolk })).send().await.map_err(|e| format!("{e}"))?;
        let d: serde_json::Value = resp.json().await.map_err(|e| format!("{e}"))?;
        let urler: std::collections::HashMap<u32, String> = d["deler"].as_array().map(|a| a.iter().filter_map(|x| Some((x["nr"].as_u64()? as u32, x["url"].as_str()?.to_string()))).collect()).unwrap_or_default();
        if urler.is_empty() { return Err(d["error"].as_str().unwrap_or("Kunne ikke signere deler — opplastingen kan være utløpt; prøv igjen").to_string()); }
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
            let mut etag = None;
            for _ in 0..3 {
                match bare.put(url).header(reqwest::header::CONTENT_LENGTH, len).body(buf.clone()).send().await {
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
    // Sett sammen
    let deler: Vec<serde_json::Value> = r.deler.iter().map(|(n, e)| serde_json::json!({ "nr": n, "etag": e })).collect();
    let resp = k.post(format!("{}/api/rawskap/opplasting", portal)).json(&serde_json::json!({ "action": "multipart-fullfor", "originalKey": r.original_key, "uploadId": r.upload_id, "deler": deler })).send().await.map_err(|e| format!("{e}"))?;
    let d: serde_json::Value = resp.json().await.unwrap_or(serde_json::json!({}));
    if !d["ok"].as_bool().unwrap_or(false) { return Err(d["error"].as_str().unwrap_or("Kunne ikke sette sammen fila").to_string()); }
    let _ = std::fs::remove_file(&rsti);
    Ok((r.original_key.clone(), hentet))
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
    let xxh = fil_xxh64(std::path::Path::new(&fil.sti)).await.ok();
    // Store filer: multipart m/ resume. Små: én PUT som før.
    if bytes > DEL_GRENSE {
        let (key, _) = last_opp_multipart(app, k, bare, portal, fil, mappe_id, avbryt, struper.clone(), &navn, mime, bytes, sist).await?;
        return fullfor_opplasting(app, k, portal, fil, &key, &navn, mime, bytes, sist, mappe_id, xxh).await;
    }
    // 1) presign
    let r = k.post(format!("{}/api/rawskap/opplasting", portal)).json(&serde_json::json!({ "action": "presign", "filnavn": navn, "mimeType": mime, "filstorrelse": bytes, "sistEndret": sist, "mappeId": mappe_json(mappe_id) })).send().await.map_err(|e| format!("{e}"))?;
    let st = r.status().as_u16();
    if st == 401 || st == 403 { return Err("Ikke tilgang — logg inn på nytt (API-nøkler kan ikke laste opp)".into()); }
    let d: serde_json::Value = r.json().await.map_err(|e| format!("{e}"))?;
    if d["kvoteSperre"].as_bool().unwrap_or(false) { return Err(d["error"].as_str().unwrap_or("Lagringen er full").to_string()); }
    let (url, key) = match (d["uploadUrl"].as_str(), d["originalKey"].as_str()) { (Some(u), Some(k)) => (u.to_string(), k.to_string()), _ => return Err(d["error"].as_str().unwrap_or("presign feilet").to_string()) };
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
    fullfor_opplasting(app, k, portal, fil, &key, &navn, mime, bytes, sist, mappe_id, xxh).await
}

/// Fullfør-steget (server: thumb/EXIF/dedup for bilder; video: poster/sprite fra ffmpeg her).
async fn fullfor_opplasting(app: &AppHandle, k: &reqwest::Client, portal: &str, fil: &OppFil, key: &str, navn: &str, mime: &str, bytes: u64, sist: u64, mappe_id: &str, xxh: Option<String>) -> Result<(), String> {
    let mut body = serde_json::json!({ "action": "fullfor", "originalKey": key, "filnavn": navn, "mimeType": mime, "filstorrelse": bytes, "sistEndret": sist, "mappeId": mappe_json(mappe_id) });
    if let Some(x) = xxh { body["xxh64"] = serde_json::json!(x); }
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

#[tauri::command]
fn avbryt(tilstand: State<'_, Tilstand>) {
    tilstand.avbryt.store(true, Ordering::Relaxed);
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_shell::init())
        .plugin(tauri_plugin_store::Builder::default().build())
        .plugin(tauri_plugin_notification::init())
        .manage(Tilstand::default())
        .manage(SynkTilstand::default())
        .invoke_handler(tauri::generate_handler![hent_liste, last_ned, last_opp, les_mappe, ny_mappe, er_mappe, slett_filer, sett_nettverk, synk_sett, synk_merk, avbryt, kobling_start, kobling_poll, maskinnavn])
        .run(tauri::generate_context!())
        .expect("Rawskap Transfer kunne ikke starte");
}
