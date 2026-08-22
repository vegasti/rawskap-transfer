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
use tokio::io::AsyncWriteExt;
use tokio::sync::Semaphore;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Fil {
    pub id: String,
    pub filnavn: String,
    pub bytes: u64,
    #[serde(default)]
    pub sti: String,
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
) -> Result<(), String> {
    let mappe = if fil.sti.is_empty() { rot.to_path_buf() } else { rot.join(&fil.sti) };
    tokio::fs::create_dir_all(&mappe).await.map_err(|e| format!("{e}"))?;
    let maal = mappe.join(trygt_navn(&fil.filnavn));
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
    tokio::fs::rename(&part, &maal).await.map_err(|e| format!("{e}"))?;
    let _ = app.emit("framdrift", Framdrift { id: fil.id.clone(), hentet: n, total, status: "ferdig".into(), feil: None });
    Ok(())
}

/// Last ned et sett filer til en mappe — `parallell` samtidige strømmer.
#[tauri::command]
async fn last_ned(app: AppHandle, tilstand: State<'_, Tilstand>, portal: String, nokkel: String, filer: Vec<Fil>, maal: String, parallell: usize, logg: bool, jobbnavn: String) -> Result<serde_json::Value, String> {
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
    let mut jobber = Vec::new();
    for fil in filer {
        let (app, k, bare, portal, rot, sem, avbryt, loggen) = (app.clone(), k.clone(), bare.clone(), portal.clone(), rot.clone(), sem.clone(), avbryt.clone(), loggen.clone());
        jobber.push(tokio::spawn(async move {
            let _p = sem.acquire().await;
            let sti = if fil.sti.is_empty() { rot.join(trygt_navn(&fil.filnavn)) } else { rot.join(&fil.sti).join(trygt_navn(&fil.filnavn)) };
            if let Some(l) = &loggen { l.skriv(&format!("🚀 Startet                | ID: {} | {}", fil.id, sti.display())).await; }
            if avbryt.load(Ordering::Relaxed) { return (fil.id.clone(), Err::<(), String>("Avbrutt".into())); }
            let _ = app.emit("framdrift", Framdrift { id: fil.id.clone(), hentet: 0, total: fil.bytes, status: "laster".into(), feil: None });
            // Inntil 3 forsøk per fil — resume gjør hvert forsøk billig.
            let mut res = Err("".into());
            for _ in 0..3 {
                res = last_ned_en(&app, &k, &bare, &portal, &fil, &rot, &avbryt).await;
                if res.is_ok() || avbryt.load(Ordering::Relaxed) { break; }
                tokio::time::sleep(std::time::Duration::from_millis(800)).await;
            }
            if let Err(e) = &res {
                let _ = app.emit("framdrift", Framdrift { id: fil.id.clone(), hentet: 0, total: fil.bytes, status: "feil".into(), feil: Some(e.clone()) });
                if let Some(l) = &loggen { l.skriv(&format!("❌ {:<22} | ID: {} | {}", if e == "Avbrutt" { "Avbrutt" } else { "Feilet" }, fil.id, e)).await; }
            } else if let Some(l) = &loggen { l.skriv(&format!("✅ Ferdig & størrelse ok  | {:>12} B | ID: {} | {}", fil.bytes, fil.id, sti.display())).await; }
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
fn fil_strom(f: tokio::fs::File, mut tell: impl FnMut(u64) + Send + 'static) -> impl futures_util::Stream<Item = Result<bytes::Bytes, std::io::Error>> + Send + 'static {
    use tokio::io::AsyncReadExt;
    futures_util::stream::unfold(f, |mut f| async move {
        let mut buf = vec![0u8; 1 << 20];
        match f.read(&mut buf).await {
            Ok(0) => None,
            Ok(n) => { buf.truncate(n); Some((Ok(bytes::Bytes::from(buf)), f)) }
            Err(e) => Some((Err(e), f)),
        }
    }).inspect(move |r| { if let Ok(b) = r { tell(b.len() as u64); } })
}

async fn last_opp_en(app: &AppHandle, k: &reqwest::Client, bare: &reqwest::Client, portal: &str, fil: &OppFil, mappe_id: &str, avbryt: &AtomicBool) -> Result<(), String> {
    let navn = std::path::Path::new(&fil.sti).file_name().and_then(|n| n.to_str()).unwrap_or("fil").to_string();
    let mime = mime_fra(&navn);
    let meta = tokio::fs::metadata(&fil.sti).await.map_err(|e| format!("{e}"))?;
    let bytes = meta.len();
    let sist = meta.modified().ok().and_then(|m| m.duration_since(std::time::UNIX_EPOCH).ok()).map(|d| d.as_millis() as u64).unwrap_or(0);
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
    let strom = fil_strom(f, move |n| {
        let t = sendt2.fetch_add(n, Ordering::Relaxed) + n;
        if sist_meldt.elapsed().as_millis() > 150 || t == bytes { sist_meldt = std::time::Instant::now(); let _ = app2.emit("framdrift", Framdrift { id: id.clone(), hentet: t, total: bytes, status: "laster".into(), feil: None }); }
    });
    let resp = bare.put(&url).header(reqwest::header::CONTENT_TYPE, mime).header(reqwest::header::CONTENT_LENGTH, bytes).body(reqwest::Body::wrap_stream(strom)).send().await.map_err(|e| format!("{e}"))?;
    if !resp.status().is_success() { return Err(format!("Lageret svarte {}", resp.status())); }
    if avbryt.load(Ordering::Relaxed) {
        let _ = k.post(format!("{}/api/rawskap/opplasting", portal)).json(&serde_json::json!({ "action": "avbryt", "originalKeys": [key] })).send().await;
        return Err("Avbrutt".into());
    }
    // 3) fullfør (server: thumb/EXIF/dedup for bilder)
    let r = k.post(format!("{}/api/rawskap/opplasting", portal)).json(&serde_json::json!({ "action": "fullfor", "originalKey": key, "filnavn": navn, "mimeType": mime, "filstorrelse": bytes, "sistEndret": sist, "mappeId": mappe_json(mappe_id) })).send().await.map_err(|e| format!("{e}"))?;
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
    let mut jobber = Vec::new();
    for fil in filer {
        let (app, k, bare, portal, cache, sem, avbryt, mappe_id) = (app.clone(), k.clone(), bare.clone(), portal.clone(), cache.clone(), sem.clone(), avbryt.clone(), mappe_id.clone());
        jobber.push(tokio::spawn(async move {
            let _p = sem.acquire().await;
            if avbryt.load(Ordering::Relaxed) { return (fil.sti.clone(), Err::<(), String>("Avbrutt".into())); }
            let _ = app.emit("framdrift", Framdrift { id: fil.sti.clone(), hentet: 0, total: fil.bytes, status: "laster".into(), feil: None });
            let rel_dir = std::path::Path::new(&fil.relativ).parent().map(|p| p.to_string_lossy().replace('\\', "/")).unwrap_or_default();
            let res = match sikre_mappe(&k, &portal, &mappe_id, &rel_dir, &cache).await {
                Ok(mid) => { let mut r = Err(String::new()); for _ in 0..2 { r = last_opp_en(&app, &k, &bare, &portal, &fil, &mid, &avbryt).await; if r.is_ok() || avbryt.load(Ordering::Relaxed) { break; } } r }
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

#[tauri::command]
async fn er_mappe(sti: String) -> bool { tokio::fs::metadata(&sti).await.map(|m| m.is_dir()).unwrap_or(false) }

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
        .manage(Tilstand::default())
        .invoke_handler(tauri::generate_handler![hent_liste, last_ned, last_opp, les_mappe, ny_mappe, er_mappe, avbryt, kobling_start, kobling_poll, maskinnavn])
        .run(tauri::generate_context!())
        .expect("Rawskap Transfer kunne ikke starte");
}
