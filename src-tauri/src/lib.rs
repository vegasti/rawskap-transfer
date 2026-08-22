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
async fn last_ned(app: AppHandle, tilstand: State<'_, Tilstand>, portal: String, nokkel: String, filer: Vec<Fil>, maal: String, parallell: usize) -> Result<serde_json::Value, String> {
    tilstand.avbryt.store(false, Ordering::Relaxed);
    let k = klient(&nokkel)?;
    let bare = reqwest::Client::builder().build().map_err(|e| format!("{e}"))?;
    let rot = PathBuf::from(&maal);
    tokio::fs::create_dir_all(&rot).await.map_err(|e| format!("{e}"))?;
    let sem = Arc::new(Semaphore::new(parallell.clamp(1, 8)));
    let avbryt = tilstand.avbryt.clone();
    let mut jobber = Vec::new();
    for fil in filer {
        let (app, k, bare, portal, rot, sem, avbryt) = (app.clone(), k.clone(), bare.clone(), portal.clone(), rot.clone(), sem.clone(), avbryt.clone());
        jobber.push(tokio::spawn(async move {
            let _p = sem.acquire().await;
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
            }
            (fil.id.clone(), res)
        }));
    }
    let mut ok = 0usize; let mut feil = Vec::new();
    for j in jobber {
        match j.await { Ok((_, Ok(()))) => ok += 1, Ok((id, Err(e))) => feil.push(serde_json::json!({ "id": id, "feil": e })), Err(e) => feil.push(serde_json::json!({ "feil": format!("{e}") })) }
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
        .invoke_handler(tauri::generate_handler![hent_liste, last_ned, avbryt, kobling_start, kobling_poll, maskinnavn])
        .run(tauri::generate_context!())
        .expect("Rawskap Transfer kunne ikke starte");
}
