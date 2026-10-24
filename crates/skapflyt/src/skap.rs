// Skapet: kobling av maskinen, mappetre og filer, og mapper som må finnes før opplasting.
use crate::{enkel_klient, klient, mappe_json, nettfeil};

/// Hent mappetre + filer fra portalen. `ua` er appens User-Agent (se `klient`).
pub async fn hent_liste(portal: String, nokkel: String, mappe: String, ua: &'static str) -> Result<serde_json::Value, String> {
    let k = klient(&nokkel, ua)?;
    let url = format!("{}/api/rawskap/transfer/liste?mappe={}", portal.trim_end_matches('/'), mappe);
    let r = k.get(&url).send().await.map_err(nettfeil)?;
    if r.status() == 401 || r.status() == 403 {
        return Err("Nøkkelen er ugyldig eller utløpt — lag en ny i portalen.".into());
    }
    if !r.status().is_success() {
        return Err(format!("Portalen svarte {}", r.status()));
    }
    r.json::<serde_json::Value>().await.map_err(nettfeil)
}

/// Device-kobling (webviewen kan ikke fetch-e portalen — CORS): start → kode.
pub async fn kobling_start(portal: String, maskin: String) -> Result<serde_json::Value, String> {
    let k = enkel_klient();
    let r = k.post(format!("{}/api/rawskap/transfer/kobling", portal.trim_end_matches('/')))
        .json(&serde_json::json!({ "maskin": maskin })).send().await.map_err(nettfeil)?;
    if !r.status().is_success() { return Err(format!("Portalen svarte {}", r.status())); }
    r.json::<serde_json::Value>().await.map_err(nettfeil)
}

/// Device-kobling: poll til nøkkelen er godkjent i nettleseren.
///
/// ⚠ `hemmelighet` (18/9) er den lange strengen `kobling_start` fikk tilbake.
/// De seks tegnene i koden er til å SE PÅ — de leses høyt av skjermen — mens
/// det er hemmeligheten som beviser at det er VI som spør. Uten den var koden
/// alene nok til å hente ut en 180-dagers sesjon fra et uinnlogget endepunkt.
/// Tom streng = eldre portal som ennå ikke sender den; da polles som før.
pub async fn kobling_poll(portal: String, kode: String, hemmelighet: String) -> Result<serde_json::Value, String> {
    let k = enkel_klient();
    let mut url = format!("{}/api/rawskap/transfer/kobling?kode={}", portal.trim_end_matches('/'), kode);
    // Hemmeligheten er ren hex fra serveren — ingen prosentkoding trengs. Vi
    // filtrerer likevel, så et uventet svar aldri kan sette sammen en annen URL.
    let rent: String = hemmelighet.chars().filter(|c| c.is_ascii_alphanumeric()).collect();
    if !rent.is_empty() {
        url.push_str(&format!("&h={rent}"));
    }
    let r = k.get(url).send().await.map_err(nettfeil)?;
    if !r.status().is_success() { return Err(format!("Portalen svarte {}", r.status())); }
    r.json::<serde_json::Value>().await.map_err(nettfeil)
}

/// Sørger for at undermappene i `relativ_dir` finnes under `rot` i skapet, og gir id-en til den
/// innerste. `cache` husker sti → id for hele jobben, så hver mappe lages én gang.
pub async fn sikre_mappe(k: &reqwest::Client, portal: &str, rot: &str, relativ_dir: &str, cache: &tokio::sync::Mutex<std::collections::HashMap<String, String>>) -> Result<String, String> {
    if relativ_dir.is_empty() { return Ok(rot.to_string()); }
    let mut forelder = rot.to_string(); let mut sti = String::new();
    for del in relativ_dir.split('/').filter(|d| !d.is_empty()) {
        sti = if sti.is_empty() { del.to_string() } else { format!("{sti}/{del}") };
        let mut c = cache.lock().await;
        if let Some(id) = c.get(&sti) { forelder = id.clone(); continue; }
        let r = k.post(format!("{}/api/rawskap/mapper", portal)).json(&serde_json::json!({ "navn": del, "forelderId": mappe_json(&forelder) })).send().await.map_err(nettfeil)?;
        let d: serde_json::Value = r.json().await.map_err(nettfeil)?;
        let id = d["id"].as_str().ok_or_else(|| format!("Kunne ikke lage mappe «{del}»: {}", d["error"].as_str().unwrap_or("?")))?.to_string();
        c.insert(sti.clone(), id.clone()); forelder = id;
    }
    Ok(forelder)
}

/// Ny mappe i skapet (høyreklikk / verktøylinja). `ua` er appens User-Agent (se `klient`).
pub async fn ny_mappe(portal: String, nokkel: String, navn: String, forelder: String, ua: &'static str) -> Result<serde_json::Value, String> {
    let k = klient(&nokkel, ua)?;
    let r = k.post(format!("{}/api/rawskap/mapper", portal.trim_end_matches('/'))).json(&serde_json::json!({ "navn": navn, "forelderId": mappe_json(&forelder) })).send().await.map_err(nettfeil)?;
    if r.status() == 401 || r.status() == 403 { return Err("Ikke tilgang — logg inn på nytt".into()); }
    let d: serde_json::Value = r.json().await.map_err(nettfeil)?;
    if d["id"].as_str().is_none() { return Err(d["error"].as_str().unwrap_or("Kunne ikke lage mappe").to_string()); }
    Ok(d)
}
