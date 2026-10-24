// Skapet: kobling av maskinen, mappetre og filer, og mapper som må finnes før opplasting.
use crate::{enkel_klient, mappe_json, nettfeil, Feil, Klient};

fn nett(e: reqwest::Error) -> Feil { Feil::Nett(nettfeil(e)) }

/// Hent mappetre + filer fra portalen.
pub async fn hent_liste(k: &Klient, mappe: &str) -> Result<serde_json::Value, Feil> {
    let url = format!("{}/api/rawskap/transfer/liste?mappe={}", k.portal(), mappe);
    let r = k.api().get(&url).send().await.map_err(nett)?;
    if r.status() == 401 || r.status() == 403 {
        return Err(Feil::IkkeTilgang("Nøkkelen er ugyldig eller utløpt — lag en ny i portalen.".into()));
    }
    if !r.status().is_success() {
        return Err(Feil::server(r.status().as_u16(), format!("Portalen svarte {}", r.status())));
    }
    r.json::<serde_json::Value>().await.map_err(nett)
}

/// Device-kobling (webviewen kan ikke fetch-e portalen — CORS): start → kode.
pub async fn kobling_start(portal: &str, maskin: &str) -> Result<serde_json::Value, Feil> {
    let k = enkel_klient();
    let r = k.post(format!("{}/api/rawskap/transfer/kobling", portal.trim_end_matches('/')))
        .json(&serde_json::json!({ "maskin": maskin })).send().await.map_err(nett)?;
    if !r.status().is_success() { return Err(Feil::server(r.status().as_u16(), format!("Portalen svarte {}", r.status()))); }
    r.json::<serde_json::Value>().await.map_err(nett)
}

/// Device-kobling: poll til nøkkelen er godkjent i nettleseren.
///
/// ⚠ `hemmelighet` (18/9) er den lange strengen `kobling_start` fikk tilbake.
/// De seks tegnene i koden er til å SE PÅ — de leses høyt av skjermen — mens
/// det er hemmeligheten som beviser at det er VI som spør. Uten den var koden
/// alene nok til å hente ut en 180-dagers sesjon fra et uinnlogget endepunkt.
/// Tom streng = eldre portal som ennå ikke sender den; da polles som før.
pub async fn kobling_poll(portal: &str, kode: &str, hemmelighet: &str) -> Result<serde_json::Value, Feil> {
    let k = enkel_klient();
    let mut url = format!("{}/api/rawskap/transfer/kobling?kode={}", portal.trim_end_matches('/'), kode);
    // Hemmeligheten er ren hex fra serveren — ingen prosentkoding trengs. Vi
    // filtrerer likevel, så et uventet svar aldri kan sette sammen en annen URL.
    let rent: String = hemmelighet.chars().filter(|c| c.is_ascii_alphanumeric()).collect();
    if !rent.is_empty() {
        url.push_str(&format!("&h={rent}"));
    }
    let r = k.get(url).send().await.map_err(nett)?;
    if !r.status().is_success() { return Err(Feil::server(r.status().as_u16(), format!("Portalen svarte {}", r.status()))); }
    r.json::<serde_json::Value>().await.map_err(nett)
}

/// Sørger for at undermappene i `relativ_dir` finnes under `rot` i skapet, og gir id-en til den
/// innerste. `cache` husker sti → id for hele jobben, så hver mappe lages én gang.
pub async fn sikre_mappe(k: &Klient, rot: &str, relativ_dir: &str, cache: &tokio::sync::Mutex<std::collections::HashMap<String, String>>) -> Result<String, Feil> {
    if relativ_dir.is_empty() { return Ok(rot.to_string()); }
    let mut forelder = rot.to_string(); let mut sti = String::new();
    for del in relativ_dir.split('/').filter(|d| !d.is_empty()) {
        sti = if sti.is_empty() { del.to_string() } else { format!("{sti}/{del}") };
        let mut c = cache.lock().await;
        if let Some(id) = c.get(&sti) { forelder = id.clone(); continue; }
        let r = k.api().post(format!("{}/api/rawskap/mapper", k.portal())).json(&serde_json::json!({ "navn": del, "forelderId": mappe_json(&forelder) })).send().await.map_err(nett)?;
        let st = r.status().as_u16();
        let d: serde_json::Value = r.json().await.map_err(nett)?;
        let id = d["id"].as_str().ok_or_else(|| Feil::server(st, format!("Kunne ikke lage mappe «{del}»: {}", d["error"].as_str().unwrap_or("?"))))?.to_string();
        c.insert(sti.clone(), id.clone()); forelder = id;
    }
    Ok(forelder)
}

/// Ny mappe i skapet (høyreklikk / verktøylinja).
pub async fn ny_mappe(k: &Klient, navn: &str, forelder: &str) -> Result<serde_json::Value, Feil> {
    let r = k.api().post(format!("{}/api/rawskap/mapper", k.portal())).json(&serde_json::json!({ "navn": navn, "forelderId": mappe_json(forelder) })).send().await.map_err(nett)?;
    if r.status() == 401 || r.status() == 403 { return Err(Feil::IkkeTilgang("Ikke tilgang — logg inn på nytt".into())); }
    let st = r.status().as_u16();
    let d: serde_json::Value = r.json().await.map_err(nett)?;
    if d["id"].as_str().is_none() { return Err(Feil::server(st, d["error"].as_str().unwrap_or("Kunne ikke lage mappe"))); }
    Ok(d)
}
