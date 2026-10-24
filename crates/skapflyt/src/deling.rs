// Delinger: hente en deling som fil-liste, låse opp med passord, og opprette en mappe-deling.
use crate::{enkel_klient, klient, nettfeil, urlenc};

/// Deling → fil-liste (token = auth). Brukes av rawskap://deling/<token>.
///
/// `nokkel` er en KORTLEVD ADGANGSNØKKEL for passord-/inviterte delinger
/// (19/9). Token-kunnskap alene holder ikke der: serveren krever at man har
/// passert porten. Cookien som beviser det bor i nettleseren og kan ikke nå
/// hit, så delingssiden signerer i stedet en nøkkel som gjelder i 10 minutter
/// og legger den i deep-linken. Tom streng for åpne delinger.
pub async fn hent_deling(portal: String, token: String, nokkel: Option<String>) -> Result<serde_json::Value, String> {
    let k = enkel_klient();
    let n = nokkel.unwrap_or_default();
    let hale = if n.is_empty() { String::new() } else { format!("&n={}", urlenc(&n)) };
    let r = k.get(format!("{}/api/bildebank/samling/transfer-liste?token={}{}", portal.trim_end_matches('/'), token, hale)).send().await.map_err(nettfeil)?;
    let st = r.status().as_u16();
    let d: serde_json::Value = r.json().await.map_err(nettfeil)?;
    // 401 = låst deling uten gyldig nøkkel. Serveren sier HVA slags lås det er,
    // så frontenden vet om den kan spørre om passord (passord) eller må sende
    // brukeren til nettleseren (inviterte — der finnes bare e-postlenka).
    if st == 401 { return Err(format!("DELING_LAAST:{}", d["tilgang"].as_str().unwrap_or("passord"))); }
    if !(200..300).contains(&st) { return Err(d["error"].as_str().unwrap_or("Kunne ikke hente delingen").to_string()); }
    Ok(d)
}

/// Passord → kortlevd adgangsnøkkel for en passordbeskyttet deling (19/9).
///
/// Brukes når Transfer står uten nøkkel: en jobb som har ligget over sju dager,
/// eller en vanlig /d/-lenke limt rett inn i appen. ⚠ Passordet sendes ÉN gang
/// og lagres ingen steder — verken her eller i frontenden. Serveren har samme
/// grense mot gjetting som nettleserens opplåsing.
pub async fn deling_passord(portal: String, token: String, passord: String) -> Result<String, String> {
    let r = enkel_klient()
        .post(format!("{}/api/bildebank/samling/transfer-nokkel", portal.trim_end_matches('/')))
        .json(&serde_json::json!({ "token": token, "passord": passord }))
        .send().await.map_err(nettfeil)?;
    let st = r.status().as_u16();
    let d: serde_json::Value = r.json().await.unwrap_or(serde_json::Value::Null);
    match st {
        200..=299 => d["nokkel"].as_str().map(|s| s.to_string()).ok_or_else(|| "Serveren svarte uten nøkkel".to_string()),
        429 => Err(d["error"].as_str().unwrap_or("For mange forsøk — vent litt").to_string()),
        _ => Err(d["error"].as_str().unwrap_or("Feil passord").to_string()),
    }
}

/// DEL FRA APPEN (0.2.0): opprett en LEVENDE mappe-deling — samme API som
/// portalen bruker (mappeRef = innholdet unioneres ved visning, ikke
/// snapshot), og kundens egne delings-defaults hentes først så en deling
/// laget her ser ut som en deling laget der. `ua` er appens User-Agent (se `klient`).
pub async fn del_mappe(portal: String, nokkel: String, mappe_id: String, navn: String, ua: &'static str) -> Result<serde_json::Value, String> {
    let portal = portal.trim_end_matches('/').to_string();
    let k = klient(&nokkel, ua)?;
    let mut body = serde_json::json!({});
    if let Ok(r) = k.get(format!("{}/api/rawskap/deling-defaults", portal)).send().await {
        if let Ok(d) = r.json::<serde_json::Value>().await {
            if let Some(o) = d["defaults"].as_object() { body = serde_json::Value::Object(o.clone()); }
        }
    }
    body["navn"] = serde_json::json!(navn);
    body["mappeRef"] = serde_json::json!(mappe_id);
    body["bildeIds"] = serde_json::json!([]);
    let r = k.post(format!("{}/api/rawskap/del", portal)).json(&body).send().await.map_err(nettfeil)?;
    let d: serde_json::Value = r.json().await.map_err(nettfeil)?;
    let token = d["token"].as_str().unwrap_or("").to_string();
    if token.is_empty() { return Err(d["error"].as_str().unwrap_or("Kunne ikke opprette deling").to_string()); }
    Ok(serde_json::json!({ "token": token, "id": d["id"] }))
}
