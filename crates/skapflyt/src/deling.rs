// Delinger: hente en deling som fil-liste, låse opp med passord, og opprette en mappe-deling.
use crate::{enkel_klient, nettfeil, urlenc, Feil, Klient};

fn nett(e: reqwest::Error) -> Feil { Feil::Nett(nettfeil(e)) }

/// Deling → fil-liste (token = auth). Brukes av rawskap://deling/<token>.
///
/// `nokkel` er en KORTLEVD ADGANGSNØKKEL for passord-/inviterte delinger
/// (19/9). Token-kunnskap alene holder ikke der: serveren krever at man har
/// passert porten. Cookien som beviser det bor i nettleseren og kan ikke nå
/// hit, så delingssiden signerer i stedet en nøkkel som gjelder i 10 minutter
/// og legger den i deep-linken. Tom streng for åpne delinger.
pub async fn hent_deling(portal: &str, token: &str, nokkel: Option<&str>) -> Result<serde_json::Value, Feil> {
    let k = enkel_klient();
    let n = nokkel.unwrap_or_default();
    let hale = if n.is_empty() { String::new() } else { format!("&n={}", urlenc(n)) };
    let r = k.get(format!("{}/api/bildebank/samling/transfer-liste?token={}{}", portal.trim_end_matches('/'), token, hale)).send().await.map_err(nett)?;
    let st = r.status().as_u16();
    let d: serde_json::Value = r.json().await.map_err(nett)?;
    // 401 = låst deling uten gyldig nøkkel. Serveren sier HVA slags lås det er,
    // så frontenden vet om den kan spørre om passord (passord) eller må sende
    // brukeren til nettleseren (inviterte — der finnes bare e-postlenka).
    if st == 401 { return Err(Feil::DelingLaast(d["tilgang"].as_str().unwrap_or("passord").to_string())); }
    if !(200..300).contains(&st) { return Err(Feil::server(st, d["error"].as_str().unwrap_or("Kunne ikke hente delingen"))); }
    Ok(d)
}

/// Passord → kortlevd adgangsnøkkel for en passordbeskyttet deling (19/9).
///
/// Brukes når Transfer står uten nøkkel: en jobb som har ligget over sju dager,
/// eller en vanlig /d/-lenke limt rett inn i appen. ⚠ Passordet sendes ÉN gang
/// og lagres ingen steder — verken her eller i frontenden. Serveren har samme
/// grense mot gjetting som nettleserens opplåsing.
pub async fn deling_passord(portal: &str, token: &str, passord: &str) -> Result<String, Feil> {
    let r = enkel_klient()
        .post(format!("{}/api/bildebank/samling/transfer-nokkel", portal.trim_end_matches('/')))
        .json(&serde_json::json!({ "token": token, "passord": passord }))
        .send().await.map_err(nett)?;
    let st = r.status().as_u16();
    let d: serde_json::Value = r.json().await.unwrap_or(serde_json::Value::Null);
    match st {
        200..=299 => d["nokkel"].as_str().map(|s| s.to_string()).ok_or_else(|| Feil::server(st, "Serveren svarte uten nøkkel")),
        429 => Err(Feil::server(st, d["error"].as_str().unwrap_or("For mange forsøk — vent litt"))),
        _ => Err(Feil::server(st, d["error"].as_str().unwrap_or("Feil passord"))),
    }
}

/// DEL FRA APPEN (0.2.0): opprett en LEVENDE mappe-deling — samme API som
/// portalen bruker (mappeRef = innholdet unioneres ved visning, ikke
/// snapshot), og kundens egne delings-defaults hentes først så en deling
/// laget her ser ut som en deling laget der.
pub async fn del_mappe(k: &Klient, mappe_id: &str, navn: &str) -> Result<serde_json::Value, Feil> {
    let portal = k.portal();
    let mut body = serde_json::json!({});
    if let Ok(r) = k.api().get(format!("{}/api/rawskap/deling-defaults", portal)).send().await {
        if let Ok(d) = r.json::<serde_json::Value>().await {
            if let Some(o) = d["defaults"].as_object() { body = serde_json::Value::Object(o.clone()); }
        }
    }
    body["navn"] = serde_json::json!(navn);
    body["mappeRef"] = serde_json::json!(mappe_id);
    body["bildeIds"] = serde_json::json!([]);
    let r = k.api().post(format!("{}/api/rawskap/del", portal)).json(&body).send().await.map_err(nett)?;
    let st = r.status().as_u16();
    let d: serde_json::Value = r.json().await.map_err(nett)?;
    let token = d["token"].as_str().unwrap_or("").to_string();
    if token.is_empty() { return Err(Feil::server(st, d["error"].as_str().unwrap_or("Kunne ikke opprette deling"))); }
    Ok(serde_json::json!({ "token": token, "id": d["id"] }))
}
