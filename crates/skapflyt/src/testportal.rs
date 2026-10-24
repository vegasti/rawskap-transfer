// ── FALSK PORTAL + LAGER (0.3.0) ─────────────────────────────────────────────────────────────
//
// Nett-logikken er det som ødelegger data eller henger appen når den er feil, og den kan ikke
// prøves mot prod uten å rote til et ekte skap. Denne serveren snakker det appen bruker av
// /api/rawskap/opplasting og R2, og kan be om uhell: kutte forbindelsen midt i en del, svare
// 500 eller 403, eller slutte å lese (en død linje).
//
// Bak feature `testportal` (cratens egne tester har den alltid), så andre som bruker craten kan
// teste mot den samme. ⚠ Den skal følge portalens route.ts: et nytt felt i ruta er et nytt felt
// her i samme endring.
//
// T2 (24/10, route.ts på portal ea77e32a — P1): pending-rader (presign og multipart-start lager dem, avbryt og et
// endelig nei på versjonAv rydder dem, fullfør krever dem), raden fullfør lager (`id`, `allerede`), proxy-tilbudet
// (`proxyKey` + `proxyPutUrl` for video og MXF), og `versjonAv`: forsjekk ved presign/multipart-start, og ved fullfør
// (BARE kallets felt, eierskapet på nøkkelen først) OK (`versjonAv: <rot>`), endelig nei (`ryddet` = noe ble fjernet,
// `borte: true` da; `id` + `ryddet: false` når feiekosten alt laget raden) og `midlertidig` (503, alt står).

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

#[derive(Clone, Copy, Debug)]
pub enum Uhell { Status(u16), Kutt(usize), Stopp(usize) }

/// Hva serveren sier om en `versjonAv`.
#[derive(Clone, Debug, PartialEq)]
pub enum TestUtgave {
    /// Gyldig mål; `rot` er stakkens rot.
    Ok { rot: String },
    /// Avvist med `versjonAvFeil` = koden (ugyldig, avledning, ukjent, slettet, frosset).
    Nei(&'static str),
    /// Fullfør fikk ikke sjekket målet (503, alt står). Forsjekken slipper den gjennom, som ruta.
    Midlertidig,
}

/// En pending-rad: nøkkelen er utstedt og venter på fullfør.
#[derive(Clone, Debug, Default)]
pub struct Pending {
    /// Multipartens id; None = enkel PUT, eller multiparten er satt sammen (ruta setter UploadId = null).
    pub upload_id: Option<String>,
    /// Målet fra forsjekken (ruta lagrer det på pending-raden — for feiekosten; fullfør bruker bare kallets).
    pub versjon_av: String,
}

/// En rad i skapet.
#[derive(Clone, Debug, Default)]
pub struct Rad { pub id: String, pub versjon_av: String }

#[derive(Default)]
pub struct Lager {
    pub deler: BTreeMap<u32, Vec<u8>>,
    pub objekt: Option<Vec<u8>>,
    /// PUT-forsøk per del (0 = enkel PUT).
    pub forsok: HashMap<u32, u32>,
    /// (del, forsøk nr.) → uhell.
    pub uhell: HashMap<(u32, u32), Uhell>,
    pub handlinger: Vec<serde_json::Value>,
    pub signeringer: u32,
    pub del_bytes: u64,
    /// Multipartene som er startet (ny id for hver: u1, u2 …).
    pub startet: u32,
    pub nedlasting: Vec<u8>,
    /// GET-forsøk nr. → uhell.
    pub ned_uhell: HashMap<u32, Uhell>,
    pub ned_range: Vec<Option<u64>>,
    /// originalKey → pending-rad.
    pub pending: HashMap<String, Pending>,
    /// originalKey → raden fullfør laget (eller feiekosten).
    pub rader: HashMap<String, Rad>,
    /// Nøklene `avbryt` (eller et endelig nei) ryddet.
    pub ryddet: Vec<String>,
    /// versjonAv-mål → svaret ved forsjekk og fullfør. Mangler = Ok med målet som rot.
    pub utgaver: HashMap<String, TestUtgave>,
    /// Overstyrer `utgaver` ved fullfør (målet ble slettet etter at bytene gikk, osv.).
    pub utgaver_fullfor: HashMap<String, TestUtgave>,
    /// En portal fra før P1: et endelig nei uten `ryddet`, og ingen rydding.
    pub eldre_server: bool,
    /// Ryddingen ved et endelig nei fjerner ingenting (feilet hos serveren): `ryddet: false`.
    pub rydding_feiler: bool,
    /// Overstyrer prefikset i proxy-nøkkelen serveren tilbyr (som om konvensjonen var endret) — se `proxy_nokkel`.
    pub proxy_prefiks: Option<String>,
    /// Proxy-fila som ble PUT-et til tilbudets adresse.
    pub proxy: Option<Vec<u8>>,
    /// Fullfør godtok proxy-nøkkelen (den var lik den serveren selv ville laget).
    pub proxy_godtatt: Option<String>,
    /// Pause før lageret svarer på en del (så flere deler i lufta faktisk overlapper).
    pub put_pause_ms: u64,
    pub samtidige: usize,
    pub maks_samtidige: usize,
}
impl Lager {
    pub fn handlinger(&self, navn: &str) -> Vec<serde_json::Value> { self.handlinger.iter().filter(|h| h["action"] == navn).cloned().collect() }
    pub fn forsok(&self, nr: u32) -> u32 { self.forsok.get(&nr).copied().unwrap_or(0) }
    /// En multipart som er i gang fra før (en tilstandsfil testen skriver selv peker på den).
    pub fn paabegynt(&mut self, key: &str, upload_id: &str) {
        self.pending.insert(key.into(), Pending { upload_id: Some(upload_id.into()), versjon_av: String::new() });
    }
    /// Serverens proxy-nøkkel for en original (route.ts `proxyNokkel`, med testens prefiks).
    pub fn proxy_nokkel(&self, key: &str) -> String {
        let basis = key.trim_start_matches("originals/");
        let basis = basis.rsplit_once('.').map(|(a, _)| a).unwrap_or(basis);
        format!("{}{basis}.mp4", self.proxy_prefiks.as_deref().unwrap_or("proxy/"))
    }
}

pub struct Mock { pub adr: String, pub lager: Arc<Mutex<Lager>> }

pub async fn start_mock() -> Mock {
    let lytter = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let adr = format!("http://{}", lytter.local_addr().unwrap());
    let lager = Arc::new(Mutex::new(Lager::default()));
    let (l2, a2) = (lager.clone(), adr.clone());
    tokio::spawn(async move {
        while let Ok((s, _)) = lytter.accept().await {
            let (l3, a3) = (l2.clone(), a2.clone());
            tokio::spawn(async move { let _ = betjen(s, l3, a3).await; });
        }
    });
    Mock { adr, lager }
}

fn http_svar(status: u16, ekstra: &str, kropp: &[u8]) -> Vec<u8> {
    let mut v = format!("HTTP/1.1 {status} X\r\nContent-Length: {}\r\nConnection: close\r\n{ekstra}\r\n", kropp.len()).into_bytes();
    v.extend_from_slice(kropp);
    v
}

async fn betjen(mut s: tokio::net::TcpStream, lager: Arc<Mutex<Lager>>, adr: String) -> std::io::Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut hode = Vec::new();
    let mut b = [0u8; 1];
    while !hode.ends_with(b"\r\n\r\n") {
        if s.read(&mut b).await? == 0 { return Ok(()); }
        hode.push(b[0]);
    }
    let hode = String::from_utf8_lossy(&hode).to_string();
    let mut linjer = hode.split("\r\n");
    let mut forste = linjer.next().unwrap_or("").split(' ');
    let (metode, sti) = (forste.next().unwrap_or("").to_string(), forste.next().unwrap_or("").to_string());
    let (mut lengde, mut range) = (0usize, None::<u64>);
    for l in linjer {
        let l = l.to_ascii_lowercase();
        if let Some(v) = l.strip_prefix("content-length:") { lengde = v.trim().parse().unwrap_or(0); }
        if let Some(v) = l.strip_prefix("range:") { range = v.trim().trim_start_matches("bytes=").trim_end_matches('-').parse().ok(); }
    }
    // Avspillingskopien til tilbudets adresse.
    if metode == "PUT" && sti.starts_with("/r2/proxy") {
        let mut kropp = vec![0u8; lengde];
        s.read_exact(&mut kropp).await?;
        lager.lock().unwrap().proxy = Some(kropp);
        return s.write_all(&http_svar(200, "ETag: \"proxy\"\r\n", b"")).await;
    }
    // PUT mot lageret. Uhell kan inntreffe FØR hele kroppen er lest.
    if metode == "PUT" && sti.starts_with("/r2/") {
        let nr: u32 = sti.trim_start_matches("/r2/").split(['/', '?']).nth(1).and_then(|x| x.parse().ok()).unwrap_or(0);
        let (uhell, pause) = {
            let mut l = lager.lock().unwrap();
            let f = l.forsok.entry(nr).or_insert(0);
            *f += 1;
            let f = *f;
            l.samtidige += 1;
            l.maks_samtidige = l.maks_samtidige.max(l.samtidige);
            (l.uhell.get(&(nr, f)).copied(), l.put_pause_ms)
        };
        let svar = async {
            match uhell {
                Some(Uhell::Kutt(n)) => { let mut t = vec![0u8; n.min(lengde)]; let _ = s.read_exact(&mut t).await; return Ok(()); }
                Some(Uhell::Stopp(n)) => { let mut t = vec![0u8; n.min(lengde)]; let _ = s.read_exact(&mut t).await; tokio::time::sleep(std::time::Duration::from_secs(60)).await; return Ok(()); }
                _ => {}
            }
            let mut kropp = vec![0u8; lengde];
            s.read_exact(&mut kropp).await?;
            if pause > 0 && nr > 0 { tokio::time::sleep(std::time::Duration::from_millis(pause)).await; }
            if let Some(Uhell::Status(st)) = uhell { return s.write_all(&http_svar(st, "", b"")).await; }
            let etag = format!("\"e{nr}-{}\"", kropp.len());
            { let mut l = lager.lock().unwrap(); if nr == 0 { l.objekt = Some(kropp); } else { l.deler.insert(nr, kropp); } }
            s.write_all(&http_svar(200, &format!("ETag: {etag}\r\n"), b"")).await
        }.await;
        lager.lock().unwrap().samtidige -= 1;
        return svar;
    }
    if metode == "GET" && sti.starts_with("/api/transfer/versjon") {
        return s.write_all(&http_svar(200, "Content-Type: application/json\r\n", b"{}")).await;
    }
    if metode == "GET" && sti.starts_with("/api/rawskap/original/") {
        return s.write_all(&http_svar(302, &format!("Location: {adr}/r2/ned\r\n"), b"")).await;
    }
    if metode == "GET" && sti.starts_with("/r2/ned") {
        let (data, uhell) = {
            let mut l = lager.lock().unwrap();
            l.ned_range.push(range);
            let n = l.ned_range.len() as u32;
            (l.nedlasting.clone(), l.ned_uhell.get(&n).copied())
        };
        let fra = (range.unwrap_or(0) as usize).min(data.len());
        let rest = &data[fra..];
        let hode = if range.is_some() {
            format!("HTTP/1.1 206 X\r\nContent-Length: {}\r\nContent-Range: bytes {}-{}/{}\r\nConnection: close\r\n\r\n", rest.len(), fra, data.len().saturating_sub(1), data.len())
        } else {
            format!("HTTP/1.1 200 X\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", rest.len())
        };
        s.write_all(hode.as_bytes()).await?;
        if let Some(Uhell::Kutt(n)) = uhell { s.write_all(&rest[..n.min(rest.len())]).await?; return s.flush().await; }
        return s.write_all(rest).await;
    }
    if metode == "POST" && sti.starts_with("/api/rawskap/opplasting") {
        let mut kropp = vec![0u8; lengde];
        s.read_exact(&mut kropp).await?;
        let b: serde_json::Value = serde_json::from_slice(&kropp).unwrap_or_default();
        let (st, d) = handling(&lager, &adr, &b);
        return s.write_all(&http_svar(st, "Content-Type: application/json\r\n", d.to_string().as_bytes())).await;
    }
    s.write_all(&http_svar(404, "", b"")).await
}

/// Statusen og teksten for en versjonAv-kode (portalens lib/skap/ny-utgave.ts).
fn utgave_feil(kode: &str) -> (u16, &'static str) {
    match kode {
        "ugyldig" => (400, "Ugyldig versjonAv"),
        "avledning" => (400, "versjonAv og avledetFra kan ikke brukes sammen"),
        "ukjent" => (404, "Fant ikke fila du vil lage en ny versjon av"),
        "slettet" => (409, "Fila ligger i papirkurven — gjenopprett den før du laster opp en ny versjon"),
        "frosset" => (409, "Fila ligger i fryseren — tin den før du laster opp en ny versjon"),
        _ => (503, "Kunne ikke sjekke fila du vil lage en ny versjon av — prøv igjen"),
    }
}

/// Nøkkelen serveren utsteder: `originals/test.<endelse>` (én opplasting per test er nok).
fn ny_nokkel(filnavn: &str) -> String {
    let ext = filnavn.rsplit_once('.').map(|(_, e)| e.to_ascii_lowercase()).unwrap_or_else(|| "bin".into());
    format!("originals/test.{ext}")
}

/// route.ts `proxyTilbud`: video og MXF får en presignert adresse til avspillingskopien, og nøkkelen den skal ha.
fn proxy_tilbud(l: &Lager, adr: &str, key: &str, mime: &str, svar: &mut serde_json::Value) {
    if mime.starts_with("video/") || key.ends_with(".mxf") {
        svar["proxyKey"] = serde_json::json!(l.proxy_nokkel(key));
        svar["proxyPutUrl"] = serde_json::json!(format!("{adr}/r2/proxy?sig=1"));
    }
}

fn handling(lager: &Mutex<Lager>, adr: &str, b: &serde_json::Value) -> (u16, serde_json::Value) {
    use serde_json::json;
    let mut l = lager.lock().unwrap();
    l.handlinger.push(b.clone());
    let key = b["originalKey"].as_str().unwrap_or("").to_string();
    // Pending-raden med denne nøkkelen OG denne multiparten (ruta: OriginalKey + UploadId).
    let riktig = l.pending.get(&key).and_then(|p| p.upload_id.as_deref()) == b["uploadId"].as_str() && b["uploadId"].is_string();
    let versjon_av = b["versjonAv"].as_str().unwrap_or("").to_string();
    match b["action"].as_str().unwrap_or("") {
        "presign" | "multipart-start" => {
            // Forsjekken: nei FØR bytene går. `midlertidig` slipper gjennom (ruta lagrer målet og dømmer ved fullfør).
            if !versjon_av.is_empty() {
                if let Some(TestUtgave::Nei(kode)) = l.utgaver.get(&versjon_av).cloned() {
                    let (st, tekst) = utgave_feil(kode);
                    return (st, json!({ "error": tekst, "versjonAvFeil": kode }));
                }
            }
            let key = ny_nokkel(b["filnavn"].as_str().unwrap_or(""));
            let mime = b["mimeType"].as_str().unwrap_or("").to_string();
            if b["action"] == "presign" {
                l.pending.insert(key.clone(), Pending { upload_id: None, versjon_av });
                let mut svar = json!({ "uploadUrl": format!("{adr}/r2/enkel"), "originalKey": key });
                proxy_tilbud(&l, adr, &key, &mime, &mut svar);
                return (200, svar);
            }
            l.startet += 1;
            let uid = format!("u{}", l.startet);
            l.del_bytes = b["delBytes"].as_u64().unwrap_or(0);
            l.deler.clear();
            l.pending.insert(key.clone(), Pending { upload_id: Some(uid.clone()), versjon_av });
            let mut svar = json!({ "ok": true, "uploadId": uid, "originalKey": key });
            if l.del_bytes > 0 { svar["delBytes"] = json!(l.del_bytes); }
            proxy_tilbud(&l, adr, &key, &mime, &mut svar);
            (200, svar)
        }
        "multipart-deler" if riktig => {
            l.signeringer += 1;
            let s = l.signeringer;
            let deler: Vec<_> = b["deler"].as_array().unwrap().iter().map(|n| json!({ "nr": n, "url": format!("{adr}/r2/del/{n}?sig={s}") })).collect();
            (200, json!({ "ok": true, "deler": deler }))
        }
        "multipart-status" if riktig => {
            let deler: Vec<_> = l.deler.iter().map(|(nr, d)| json!({ "nr": nr, "bytes": d.len(), "etag": format!("e{nr}-{}", d.len()) })).collect();
            (200, json!({ "ok": true, "delBytes": l.del_bytes, "deler": deler }))
        }
        "multipart-fullfor" if riktig => {
            // Lageret krever stigende delnumre — akkurat som R2.
            let numre: Vec<u32> = b["deler"].as_array().unwrap().iter().map(|d| d["nr"].as_u64().unwrap() as u32).collect();
            if numre.windows(2).any(|w| w[0] >= w[1]) { return (400, json!({ "error": "InvalidPartOrder" })); }
            let mut hel = Vec::new();
            for nr in &numre {
                match l.deler.get(nr) { Some(d) => hel.extend_from_slice(d), None => return (400, json!({ "error": format!("del {nr} mangler") })) }
            }
            l.objekt = Some(hel);
            // Ruta: UploadId = null — multiparten er ferdig, og status/deler kjenner den ikke lenger.
            if let Some(p) = l.pending.get_mut(&key) { p.upload_id = None; }
            (200, json!({ "ok": true }))
        }
        "multipart-deler" | "multipart-status" | "multipart-fullfor" => (404, json!({ "error": "ukjent opplasting" })),
        "fullfor" => fullfor(&mut l, &key, &versjon_av, b),
        "avbryt" => {
            for k in b["originalKeys"].as_array().cloned().unwrap_or_default() {
                let k = k.as_str().unwrap_or("").to_string();
                if l.pending.remove(&k).is_some() { l.ryddet.push(k); }
            }
            (200, json!({ "ok": true }))
        }
        _ => (400, json!({ "error": "ukjent handling" })),
    }
}

fn fullfor(l: &mut Lager, key: &str, versjon_av: &str, b: &serde_json::Value) -> (u16, serde_json::Value) {
    use serde_json::json;
    // BARE kallets versjonAv (ea77e32a) — målet på pending-raden er feiekostens, ikke appens.
    let mut rot: Option<String> = None;
    if !versjon_av.is_empty() {
        // Eierskapet på nøkkelen FØRST: pending-raden, eller en rad kunden alt har med nøkkelen. Ellers røres ingenting.
        if !l.pending.contains_key(key) && !l.rader.contains_key(key) { return (404, json!({ "error": "ukjent opplasting", "borte": true })); }
        let dom = l.utgaver_fullfor.get(versjon_av).or(l.utgaver.get(versjon_av)).cloned().unwrap_or(TestUtgave::Ok { rot: versjon_av.to_string() });
        match dom {
            TestUtgave::Midlertidig => return (503, json!({ "error": utgave_feil("midlertidig").1, "versjonAvFeil": "midlertidig" })),
            TestUtgave::Nei(kode) => {
                let (st, tekst) = utgave_feil(kode);
                // Feiekosten rakk å lage raden: i en stakk = jobben er gjort; alene = nei med id, og ingenting røres.
                if let Some(rad) = l.rader.get(key) {
                    if !rad.versjon_av.is_empty() { return (200, json!({ "ok": true, "id": rad.id, "allerede": true, "versjonAv": rad.versjon_av })); }
                    return (st, json!({ "error": tekst, "versjonAvFeil": kode, "id": rad.id, "ryddet": false }));
                }
                if l.eldre_server { return (st, json!({ "error": tekst, "versjonAvFeil": kode })); }
                // Endelig nei: rydd som `avbryt` (pending + objektet). `ryddet` = noe ble faktisk fjernet.
                let ryddet = !l.rydding_feiler && l.pending.remove(key).is_some();
                if ryddet { l.ryddet.push(key.to_string()); l.objekt = None; }
                let mut svar = json!({ "error": tekst, "versjonAvFeil": kode, "ryddet": ryddet });
                if ryddet { svar["borte"] = json!(true); }
                return (st, svar);
            }
            TestUtgave::Ok { rot: r } => rot = Some(r),
        }
    }
    // Idempotens: raden finnes alt (et tidligere kall, eller feiekosten).
    if let Some(rad) = l.rader.get_mut(key) {
        if let (Some(r), true) = (&rot, rad.versjon_av.is_empty()) { rad.versjon_av = r.clone(); }
        let (id, av) = (rad.id.clone(), rad.versjon_av.clone());
        l.pending.remove(key);
        let mut svar = json!({ "ok": true, "id": id, "allerede": true });
        if rot.is_some() && !av.is_empty() { svar["versjonAv"] = json!(av); }
        return (200, svar);
    }
    // Eierskapet på nøkkelen: bare en nøkkel serveren selv utstedte (pending-raden er kvitteringen).
    if !l.pending.contains_key(key) { return (404, json!({ "error": "ukjent opplasting", "borte": true })); }
    if b["proxyKey"].as_str().is_some_and(|p| p == l.proxy_nokkel(key)) { l.proxy_godtatt = b["proxyKey"].as_str().map(String::from); }
    let id = format!("recTest{:010}", l.rader.len() + 1);
    l.rader.insert(key.to_string(), Rad { id: id.clone(), versjon_av: rot.clone().unwrap_or_default() });
    l.pending.remove(key);
    let mut svar = json!({ "ok": true, "id": id });
    if let Some(r) = rot { svar["versjonAv"] = json!(r); }
    (200, svar)
}
