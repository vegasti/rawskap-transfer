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

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

#[derive(Clone, Copy, Debug)]
pub enum Uhell { Status(u16), Kutt(usize), Stopp(usize) }

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
    pub upload_id: String,
    pub nedlasting: Vec<u8>,
    /// GET-forsøk nr. → uhell.
    pub ned_uhell: HashMap<u32, Uhell>,
    pub ned_range: Vec<Option<u64>>,
}
impl Lager {
    pub fn handlinger(&self, navn: &str) -> Vec<serde_json::Value> { self.handlinger.iter().filter(|h| h["action"] == navn).cloned().collect() }
    pub fn forsok(&self, nr: u32) -> u32 { self.forsok.get(&nr).copied().unwrap_or(0) }
}

pub struct Mock { pub adr: String, pub lager: Arc<Mutex<Lager>> }

pub async fn start_mock() -> Mock {
    let lytter = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let adr = format!("http://{}", lytter.local_addr().unwrap());
    let lager = Arc::new(Mutex::new(Lager { upload_id: "u1".into(), ..Default::default() }));
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
    // PUT mot lageret. Uhell kan inntreffe FØR hele kroppen er lest.
    if metode == "PUT" && sti.starts_with("/r2/") {
        let nr: u32 = sti.trim_start_matches("/r2/").split(['/', '?']).nth(1).and_then(|x| x.parse().ok()).unwrap_or(0);
        let uhell = {
            let mut l = lager.lock().unwrap();
            let f = l.forsok.entry(nr).or_insert(0);
            *f += 1;
            let f = *f;
            l.uhell.get(&(nr, f)).copied()
        };
        match uhell {
            Some(Uhell::Kutt(n)) => { let mut t = vec![0u8; n.min(lengde)]; let _ = s.read_exact(&mut t).await; return Ok(()); }
            Some(Uhell::Stopp(n)) => { let mut t = vec![0u8; n.min(lengde)]; let _ = s.read_exact(&mut t).await; tokio::time::sleep(std::time::Duration::from_secs(60)).await; return Ok(()); }
            _ => {}
        }
        let mut kropp = vec![0u8; lengde];
        s.read_exact(&mut kropp).await?;
        if let Some(Uhell::Status(st)) = uhell { return s.write_all(&http_svar(st, "", b"")).await; }
        let etag = format!("\"e{nr}-{}\"", kropp.len());
        { let mut l = lager.lock().unwrap(); if nr == 0 { l.objekt = Some(kropp); } else { l.deler.insert(nr, kropp); } }
        return s.write_all(&http_svar(200, &format!("ETag: {etag}\r\n"), b"")).await;
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

fn handling(lager: &Mutex<Lager>, adr: &str, b: &serde_json::Value) -> (u16, serde_json::Value) {
    use serde_json::json;
    let mut l = lager.lock().unwrap();
    l.handlinger.push(b.clone());
    let riktig = b["uploadId"].as_str() == Some(l.upload_id.as_str());
    match b["action"].as_str().unwrap_or("") {
        "presign" => (200, json!({ "uploadUrl": format!("{adr}/r2/enkel"), "originalKey": "originals/test.bin" })),
        "multipart-start" => {
            l.del_bytes = b["delBytes"].as_u64().unwrap_or(0);
            l.deler.clear();
            (200, json!({ "ok": true, "uploadId": l.upload_id, "originalKey": "originals/test.bin" }))
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
            (200, json!({ "ok": true }))
        }
        "multipart-deler" | "multipart-status" | "multipart-fullfor" => (404, json!({ "error": "ukjent opplasting" })),
        "fullfor" | "avbryt" => (200, json!({ "ok": true })),
        _ => (400, json!({ "error": "ukjent handling" })),
    }
}
