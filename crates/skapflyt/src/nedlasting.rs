// Nedlasting fra skapet: én fil med Range-resume, omforsøk, og «hva mangler lokalt?».
use crate::{bare_uten_redirect, fil_xxh64, meld, nettfeil, pause_foer_forsok, sov, trygt_navn, vent_paa_nett, Fil, Framdrift, Melder, SjekkFil, Stopp, Struper, NED_FORSOK};
use futures_util::StreamExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;

/// Last ned én fil med Range-resume. Skriver til «<navn>.part», flytter til
/// endelig navn når alt er nede. Finnes endelig fil med riktig størrelse → hopp.
pub async fn last_ned_en<K: Melder>(
    app: &K,
    k: &reqwest::Client,
    bare: &reqwest::Client,
    portal: &str,
    fil: &Fil,
    rot: &Path,
    avbryt: &Stopp,
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
    // Når fila finnes: 'hopp' (LA DEN VÆRE — standard), 'begge' (nytt navn «navn (2).ext»),
    // 'overskriv' (alltid på nytt).
    if tokio::fs::metadata(&maal).await.is_ok() {
        match konflikt {
            "begge" => { let (stamme, ext) = match trygt_navn(&fil.filnavn).rsplit_once('.') { Some((a, b)) => (a.to_string(), format!(".{b}")), None => (trygt_navn(&fil.filnavn), String::new()) }; let mut n = 2; loop { let k = mappe.join(format!("{stamme} ({n}){ext}")); if tokio::fs::metadata(&k).await.is_err() { maal = k; break; } n += 1; } }
            "overskriv" => { let _ = tokio::fs::remove_file(&maal).await; }
            _ => {}
        }
    }
    let part = mappe.join(format!("{}.part", trygt_navn(&fil.filnavn)));

    // ⚠ FINNES FILA MED SITT ENDELIGE NAVN, RØRER VI DEN IKKE (17/9).
    //
    // Før dette hoppet vi bare over når størrelsen var LIK, og lastet ned oppå når den avvek. Det
    // ødela arbeid: en redigerer som jobber i nedlastingsmappa endrer størrelsen på fila. Lightroom
    // skriver metadata rett inn i DNG-filer, og .xmp-sidecars til ARW endrer størrelse ved hver
    // justering — så neste «last ned mappa» la originalen tilbake oppå hennes versjon, uten å spørre.
    //
    // At vi trygt kan la den være, følger av hvor gjenopptaks-tilstanden bor: en avbrutt nedlasting
    // ligger som «navn.ext.part», ALDRI under sitt endelige navn. En fil med endelig navn og avvikende
    // størrelse er derfor aldri en halvferdig nedlasting — den er noens endrede fil.
    //
    // Størrelsen brukes fortsatt, men bare til å SI hva som skjedde: lik = allerede overført,
    // ulik = endret lokalt. «Overskriv» og «Behold begge» er uendret for den som vil ha dem.
    // Unntak: en TOM fil er ingens arbeid. Uten dette ville en 0-byte rest (fra et annet program,
    // eller en disk som gikk full) blitt stående som «endret lokalt» for alltid.
    if let Ok(m) = tokio::fs::metadata(&maal).await {
        if m.len() == 0 { let _ = tokio::fs::remove_file(&maal).await; }
    }
    if let Ok(m) = tokio::fs::metadata(&maal).await {
        // Død .part ved siden av komplett fil (kø 25/8): en avbrutt runde etterlot resten — fila ER
        // her, så resume-dataene er verdiløse.
        let _ = tokio::fs::remove_file(&part).await;
        let lik = fil.bytes > 0 && m.len() == fil.bytes;
        let _ = app.framdrift(Framdrift {
            id: fil.id.clone(), hentet: fil.bytes, total: fil.bytes,
            status: if lik { "hoppet".into() } else { "endretLokalt".to_string() }, feil: None,
        });
        return Ok(());
    }
    let allerede = tokio::fs::metadata(&part).await.map(|m| m.len()).unwrap_or(0);
    let hentet = Arc::new(AtomicU64::new(allerede));

    // 1) portalen → 302 (signert R2-URL). Cookien følger KUN hit.
    let url = if fil.url.is_empty() { format!("{}/api/rawskap/original/{}?last=1", portal.trim_end_matches('/'), fil.id) } else { fil.url.clone() };
    let r = if fil.url.is_empty() { k.get(&url) } else { bare_uten_redirect().get(&url) }.send().await.map_err(nettfeil)?;
    let r2 = match r.status().as_u16() {
        301 | 302 | 303 | 307 | 308 => r
            .headers()
            .get(reqwest::header::LOCATION)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
            .ok_or("302 uten Location")?,
        403 => return Err("Blokkert (samtykke trukket/utløpt)".into()),
        // ⚠ EN DELINGSFIL SOM FÅR 401 ER IKKE EN UGYLDIG APP-NØKKEL (19/9). Før fikk
        // alle 401 samme melding, «Nøkkelen er ugyldig — lag en ny», også når fila
        // tilhørte en passordbeskyttet deling der adgangsnøkkelen hadde gått ut —
        // og der finnes det ingen nøkkel å «lage». DELING_LAAST er et fast signal
        // frontenden kjenner igjen: den pauser jobben og ber om passordet i stedet
        // for å la hver eneste fil feile. (Delingsfiler har alltid `url`; filer fra
        // eget skap har den tom og går via app-nøkkelen.)
        401 if !fil.url.is_empty() => return Err("DELING_LAAST".into()),
        401 => return Err("Nøkkelen er ugyldig — lag en ny".into()),
        s if (200..300).contains(&s) => url.clone(), // ikke R2 (Drive) → strøm fra portalen
        s => return Err(format!("Portalen svarte {s}")),
    };
    // 2) R2 med Range fra der vi slapp.
    let mut req = if r2 == url { k.get(&r2) } else { bare.get(&r2) };
    if allerede > 0 {
        req = req.header(reqwest::header::RANGE, format!("bytes={}-", allerede));
    }
    let resp = req.send().await.map_err(nettfeil)?;
    let status = resp.status().as_u16();
    let (append, start) = match status {
        206 => (true, allerede),
        200 => (false, 0u64), // serveren ignorerte Range → start på nytt
        416 => { // alt er alt nede
            tokio::fs::rename(&part, &maal).await.map_err(|e| format!("{e}"))?;
            let _ = app.framdrift(Framdrift { id: fil.id.clone(), hentet: fil.bytes, total: fil.bytes, status: "ferdig".into(), feil: None });
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
        // ⚠ Flush før vi gir opp (0.3.0): tokio skriver i bakgrunnen, og et nytt forsøk som leser
        // .part-størrelsen før siste bit har landet, ville fortsatt fra feil sted.
        if avbryt.av() { let _ = f.flush().await; return Err("Avbrutt".into()); }
        let bit = match bit { Ok(b) => b, Err(e) => { let _ = f.flush().await; return Err(nettfeil(e)); } };
        f.write_all(&bit).await.map_err(|e| format!("{e}"))?;
        struper.tell(bit.len() as u64).await;
        let n = hentet.fetch_add(bit.len() as u64, Ordering::Relaxed) + bit.len() as u64;
        if sist_meldt.elapsed().as_millis() > 150 {
            sist_meldt = std::time::Instant::now();
            let _ = app.framdrift(Framdrift { id: fil.id.clone(), hentet: n, total, status: "laster".into(), feil: None });
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
        let _ = app.framdrift(Framdrift { id: fil.id.clone(), hentet: n, total, status: "hash".into(), feil: None });
        let h = fil_xxh64(&part).await.unwrap_or_default();
        if h != fil.xxh64.to_lowercase() { let _ = tokio::fs::remove_file(&part).await; return Err("Verifisering feilet (xxHash) — lastet ned på nytt".into()); }
    }
    tokio::fs::rename(&part, &maal).await.map_err(|e| format!("{e}"))?;
    let _ = app.framdrift(Framdrift { id: fil.id.clone(), hentet: n, total, status: if fil.xxh64.is_empty() { "ferdig".into() } else { "verifisert".into() }, feil: None });
    Ok(())
}

/// Én fil, med omforsøk (0.3.0). Gjenopptak via .part gjør hvert forsøk billig, så tålmodigheten kan
/// være stor: økende pauser, og er nettet borte, venter vi på det. Før var det tre forsøk med 0,8 s
/// mellom — et wifi-hopp på et halvt minutt felte fila.
///
/// ⚠ Varige feil (låst deling, ugyldig nøkkel, blokkert fil) prøves IKKE igjen: svaret blir det samme.
pub async fn last_ned_med_forsok<K: Melder>(app: &K, k: &reqwest::Client, bare: &reqwest::Client, portal: &str, fil: &Fil, rot: &Path, avbryt: &Stopp, struper: &Struper, konflikt: &str) -> Result<(), String> {
    let mut n = 0u32;
    loop {
        let e = match last_ned_en(app, k, bare, portal, fil, rot, avbryt, struper, konflikt).await { Ok(()) => return Ok(()), Err(e) => e };
        if avbryt.av() || varig_nedlastingsfeil(&e) || n + 1 >= NED_FORSOK {
            if e != "Avbrutt" { log::warn!("ned feilet: {}: {e}", fil.filnavn); }
            return Err(e);
        }
        log::warn!("ned {} forsøk {} feilet: {e}", fil.filnavn, n + 1);
        let (a, id, total) = (app.clone(), fil.id.clone(), fil.bytes);
        vent_paa_nett(portal, Some(avbryt), &move || meld(&a, &id, 0, total, "venterNett")).await;
        meld(app, &fil.id, 0, fil.bytes, "nyttForsok");
        sov(pause_foer_forsok(n), Some(avbryt)).await;
        if avbryt.av() { return Err("Avbrutt".into()); }
        n += 1;
    }
}
pub fn varig_nedlastingsfeil(e: &str) -> bool {
    e == "DELING_LAAST" || e.starts_with("Nøkkelen er ugyldig") || e.starts_with("Blokkert") || e.starts_with("Portalen svarte 4")
}

/// Hvilke av disse filene finnes IKKE i nedlastingsmappa?
///
/// Svaret er hele grunnlaget for «N nye» og «Last ned nye» (17/9). Vegard: en redigerer som skal ha
/// siste bunke maatte laste ned HELE mappa paa nytt. Nedlastingen hoppet riktignok over det som alt
/// laa der, men ingenting FORTALTE henne at det fantes noe nytt, og jobben viste 500 filer for aa
/// hente 12.
///
/// ⚠ «Finnes» betyr her nøyaktig det samme som i last_ned_en: ligger fila der med sitt endelige
/// navn, er den ikke ny — uansett stoerrelse. En fil hun har redigert (DNG som Lightroom har skrevet
/// i) skal ikke dukke opp som «ny» og friste til aa hente originalen tilbake. Tomme filer teller som
/// fravaerende, samme unntak som der.
pub async fn mangler_lokalt(rot: String, filer: Vec<SjekkFil>) -> Result<Vec<String>, String> {
    let r = PathBuf::from(&rot);
    // ⚠ FINNES IKKE MAPPA, ER ALT NYTT. Det vanligste tilfellet: mappa er aldri lastet ned. Uten
    // denne linja spurte vi disken én gang PER FIL om noe vi allerede visste svaret på.
    if tokio::fs::metadata(&r).await.is_err() {
        return Ok(filer.into_iter().map(|f| f.id).collect());
    }
    // ⚠ ÉN KATALOGLESING PER MAPPE, ikke én stat per fil (18/9). Denne kjører hver gang en mappe
    // åpnes, og den første varianten gjorde ett systemkall per fil: med et par tusen filer på en
    // nettverksdisk ble det sekunder med venting før lista kom opp. Katalogen leses nå én gang og
    // navnene slås opp i minnet. På Windows bærer read_dir-oppføringene metadataene sine fra
    // katalogskanningen, så `metadata()` her koster ikke et nytt kall.
    let mut kataloger: std::collections::HashMap<String, std::collections::HashSet<String>> = std::collections::HashMap::new();
    let mut ut = Vec::new();
    for f in filer {
        if !kataloger.contains_key(&f.sti) {
            let mut p = r.clone();
            for d in f.sti.split('/').filter(|s| !s.is_empty()) { p.push(trygt_navn(d)); }
            let mut sett = std::collections::HashSet::new();
            if let Ok(mut kat) = tokio::fs::read_dir(&p).await {
                while let Ok(Some(e)) = kat.next_entry().await {
                    // Tomme filer teller som fraværende — samme unntak som i last_ned_en.
                    let stor_nok = e.metadata().await.map(|m| m.len() > 0).unwrap_or(false);
                    if stor_nok { sett.insert(e.file_name().to_string_lossy().to_string()); }
                }
            }
            kataloger.insert(f.sti.clone(), sett);
        }
        let finnes = kataloger.get(&f.sti).map(|s| s.contains(&trygt_navn(&f.filnavn))).unwrap_or(false);
        if !finnes { ut.push(f.id); }
    }
    Ok(ut)
}
