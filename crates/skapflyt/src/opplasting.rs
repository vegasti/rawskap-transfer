// Opplasting til skapet: enkel PUT, multipart med gjenopptak, fullfør, forkast og økt.
use crate::{forbigaaende, forkast_nokler, klient, mappe_json, med_stoppvakt, meld, mime_fra, nettfeil, pause_foer_forsok, portal_post, sov, vent_paa_nett, Etterarbeid, Framdrift, Melder, OppFil, Stopp, Struper, DEL_FORSOK, ENKEL_FORSOK};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

// ── MULTIPART + RESUME (22/8): filer over DEL_GRENSE går i deler (64 MB → 16 MB i 0.2.8,
// og fra 0.3.0 vokser delen med fila — se delstorrelse).
// Tilstand per fil ligger i en liten JSON i appens datamappe (nøkkel =
// xxh64 av sti|størrelse|mtime) — uploadId, originalKey, ferdige deler m/
// ETag. Starter man på nytt (nettbrudd, lukket app) fortsettes fra siste
// ferdige del. Rådes til å ha lifecycle-regel i R2 for forlatte multiparts.
/// ⚠ GAMMEL delstørrelse. Resume-filer uten `del_bytes` ble skrevet med denne, og MÅ leses med den.
pub const DEL_BYTES_GAMMEL: u64 = 64 * 1024 * 1024;
/// Delstørrelse for NYE opplastinger. Senket fra 64 MB (17/9): et tapt delforsøk kostet opptil
/// 64 MB å gjenta — omtrent hele en råfil. 16 MB koster sekunder. (R2/S3 krever minst 5 MB.)
pub const DEL_BYTES: u64 = 16 * 1024 * 1024;
/// Over denne går opplastingen i deler med gjenopptak. Senket fra 96 MB (17/9): en fotograf som
/// skyter 60-70 MB råfiler lå UNDER hele grensa, så nettopp filene som tar lang nok tid til å ryke
/// var de eneste uten gjenopptak. 24 MB gir deling på alt som varer mer enn et øyeblikk.
pub const DEL_GRENSE: u64 = 24 * 1024 * 1024;
/// Aldri flere deler enn dette per fil (R2/S3 tar 10 000; samme margin som nettleseren).
pub const MAKS_DELER: u64 = 9_000;

/// Delstørrelse for en NY opplasting (0.3.0).
///
/// ⚠ R2/S3 tar maks 10 000 deler per fil. Med fast 16 MB-del (0.2.8) var taket derfor ~156 GiB — og
/// et langt 8K ProRes RAW-klipp (~24 GB i minuttet) passerer det på under sju minutter. Fila ville
/// feilet på del 10 001. Delen vokser nå med fila, så det aldri blir flere enn MAKS_DELER, rundet opp
/// til hel MiB. Under ~140 GiB er den uendret 16 MiB.
pub fn delstorrelse(bytes: u64) -> u64 {
    const MIB: u64 = 1024 * 1024;
    DEL_BYTES.max(bytes.div_ceil(MAKS_DELER).div_ceil(MIB) * MIB)
}

#[derive(Clone, Serialize, Deserialize, Default)]
pub struct Resume { pub upload_id: String, pub original_key: String, pub mappe_id: String, pub bytes: u64, pub deler: Vec<(u32, String)>, #[serde(default)] pub proxy_put: String,
    /// ⚠ Delstørrelsen opplastingen FAKTISK ble startet med. Må følge tilstanden, ikke leses fra
    /// konstanten: endrer vi DEL_BYTES, ville en gjenopptatt opplasting fra forrige versjon fått
    /// delene skrevet på feil forskyvning — korrupt fil, uten feilmelding. 0 = skrevet før dette
    /// feltet fantes, og da var den alltid 64 MB.
    #[serde(default)] pub del_bytes: u64 }

/// Mappa med tilstandsfilene for gjenopptak (og Transfers synk-minne).
///
/// ⚠ Stien er den samme som før uttrekket (S1): en opplasting som er i gang hos en kunde når appen
/// oppdaterer seg, skal finne tilstanden sin igjen. Flytt den aldri uten å lese den gamle også.
pub fn resume_dir() -> PathBuf {
    // Testene får sin egen mappe, så de aldri rører en ekte opplasting på maskina de kjøres på.
    #[cfg(test)]
    let d = std::env::temp_dir().join("rawskap-transfer-test").join("opplasting");
    #[cfg(not(test))]
    let d = dirs::data_local_dir().unwrap_or(std::env::temp_dir()).join("RawskapTransfer").join("opplasting");
    let _ = std::fs::create_dir_all(&d); d
}
pub fn resume_sti(sti: &str, bytes: u64, mtime: u64) -> PathBuf {
    let h = xxhash_rust::xxh64::xxh64(format!("{sti}|{bytes}|{mtime}").as_bytes(), 0);
    resume_dir().join(format!("{h:016x}.json"))
}
pub fn resume_les(p: &Path) -> Option<Resume> { std::fs::read(p).ok().and_then(|b| serde_json::from_slice(&b).ok()) }
pub fn resume_skriv(p: &Path, r: &Resume) { if let Ok(b) = serde_json::to_vec(r) { let tmp = p.with_extension("tmp"); if std::fs::write(&tmp, b).is_ok() { let _ = std::fs::rename(&tmp, p); } } }

/// Hvor i fila del `nr` starter, og hvor lang den er.
///
/// ⚠ Dette er aritmetikken som ødelegger en fil hvis den er feil, og den er lett å ta feil av: en
/// del skrevet på gal forskyvning gir en korrupt fil UTEN at noe feiler underveis. Den fikk egen
/// funksjon 17/9 nettopp for å kunne testes — delstørrelsen ble endret samme dag, og da må man
/// kunne bevise at delene fortsatt dekker fila nøyaktig én gang.
pub fn del_omrade(nr: u32, del: u64, bytes: u64) -> (u64, u64) {
    let start = (nr as u64 - 1) * del;
    (start, bytes.saturating_sub(start).min(del))
}

/// Starter en fersk multipart og gir resume-tilstanden tilbake.
/// Egen funksjon fordi den kalles to steder: ved ny opplasting, og når en
/// gjenopptatt opplasting viser seg å være død på serveren (30/8).
pub async fn multipart_start(k: &reqwest::Client, portal: &str, navn: &str, mime: &str, bytes: u64, sist: u64, mappe_id: &str, avbryt: &Stopp) -> Result<Resume, String> {
    // Delstørrelsen settes én gang, her, og følger opplastingen til den er ferdig — både i vår egen
    // resume-fil OG hos serveren (`delBytes` → opplastinger_pending.DelBytes). Uten den kan ikke
    // nettleseren gjenoppta en opplasting Transfer har begynt på, fordi den ikke vet hvor delegrensene går.
    let del = delstorrelse(bytes);
    let (st, d) = portal_post(k, portal, &serde_json::json!({ "action": "multipart-start", "filnavn": navn, "mimeType": mime, "filstorrelse": bytes, "sistEndret": sist, "mappeId": mappe_json(mappe_id), "delBytes": del }), Some(avbryt)).await?;
    if st == 401 || st == 403 { return Err("Ikke tilgang — logg inn på nytt".into()); }
    let (uid, key) = match (d["uploadId"].as_str(), d["originalKey"].as_str()) { (Some(u), Some(kk)) => (u.to_string(), kk.to_string()), _ => return Err(d["error"].as_str().unwrap_or("multipart-start feilet").to_string()) };
    // Serveren tilbyr en presignert URL for avspillingsproxyen (kun video).
    // Den lagres i resume-fila så en gjenopptatt opplasting ikke mister den.
    Ok(Resume { upload_id: uid, original_key: key, mappe_id: mappe_id.to_string(), bytes, deler: vec![], proxy_put: d["proxyPutUrl"].as_str().unwrap_or("").to_string(), del_bytes: del })
}

/// Hva lageret sier om en påbegynt opplasting.
pub enum MpStatus { Deler { deler: Vec<(u32, u64, String)>, del_bytes: u64 }, Ferdig, Borte, Ukjent(String) }

pub async fn multipart_status(k: &reqwest::Client, portal: &str, r: &Resume, avbryt: &Stopp) -> MpStatus {
    match portal_post(k, portal, &serde_json::json!({ "action": "multipart-status", "originalKey": r.original_key, "uploadId": r.upload_id }), Some(avbryt)).await {
        // 410 = multiparten er borte hos lageret; 404 = portalen kjenner den ikke lenger (feiekosten
        // ryddet raden etter 72 stille timer). Begge betyr: ingenting å fortsette på.
        Ok((410, _)) | Ok((404, _)) => MpStatus::Borte,
        Ok((s, d)) if (200..300).contains(&s) => {
            if d["ferdig"].as_bool().unwrap_or(false) { return MpStatus::Ferdig; }
            let deler = d["deler"].as_array().map(|a| a.iter().filter_map(|x| Some((x["nr"].as_u64()? as u32, x["bytes"].as_u64().unwrap_or(0), x["etag"].as_str()?.trim_matches('"').to_string()))).collect()).unwrap_or_default();
            MpStatus::Deler { deler, del_bytes: d["delBytes"].as_u64().unwrap_or(0) }
        }
        Ok((s, d)) => MpStatus::Ukjent(format!("{s} {}", d["error"].as_str().unwrap_or(""))),
        Err(e) => MpStatus::Ukjent(e),
    }
}

/// Delene lageret har, filtrert til dem vi kan stole på: gyldig nummer og NØYAKTIG den lengden delen
/// skal ha. En del med feil lengde lastes opp på nytt — samme nummer overskriver den.
pub fn gyldige_deler(fra_lager: &[(u32, u64, String)], del: u64, bytes: u64) -> Vec<(u32, String)> {
    let antall = bytes.div_ceil(del) as u32;
    let mut ut: Vec<(u32, String)> = fra_lager.iter()
        .filter(|(nr, b, e)| *nr >= 1 && *nr <= antall && !e.is_empty() && *b == del_omrade(*nr, del, bytes).1)
        .map(|(nr, _, e)| (*nr, e.clone()))
        .collect();
    ut.sort_by_key(|(nr, _)| *nr);
    ut.dedup_by_key(|(nr, _)| *nr);
    ut
}

pub async fn signer_deler(k: &reqwest::Client, portal: &str, r: &Resume, numre: &[u32], avbryt: &Stopp) -> Result<std::collections::HashMap<u32, String>, String> {
    let (_, d) = portal_post(k, portal, &serde_json::json!({ "action": "multipart-deler", "originalKey": r.original_key, "uploadId": r.upload_id, "deler": numre }), Some(avbryt)).await?;
    Ok(d["deler"].as_array().map(|a| a.iter().filter_map(|x| Some((x["nr"].as_u64()? as u32, x["url"].as_str()?.to_string()))).collect()).unwrap_or_default())
}

pub async fn last_opp_multipart<K: Melder>(app: &K, k: &reqwest::Client, bare: &reqwest::Client, portal: &str, fil: &OppFil, mappe_id: &str, avbryt: &Stopp, struper: Arc<Struper>, navn: &str, mime: &str, bytes: u64, sist: u64) -> Result<(String, u64, String, Option<String>), String> {
    // xxh64 regnes mens delene likevel leses (17/9) — se fil_strom for hvorfor.
    let hasher = std::sync::Mutex::new(xxhash_rust::xxh64::Xxh64::new(0));
    let mut hel = true;
    use tokio::io::{AsyncReadExt, AsyncSeekExt};
    let rsti = resume_sti(&fil.sti, bytes, sist);
    let mut r = resume_les(&rsti).filter(|r| r.bytes == bytes && r.mappe_id == mappe_id).unwrap_or_default();
    // Rundturen til portalen (multipart-start, og siden signering av deler) tar
    // et lite oyeblikk der ingen bytes gaar. Si det, i stedet for a staa pa 0.
    meld(app, &fil.sti, 0, bytes, "kobler");
    // ⚠ LAGERET ER FASIT VED GJENOPPTAK (0.3.0). Før stolte vi blindt på vår egen bokføring: ble en del
    // sendt uten at vi rakk å skrive den ned (appen døde i sekundet mellom), visste vi ikke om den, og
    // en ødelagt resume-fil betød hele fila på nytt. Nå spør vi lageret hvilke deler som faktisk ligger
    // der — samme spørsmål nettleseren stiller — og tar bare med deler med nøyaktig riktig lengde.
    if !r.upload_id.is_empty() {
        match multipart_status(k, portal, &r, avbryt).await {
            MpStatus::Deler { deler, del_bytes } => {
                if r.del_bytes == 0 && del_bytes > 0 { r.del_bytes = del_bytes; }
                let del = if r.del_bytes > 0 { r.del_bytes } else { DEL_BYTES_GAMMEL };
                let lokalt = r.deler.len();
                r.deler = gyldige_deler(&deler, del, bytes);
                log::info!("gjenopptar {navn}: lageret har {} ferdige deler (lokal bokføring: {lokalt})", r.deler.len());
                resume_skriv(&rsti, &r);
            }
            MpStatus::Ferdig => {
                // Satt sammen alt — appen døde mellom sammensetting og fullfør. Bare fullfør gjenstår.
                log::info!("gjenopptar {navn}: fila er allerede satt sammen hos lageret");
                let _ = std::fs::remove_file(&rsti);
                return Ok((r.original_key.clone(), bytes, r.proxy_put.clone(), None));
            }
            MpStatus::Borte => {
                log::warn!("{navn}: opplastingen finnes ikke lenger hos lageret — begynner på nytt");
                r = Resume::default();
            }
            MpStatus::Ukjent(e) => log::warn!("{navn}: fikk ikke lest status hos lageret ({e}) — bruker lokal bokføring"),
        }
    }
    if r.upload_id.is_empty() {
        r = multipart_start(k, portal, navn, mime, bytes, sist, mappe_id, avbryt).await?;
        resume_skriv(&rsti, &r);
    }
    let mut f = tokio::fs::File::open(&fil.sti).await.map_err(|e| format!("{e}"))?;
    let mut hentet;
    // ⚠ DØD OPPLASTING (30/8): kjenner ikke serveren opplastingen igjen, er
    // resume-tilstanden verdiløs — og før dette var fila da PERMANENT brekt:
    // hvert nytt forsøk leste den samme døde nøkkelen fra resume-fila og ga
    // opp med en gang. Nå starter vi friskt i stedet. Én gang, så en server
    // som avviser alt ikke blir en evig løkke.
    let mut omstart_brukt = false;
    'omstart: loop {
    // ⚠ REGN MED OPPLASTINGENS EGEN DELSTØRRELSE, ikke konstanten. En resume-fil fra en tidligere
    // versjon ble skrevet med 64 MB; leser vi den med dagens tall, havner delene på feil
    // forskyvning og fila blir korrupt uten at noe sier fra. 0 = fil fra før feltet fantes.
    // (Regnes INNE i løkka: en omstart kan gi en ny opplasting med en annen delstørrelse.)
    let del = if r.del_bytes > 0 { r.del_bytes } else { DEL_BYTES_GAMMEL };
    let antall = bytes.div_ceil(del) as u32;
    let ferdige: std::collections::HashSet<u32> = r.deler.iter().map(|(n, _)| *n).collect();
    // Finnes det alt ferdige deler, har vi IKKE lest hele fila i denne kjøringen — da er en
    // helfils-hash umulig, og vi lar være å påstå en.
    if !ferdige.is_empty() { hel = false; }
    hentet = ferdige.iter().map(|n| del_omrade(*n, del, bytes).1).sum::<u64>().min(bytes);
    meld(app, &fil.sti, hentet, bytes, "laster");
    // Presigner deler i bolker på 20 (URL-ene lever 1 t).
    let mangler: Vec<u32> = (1..=antall).filter(|n| !ferdige.contains(n)).collect();
    for bolk in mangler.chunks(20) {
        if hentet == 0 { meld(app, &fil.sti, hentet, bytes, "kobler"); }
        let mut urler = signer_deler(k, portal, &r, bolk, avbryt).await?;
        if urler.is_empty() {
            if !omstart_brukt {
                omstart_brukt = true;
                log::warn!("{navn}: portalen kjente ikke opplastingen igjen — begynner på nytt");
                meld(app, &fil.sti, 0, bytes, "kobler");
                r = multipart_start(k, portal, navn, mime, bytes, sist, mappe_id, avbryt).await?;
                resume_skriv(&rsti, &r);
                // Ny opplasting = alle deler på nytt, så vi ser hele fila igjen. Men hasheren kan alt
                // ha sett deler fra bolkene før — nullstill, ellers blir summen feil (fikset 0.3.0).
                if let Ok(mut h) = hasher.lock() { *h = xxhash_rust::xxh64::Xxh64::new(0); }
                hel = true;
                continue 'omstart;
            }
            return Err("Kunne ikke signere deler — opplastingen kan være utløpt; prøv igjen".into());
        }
        for nr in bolk {
            if avbryt.av() { return Err("Avbrutt".into()); }
            let (start, len) = del_omrade(*nr, del, bytes);
            f.seek(std::io::SeekFrom::Start(start)).await.map_err(|e| format!("{e}"))?;
            let mut buf = vec![0u8; len as usize];
            f.read_exact(&mut buf).await.map_err(|e| format!("{e}"))?;
            // Hash HER, ikke i forsøks-løkka under: delen leses én gang, men kan sendes flere.
            // Delene går i stigende rekkefølge (1..n), så dette gir samme sum som ett gjennomløp.
            if hel { if let Ok(mut h) = hasher.lock() { h.update(&buf); } }
            // ⚠ FRAMDRIFT UNDERVEIS (28/8, Vegard: «går fra 3.8 MB/s til 22 MB/s
            // hele tiden»): før meldte vi først NÅR en 64 MB-del var ferdig. Ved
            // ~10 MB/s betyr det at telleren står bom stille i ~6 sekunder og så
            // hopper 64 MB — og en fartsmåler som sampler hvert sekund ser
            // vekselvis 0 og 64 MB/s. Nå strømmes delen i 256 kB-biter med
            // teller, akkurat som én-PUT-løypa, så tallet blir ekte.
            let buf = Arc::new(buf);
            let mut etag = None;
            let mut siste_feil = String::new();
            let mut ny_signatur_brukt = false;
            let mut n = 0u32;
            while n < DEL_FORSOK {
                // Utløpt signatur (403) ga før opp hele fila. Nå hentes en ny for akkurat denne delen.
                let url = match urler.get(nr) {
                    Some(u) => u.clone(),
                    None => match signer_deler(k, portal, &r, &[*nr], avbryt).await?.remove(nr) {
                        Some(u) => { urler.insert(*nr, u.clone()); u }
                        None => { siste_feil = "fikk ikke ny signatur for delen".into(); break; }
                    },
                };
                let app2 = app.clone();
                let id2 = fil.sti.clone();
                let base = hentet;
                let mut sist_meldt = std::time::Instant::now();
                let sendt = Arc::new(AtomicU64::new(0));
                let sendt2 = sendt.clone();
                let b = buf.clone();
                // Ved omforsøk starter delen på nytt fra `base` — ærlig, for
                // bytene MÅ sendes om igjen.
                // ⚠ STRUPING (31/8): bokføringen lå FØR PUT-en og meldte hele
                // 64 MB-delen på én gang, mens Struper sover maks 2 s per kall.
                // Taket ble dermed 64 MB / 2 s ≈ 32 MB/s uansett hva brukeren
                // satte — altså ingen struping i praksis på store filer, som er
                // nettopp de man vil dempe. Nå meldes hver 256 kB-bit, der
                // søvnen er brøkdelen av et sekund og strupingen faktisk biter.
                let str_bit = struper.clone();
                let strom = futures_util::stream::unfold((0usize, b, str_bit), |(pos, b, s)| async move {
                    if pos >= b.len() { return None; }
                    let slutt = (pos + (1 << 18)).min(b.len());
                    s.tell((slutt - pos) as u64).await;
                    let bit = bytes::Bytes::copy_from_slice(&b[pos..slutt]);
                    Some((Ok::<bytes::Bytes, std::io::Error>(bit), (slutt, b, s)))
                }).inspect(move |r: &Result<bytes::Bytes, std::io::Error>| {
                    if let Ok(bit) = r {
                        let sendt_na = sendt2.fetch_add(bit.len() as u64, Ordering::Relaxed) + bit.len() as u64;
                        if sist_meldt.elapsed().as_millis() > 150 {
                            sist_meldt = std::time::Instant::now();
                            meld(&app2, &id2, (base + sendt_na).min(bytes), bytes, "laster");
                        }
                    }
                });
                let svar = med_stoppvakt(&sendt, Some(avbryt), bare.put(&url).header(reqwest::header::CONTENT_LENGTH, len).body(reqwest::Body::wrap_stream(strom)).send()).await;
                if avbryt.av() { return Err("Avbrutt".into()); }
                match svar {
                    Some(Ok(resp)) if resp.status().is_success() => {
                        etag = resp.headers().get("etag").and_then(|v| v.to_str().ok()).map(|s| s.trim_matches('"').to_string());
                        break;
                    }
                    Some(Ok(resp)) if resp.status().as_u16() == 403 && !ny_signatur_brukt => {
                        // Signaturen har gått ut (URL-ene lever en time — en lang pause gjør det). Ny
                        // signatur med én gang: det er ikke nettet som feiler, så ingen pause.
                        ny_signatur_brukt = true;
                        urler.remove(nr);
                        siste_feil = "signaturen hadde gått ut".into();
                        log::warn!("{navn} del {nr}: signaturen hadde gått ut — henter ny");
                        n += 1;
                        continue;
                    }
                    Some(Ok(resp)) => {
                        let s = resp.status().as_u16();
                        siste_feil = format!("Lageret svarte {s}");
                        if !forbigaaende(s) { break; }
                    }
                    Some(Err(e)) => siste_feil = nettfeil(e),
                    None => siste_feil = "forbindelsen sto stille — ingenting beveget seg på ett minutt".into(),
                }
                n += 1;
                if n >= DEL_FORSOK { break; }
                log::warn!("{navn} del {nr}/{antall} forsøk {n} feilet: {siste_feil}");
                let (a, id, h) = (app.clone(), fil.sti.clone(), hentet);
                vent_paa_nett(portal, Some(avbryt), &move || meld(&a, &id, h, bytes, "venterNett")).await;
                meld(app, &fil.sti, hentet, bytes, "nyttForsok");
                sov(pause_foer_forsok(n - 1), Some(avbryt)).await;
                if avbryt.av() { return Err("Avbrutt".into()); }
            }
            let etag = etag.ok_or_else(|| format!("Del {nr} av {antall} feilet ({siste_feil}) — prøv igjen, den fortsetter fra del {nr}"))?;
            r.deler.push((*nr, etag)); resume_skriv(&rsti, &r);
            hentet = (hentet + len).min(bytes);
            meld(app, &fil.sti, hentet, bytes, "laster");
        }
    }
    break;
    }
    // Sett sammen. R2 bruker maalbar tid pa a lime sammen 100+ deler — det er
    // her «star lenge pa 100 %» begynner.
    meld(app, &fil.sti, bytes, bytes, "setter");
    // Stigende rekkefølge: lageret krever det, og etter et gjenopptak kan bokføringen ha dem hulter til bulter.
    r.deler.sort_by_key(|(n, _)| *n);
    let deler: Vec<serde_json::Value> = r.deler.iter().map(|(n, e)| serde_json::json!({ "nr": n, "etag": e })).collect();
    let (st, d) = portal_post(k, portal, &serde_json::json!({ "action": "multipart-fullfor", "originalKey": r.original_key, "uploadId": r.upload_id, "deler": deler }), Some(avbryt)).await?;
    if !d["ok"].as_bool().unwrap_or(false) {
        // 410 = byte-vakta hos serveren slo til (fila ble ikke hel) og har slettet den. Resume-tilstanden
        // er da verdiløs, og neste forsøk skal begynne på nytt.
        if st == 410 { let _ = std::fs::remove_file(&rsti); }
        return Err(d["error"].as_str().unwrap_or("Kunne ikke sette sammen fila").to_string());
    }
    let _ = std::fs::remove_file(&rsti);
    // Hashen er bare gyldig når VI har lest hver eneste del i denne kjøringen. Ble opplastingen
    // gjenopptatt, hoppet vi over ferdige deler og har dermed ikke sett hele fila — da sendes ingen
    // sjekksum, og nedlasting faller tilbake på størrelsessjekk (som den allerede håndterer).
    // Å gjette her ville vært verre enn å la være: en FEIL sjekksum gjør hver senere nedlasting
    // til en falsk verifiseringsfeil.
    let xxh = if hel { hasher.lock().ok().map(|h| format!("{:016x}", h.digest())) } else { None };
    Ok((r.original_key.clone(), hentet, r.proxy_put.clone(), xxh))
}

/// Fil → byte-strøm m/ teller (PUT-framdrift).
/// ⚠ `hash` er valgfri, og den er hele poenget med at opplasting ikke lenger tar dobbelt så lang
/// tid. Før regnet vi xxh64 i et EGET fullt gjennomløp av fila FØR første byte ble sendt — altså
/// leste vi 65 MB to ganger for å laste opp 65 MB, og telleren sto på null hele den første runden.
/// Nå hashes bitene mens de likevel går ut på nett, så sjekksummen er gratis og overføringen
/// begynner med en gang. Den er samtidig BEDRE: den er regnet over nøyaktig de bytene som faktisk
/// ble sendt, ikke over fila slik den lå da vi begynte.
pub fn fil_strom(f: tokio::fs::File, struper: Arc<Struper>, avbryt: Stopp, hash: Option<Arc<std::sync::Mutex<xxhash_rust::xxh64::Xxh64>>>, mut tell: impl FnMut(u64) + Send + 'static) -> impl futures_util::Stream<Item = Result<bytes::Bytes, std::io::Error>> + Send + 'static {
    use tokio::io::AsyncReadExt;
    futures_util::stream::unfold((f, struper, avbryt, hash), |(mut f, struper, avbryt, hash)| async move {
        // Avbryt MIDT i en PUT (22/8-bug: en gjenglemt opplasting levde videre etter
        // reload og rapporterte til samme rad som den nye) — kutt strømmen, så
        // feiler PUT-en og opprydding (action avbryt) kjører.
        if avbryt.av() { return Some((Err(std::io::Error::other("Avbrutt")), (f, struper, avbryt, hash))); }
        let mut buf = vec![0u8; 1 << 20];
        match f.read(&mut buf).await {
            Ok(0) => None,
            Ok(n) => {
                buf.truncate(n);
                if let Some(h) = &hash { if let Ok(mut h) = h.lock() { h.update(&buf); } }
                struper.tell(n as u64).await;
                Some((Ok(bytes::Bytes::from(buf)), (f, struper, avbryt, hash)))
            }
            Err(e) => Some((Err(e), (f, struper, avbryt, hash))),
        }
    }).inspect(move |r| { if let Ok(b) = r { tell(b.len() as u64); } })
}

pub async fn last_opp_en<K: Melder + Etterarbeid>(app: &K, k: &reqwest::Client, bare: &reqwest::Client, portal: &str, fil: &OppFil, mappe_id: &str, avbryt: &Stopp, struper: Arc<Struper>) -> Result<(), String> {
    let navn = std::path::Path::new(&fil.sti).file_name().and_then(|n| n.to_str()).unwrap_or("fil").to_string();
    let mime = mime_fra(&navn);
    let meta = tokio::fs::metadata(&fil.sti).await.map_err(|e| format!("{e}"))?;
    let bytes = meta.len();
    let sist = meta.modified().ok().and_then(|m| m.duration_since(std::time::UNIX_EPOCH).ok()).map(|d| d.as_millis() as u64).unwrap_or(0);
    // Store filer: multipart m/ resume. Små: én PUT som før.
    if bytes > DEL_GRENSE {
        let (key, _, proxy_put, xxh) = last_opp_multipart(app, k, bare, portal, fil, mappe_id, avbryt, struper.clone(), &navn, mime, bytes, sist).await?;
        let proxy = app.avspillingskopi(bare, portal, &fil.sti, &navn, &proxy_put, bytes).await;
        return fullfor_opplasting(app, k, portal, fil, &key, &navn, mime, bytes, sist, mappe_id, xxh, proxy).await;
    }
    // 1) presign
    meld(app, &fil.sti, 0, bytes, "kobler");
    let (st, d) = portal_post(k, portal, &serde_json::json!({ "action": "presign", "filnavn": navn, "mimeType": mime, "filstorrelse": bytes, "sistEndret": sist, "mappeId": mappe_json(mappe_id) }), Some(avbryt)).await?;
    if st == 401 || st == 403 { return Err("Ikke tilgang — logg inn på nytt (API-nøkler kan ikke laste opp)".into()); }
    if d["kvoteSperre"].as_bool().unwrap_or(false) { return Err(d["error"].as_str().unwrap_or("Lagringen er full").to_string()); }
    let (url, key) = match (d["uploadUrl"].as_str(), d["originalKey"].as_str()) { (Some(u), Some(k)) => (u.to_string(), k.to_string()), _ => return Err(d["error"].as_str().unwrap_or("presign feilet").to_string()) };
    let proxy_put = d["proxyPutUrl"].as_str().unwrap_or("").to_string();
    // 2) PUT rett til R2 m/ framdrift — og med omforsøk (0.3.0).
    //
    // URL-en lever et kvarter, så et nytt forsøk trenger ingen ny signering. Hvert forsøk leser fila
    // på nytt og får en FERSK hasher: sjekksummen skal beskrive bytene i forsøket som lyktes, ikke
    // summen av alle forsøkene — da ville hver senere nedlasting feilet verifiseringen.
    // Gir vi opp, ryddes nøkkelen hos serveren før vi returnerer: filforsøket i last_opp begynner da
    // med en ny signering, og ingen halv rad blir liggende til feiekosten.
    let mut n = 0u32;
    let xxh = loop {
        let hasher = Arc::new(std::sync::Mutex::new(xxhash_rust::xxh64::Xxh64::new(0)));
        let f = tokio::fs::File::open(&fil.sti).await.map_err(|e| format!("{e}"))?;
        let id = fil.sti.clone(); let app2 = app.clone();
        let sendt = Arc::new(AtomicU64::new(0)); let sendt2 = sendt.clone();
        let mut sist_meldt = std::time::Instant::now();
        let strom = fil_strom(f, struper.clone(), avbryt.clone(), Some(hasher.clone()), move |b| {
            let t = sendt2.fetch_add(b, Ordering::Relaxed) + b;
            if sist_meldt.elapsed().as_millis() > 150 || t == bytes { sist_meldt = std::time::Instant::now(); meld(&app2, &id, t, bytes, "laster"); }
        });
        let svar = med_stoppvakt(&sendt, Some(avbryt), bare.put(&url).header(reqwest::header::CONTENT_TYPE, mime).header(reqwest::header::CONTENT_LENGTH, bytes).body(reqwest::Body::wrap_stream(strom)).send()).await;
        if avbryt.av() {
            forkast_nokler(k, portal, &[key.clone()]).await;
            return Err("Avbrutt".into());
        }
        let (feil, gi_opp) = match svar {
            Some(Ok(r)) if r.status().is_success() => break hasher.lock().ok().map(|h| format!("{:016x}", h.digest())),
            // 403 = URL-en har gått ut: gi opp HER, så filforsøket henter en ny signering.
            Some(Ok(r)) => { let s = r.status().as_u16(); (format!("Lageret svarte {s}"), !forbigaaende(s)) }
            Some(Err(e)) => (nettfeil(e), false),
            None => ("forbindelsen sto stille — ingenting beveget seg på ett minutt".to_string(), false),
        };
        if gi_opp || n + 1 >= ENKEL_FORSOK {
            forkast_nokler(k, portal, &[key.clone()]).await;
            return Err(feil);
        }
        log::warn!("opp {navn} forsøk {} feilet: {feil}", n + 1);
        let (a, id) = (app.clone(), fil.sti.clone());
        vent_paa_nett(portal, Some(avbryt), &move || meld(&a, &id, 0, bytes, "venterNett")).await;
        meld(app, &fil.sti, 0, bytes, "nyttForsok");
        sov(pause_foer_forsok(n), Some(avbryt)).await;
        n += 1;
    };
    let proxy = app.avspillingskopi(bare, portal, &fil.sti, &navn, &proxy_put, bytes).await;
    fullfor_opplasting(app, k, portal, fil, &key, &navn, mime, bytes, sist, mappe_id, xxh, proxy).await
}

/// Fullfør-steget (server: thumb/EXIF/dedup for bilder; video: poster/sprite fra appens etterarbeid).
pub async fn fullfor_opplasting<K: Melder + Etterarbeid>(app: &K, k: &reqwest::Client, portal: &str, fil: &OppFil, key: &str, navn: &str, mime: &str, bytes: u64, sist: u64, mappe_id: &str, xxh: Option<String>, proxy: Option<u64>) -> Result<(), String> {
    let mut body = serde_json::json!({ "action": "fullfor", "originalKey": key, "filnavn": navn, "mimeType": mime, "filstorrelse": bytes, "sistEndret": sist, "mappeId": mappe_json(mappe_id) });
    if let Some(x) = xxh { body["xxh64"] = serde_json::json!(x); }
    // Proxyen ligger alt i lageret — si fra, så slipper serveren å be Cloudflare
    // Stream (som feiler på ProRes) og Rawcode å laste ned originalen på nytt.
    // Ingen proxy Å sende? Si HVORFOR når grunnen er at formatet ikke kan
    // leses (ProRes RAW): serveren dropper da Stream-forsøket i stedet for å
    // la Cloudflare hente titalls GB som uansett feiler. Proben er billig.
    //
    // ⚠ OG SI DET TIL BRUKEREN. Flagget gikk før BARE til serveren: fila ble
    // grønn og ferdig i lista, og at den aldri får en avspillingskopi oppdaget
    // man først dagen etter, i skapet, ved å holde musa over et lite merke —
    // altså i en annen app, lenge etter at maskina var lukket. Beskjeden hører
    // hjemme HER, hvor fila ligger på disken og «Lenk lokal proxy …» står i
    // høyreklikk-menyen rett ved siden av (Vegard 11/9).
    let mut uten_proxy = false;
    if proxy.is_none() && app.ulesbar_video(&fil.sti, navn).await {
        body["proxyUlesbar"] = serde_json::json!(true); uten_proxy = true;
    }
    if let Some(n) = proxy {
        // ⚠ MÅ være bit for bit lik serverens `proxyNokkel` — den avviser en
        // nøkkel den ikke selv ville laget. Reservefallet bruker det TRIMMEDE
        // navnet; `unwrap_or(key)` ville sneket «originals/» inn igjen.
        let basis = key.trim_start_matches("originals/");
        let basis = basis.rsplit_once('.').map(|(a, _)| a).unwrap_or(basis);
        body["proxyKey"] = serde_json::json!(format!("proxy/{basis}.mp4"));
        body["proxyStorrelse"] = serde_json::json!(n);
    }
    // Serveren lager thumb/EXIF/dedup her; for video kjorer ffmpeg LOKALT forst
    // (poster + sprite). Begge deler skjer ETTER at siste byte er sendt — uten
    // status sto appen bare stille pa 100 % (Vegard 28/8).
    let _ = app.framdrift(Framdrift { id: fil.sti.clone(), hentet: bytes, total: bytes, status: "fullfor".into(), feil: None });
    app.videofelt(&fil.sti, navn, bytes, &mut body).await;
    // Med omforsøk (0.3.0): bytene ligger alt i lageret, så et nett-hikk her skal ikke koste en hel
    // ny opplasting. Serveren er idempotent på fullfør («allerede»).
    let (st, d) = portal_post(k, portal, &body, None).await?;
    if !(200..300).contains(&st) { return Err(format!("fullfør: {}", d["error"].as_str().unwrap_or("feilet"))); }
    // «utenProxy» er en FERDIG tilstand — fila ER lastet opp, og originalen er
    // trygt i skapet. Den sier bare at det mangler noe å spille av, og hva man
    // gjør med det. Derfor teller den som fullført alle steder i UI-et.
    let status = if d["allerede"].as_bool().unwrap_or(false) { "hoppet" }
        else if uten_proxy { "utenProxy" } else { "ferdig" };
    let _ = app.framdrift(Framdrift { id: fil.sti.clone(), hentet: bytes, total: bytes, status: status.into(), feil: None });
    Ok(())
}

/// Laster proxy-fila opp STRØMMET fra disk, med omforsøk (0.3.0).
///
/// ⚠ Før ble hele fila lest inn i minnet (1–2 GB for et langt 8K-klipp) og sendt i ÉTT forsøk. Et
/// nett-hikk der ga et kort uten avspillingskopi — og for ProRes RAW finnes ingen annen vei til en:
/// verken Stream eller Rawcode kan lage den. Returnerer størrelsen, eller None om den ikke kom opp.
pub async fn put_proxy(bare: &reqwest::Client, portal: &str, put_url: &str, sti: &Path) -> Option<u64> {
    let n = tokio::fs::metadata(sti).await.ok()?.len();
    if n <= 10_000 { return None; }
    let ingen_grense = Struper::ny(Arc::new(AtomicU64::new(0)));
    let aldri = Stopp { global: Arc::new(AtomicBool::new(false)), sett: Arc::new(std::sync::Mutex::new(Default::default())), id: String::new() };
    for forsok in 0..4u32 {
        let f = tokio::fs::File::open(sti).await.ok()?;
        let sendt = Arc::new(AtomicU64::new(0));
        let s2 = sendt.clone();
        let strom = fil_strom(f, ingen_grense.clone(), aldri.clone(), None, move |b| { s2.fetch_add(b, Ordering::Relaxed); });
        let svar = med_stoppvakt(&sendt, None, bare.put(put_url).header(reqwest::header::CONTENT_TYPE, "video/mp4").header(reqwest::header::CONTENT_LENGTH, n).body(reqwest::Body::wrap_stream(strom)).send()).await;
        let feil = match svar {
            Some(Ok(r)) if r.status().is_success() => return Some(n),
            Some(Ok(r)) => {
                let s = r.status().as_u16();
                if !forbigaaende(s) { log::warn!("proxyen ble avvist av lageret ({s})"); return None; }
                format!("Lageret svarte {s}")
            }
            Some(Err(e)) => nettfeil(e),
            None => "forbindelsen sto stille".into(),
        };
        log::warn!("proxy-opplasting forsøk {} feilet: {feil}", forsok + 1);
        if forsok < 3 {
            vent_paa_nett(portal, None, &|| {}).await;
            sov(pause_foer_forsok(forsok), None).await;
        }
    }
    None
}

/// «Fjern» på en opplasting som ikke ble ferdig (0.3.0): rydd opp etter den med én gang.
///
/// Før ble bare raden i køen fjernet. Resume-fila, den åpne multiparten hos lageret og raden i
/// portalens venteliste ble liggende til feiekosten tok dem — tidligst etter 72 stille timer, og
/// multiparten tar plass hos lageret hele tiden. Nå får serveren beskjed (samme «avbryt» som
/// nettleseren bruker), og resume-fila slettes når serveren har svart.
///
/// ⚠ Pause og «Avbryt» skal IKKE kalle dette — der er resume-tilstanden selve poenget.
/// `ua` er appens User-Agent (se `klient`).
pub async fn forkast_opplasting(portal: String, nokkel: String, filer: Vec<OppFil>, ua: &'static str) -> Result<u32, String> {
    let mut nokler = Vec::new();
    let mut stier = Vec::new();
    for f in &filer {
        // Samme nøkkel som last_opp_en: sti + størrelse + endret-tid (ms). Finnes ikke fila lenger,
        // finner vi heller ikke resume-fila — da tar feiekosten den, som før.
        let Ok(m) = tokio::fs::metadata(&f.sti).await else { continue };
        let sist = m.modified().ok().and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok()).map(|d| d.as_millis() as u64).unwrap_or(0);
        let rsti = resume_sti(&f.sti, m.len(), sist);
        if let Some(r) = resume_les(&rsti) {
            if !r.original_key.is_empty() { nokler.push(r.original_key); }
            stier.push(rsti);
        }
    }
    if !nokler.is_empty() {
        let k = klient(&nokkel, ua)?;
        let portal = portal.trim_end_matches('/');
        for bolk in nokler.chunks(500) {
            let r = k.post(format!("{portal}/api/rawskap/opplasting")).json(&serde_json::json!({ "action": "avbryt", "originalKeys": bolk })).send().await.map_err(nettfeil)?;
            if !r.status().is_success() { return Err(format!("Portalen svarte {}", r.status().as_u16())); }
        }
        log::info!("forkastet {} påbegynte opplastinger", nokler.len());
    }
    for s in &stier { let _ = std::fs::remove_file(s); }
    Ok(nokler.len() as u32)
}

/// OPPLASTINGSØKT (5/10, Vegard: «jeg må passe på at denne blir ferdig før jeg sender den»): meld portalen
/// FASITEN for en opplastingsjobb (antall + bytes + mappe), fremdrift underveis og slutten — så den vet når ALT
/// er oppe og kan sende en ventende deling (lib/opplasting-okt.ts i portalen). Bare de tre okt-handlingene slippes
/// gjennom; en feil her skal aldri stoppe selve opplastingen (JS ignorerer den).
/// `ua` er appens User-Agent (se `klient`).
pub async fn opplasting_okt(portal: String, nokkel: String, data: serde_json::Value, ua: &'static str) -> Result<serde_json::Value, String> {
    let handling = data["action"].as_str().unwrap_or("");
    if !matches!(handling, "okt-start" | "okt-framdrift" | "okt-ferdig") { return Err("ukjent handling".into()); }
    let portal = portal.trim_end_matches('/').to_string();
    let k = klient(&nokkel, ua)?;
    let r = k.post(format!("{}/api/rawskap/opplasting", portal)).json(&data).send().await.map_err(nettfeil)?;
    let st = r.status().as_u16();
    let d: serde_json::Value = r.json().await.unwrap_or(serde_json::json!({}));
    if !(200..300).contains(&st) { return Err(d["error"].as_str().unwrap_or("økt-kallet feilet").to_string()); }
    Ok(d)
}
