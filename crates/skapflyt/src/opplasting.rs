// Opplasting til skapet: enkel PUT, multipart med gjenopptak (én eller flere deler samtidig), fullfør, forkast og økt.
//
// T2 (24/10): det rawMotion trenger før det flytter over — `Kilde` (bytene trenger ikke være en fil), `Fil` med tags,
// opptaksdato, kjent sjekksum, ny utgave (`versjonAv`) og ekstra felt, parallelle deler med xxh64 i rekkefølge, `Opplastet`
// med fil-id-en, `proxyKey` fra serverens tilbud, tilstand v2 og `uferdige()`. Transfer bruker én del om gangen og
// sender det samme som før.
//
// F8 (0.2.1): proxy-adressen i tilbudet lever 7200 s. Er den eldre enn PROXY_FERSK_MS når originalen er oppe
// (gjenopptak dagen etter, eller en fil som tar over to timer), sendes ingen PUT dit: fullfør går uten kopien, og den
// lenkes etterpå med en fersk adresse (`proxy-lenk-start` → avspillingskopi → `proxy-lenk-fullfor`).
use crate::feil::disk;
use crate::{forbigaaende, mappe_json, med_stoppvakt, meld, mime_fra, nettfeil, pause_foer_forsok, portal_post, sov, vent_paa_nett, Etterarbeid, Feil, Framdrift, Klient, Melder, OppFil, Stopp, Struper, DEL_FORSOK, ENKEL_FORSOK};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

// ── MULTIPART + RESUME (22/8): filer over DEL_GRENSE går i deler (64 MB → 16 MB i 0.2.8,
// og fra 0.3.0 vokser delen med fila — se delstorrelse).
// Tilstand per fil ligger i en liten JSON i appens datamappe — uploadId, originalKey, ferdige deler m/ ETag. Starter man
// på nytt (nettbrudd, lukket app) fortsettes fra siste ferdige del. Rådes til å ha lifecycle-regel i R2 for forlatte
// multiparts. (T2: format v2 med stien og mappen i nøkkelen — se `Resume`.)
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
/// Så mye kan ligge i minnet samtidig med flere deler i lufta (lest, men ikke sendt ferdig). Med én del om gangen
/// (Transfer) leses neste del først når forrige er oppe, som før T2.
pub const I_LUFTA: u64 = 256 * 1024 * 1024;
/// Versjonen tilstandsfilene skrives med. Filer uten `versjon` er v1 (før T2) og leses fortsatt.
pub const TILSTAND_VERSJON: u32 = 2;
/// Så gammel kan proxy-adressen fra tilbudet være når avspillingskopien skal opp (F8). Serveren signerer den for
/// 7200 s; de 20 minuttene som er igjen, er marginen for at selve PUT-en av kopien tar tid.
pub const PROXY_FERSK_MS: u64 = 100 * 60 * 1000;

/// Klokka i ms siden epoch (0 hvis den står før 1970 — da regnes en adresse som gammel).
fn naa_ms() -> u64 { std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0) }

/// Kan proxy-adressen som ble mottatt `proxy_tid`, brukes `naa`? (F8)
///
/// 0 = ukjent alder (tilstand fra før feltet fantes) og regnes som gammel. En tid langt FRAM i tid (klokka ble stilt
/// tilbake etter tilbudet) er like ukjent, derfor avstanden begge veier.
pub fn proxy_fersk(proxy_tid: u64, naa: u64) -> bool { proxy_tid > 0 && naa.abs_diff(proxy_tid) < PROXY_FERSK_MS }

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

// ── HVA SOM LASTES OPP ──────────────────────────────────────────────────────────────────────────

/// Bytene til en opplasting som ikke er en vanlig fil — noe som lages underveis (rawMotions tar-arkiv), lest i biter.
/// Lengden må være kjent og fast før første byte (multipart).
pub trait Kilde: Send + Sync {
    fn lengde(&self) -> u64;
    /// Fyller hele `buf` med bytene fra `start`. Kalles fra en blokkerende tråd (`spawn_blocking`).
    fn les(&self, start: u64, buf: &mut [u8]) -> std::io::Result<()>;
}

/// En fil som skal til skapet.
///
/// Alt utover sti, navn, mime og mappe er valgfritt: tomt/None betyr at INGENTING sendes, så det Transfer sender, er
/// det samme som før T2.
#[derive(Clone, Default)]
pub struct Fil {
    /// Fila på disken — eller, med `kilde`, det som identifiserer opplastingen (tilstandsfila og framdriften følger den).
    pub sti: PathBuf,
    /// Bytene, når de ikke er fila `sti`.
    pub kilde: Option<Arc<dyn Kilde>>,
    /// Filnavnet i skapet.
    pub navn: String,
    pub mime: String,
    /// Mappen i skapet (tom = rota).
    pub mappe_id: String,
    /// Tagger (`tags`) ved presign/multipart-start (feiekosten fullfører med dem) og i fullfør.
    pub tags: Vec<String>,
    /// `sistEndret` til serveren (opptaksdato for video og filer uten EXIF). None = filas mtime, som før.
    pub sist_endret_ms: Option<u64>,
    /// Sjekksummen fra før (innlesingen): sendes også når opplastingen gjenopptas og bytene ikke er lest i denne kjøringen.
    pub kjent_xxh64: Option<String>,
    /// Ny utgave av en fil i skapet (`versjonAv: <bildeId>`).
    pub versjon_av: Option<String>,
    /// Utgavenotat til fullfør (med `versjon_av`).
    pub utgavenotat: Option<String>,
    /// Ekstra felt til fullfør (rawmotion, bredde, hoyde …). Overstyrer aldri feltene craten selv setter.
    pub felt: serde_json::Map<String, serde_json::Value>,
}

impl Fil {
    /// En vanlig fil på disken: navnet er filnavnet, typen fra endelsen — det samme Transfer alltid har gjort.
    pub fn fra_sti(sti: impl Into<PathBuf>, mappe_id: &str) -> Fil {
        let sti = sti.into();
        let navn = sti.file_name().and_then(|n| n.to_str()).unwrap_or("fil").to_string();
        let mime = mime_fra(&navn).to_string();
        Fil { sti, navn, mime, mappe_id: mappe_id.to_string(), ..Default::default() }
    }
}

/// Hvordan opplastingen går.
#[derive(Clone)]
pub struct Valg {
    /// Der tilstandsfilene ligger. Transfer: `resume_dir()`.
    pub tilstand_dir: PathBuf,
    /// Over denne går fila i deler med gjenopptak (Transfer: DEL_GRENSE). 0 = alltid i deler.
    pub del_grense: u64,
    /// Deler i lufta samtidig. 1 = Transfer (filene går i parallell i stedet). Delene LESES alltid i rekkefølge.
    pub deler_samtidig: usize,
    /// Felles struping (token-bøtta). None = ubegrenset.
    pub struper: Option<Arc<Struper>>,
}

impl Default for Valg {
    fn default() -> Self { Valg { tilstand_dir: resume_dir(), del_grense: DEL_GRENSE, deler_samtidig: 1, struper: None } }
}

/// Resultatet: fila i skapet.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Opplastet {
    /// Fil-id-en i skapet.
    pub id: String,
    /// Fila fantes fra før (samme opplasting, eller dublett-vernet) — ingen ny rad.
    pub allerede: bool,
    pub original_key: String,
    /// Sjekksummen som ble sendt (regnet underveis, eller `kjent_xxh64`). None = ingen sendt.
    pub xxh64: Option<String>,
    /// Ny utgave: roten i stakken, slik serveren svarte.
    pub versjon_av: Option<String>,
}

/// En påbegynt opplasting fra tilstandsfilene — det appen kan tilby å fortsette etter en omstart (F9).
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Uferdig {
    pub sti: PathBuf,
    pub navn: String,
    pub mappe_id: String,
    pub bytes: u64,
    /// Bytene den lokale bokføringen sier er oppe (lageret er fasiten når den fortsettes).
    pub sendt: u64,
    /// Filas mtime (ms) slik den var — ulik nå betyr at fila er endret, og da fortsettes den ikke.
    pub sist_endret: u64,
}

// ── TILSTANDEN ──────────────────────────────────────────────────────────────────────────────────

/// Tilstanden til én opplasting.
///
/// v2 (T2): `{ versjon: 2, sti, navn, mappe_id, bytes, sist_endret, upload_id, original_key, del_bytes, deler, proxy_put,
/// proxy_key, proxy_tid, satt_sammen, xxh64 }` i `xxh64(sti|bytes|mtime_ms|mappe).json` — samme fil til to mapper deler
/// ikke lenger tilstand (F6). `proxy_tid` (F8, 0.2.1) mangler i filer skrevet før, og leses da som 0. v1 (før T2: ingen `versjon`, nøkkel uten mappe, `del_bytes` 0 = 64 MiB) leses fortsatt og flyttes til
/// v2 første gang, så opplastinger som er i gang hos kunder, overlever oppdateringen.
///
/// `satt_sammen` = bytene ligger hele i lageret (enkel PUT ferdig, eller multiparten satt sammen); bare avspillingskopi og
/// fullfør gjenstår. Tilstanden står til fullfør har svart, så et nytt forsøk fullfører den SAMME opplastingen (P1:
/// «midlertidig» på `versjonAv` lar alt stå) i stedet for å sende alle bytene igjen.
#[derive(Clone, Serialize, Deserialize, Default, Debug, PartialEq)]
pub struct Resume {
    #[serde(default)] pub versjon: u32,
    #[serde(default)] pub sti: String,
    #[serde(default)] pub navn: String,
    #[serde(default)] pub upload_id: String,
    #[serde(default)] pub original_key: String,
    #[serde(default)] pub mappe_id: String,
    #[serde(default)] pub bytes: u64,
    /// Filas mtime (ms) — den som er med i nøkkelen.
    #[serde(default)] pub sist_endret: u64,
    #[serde(default)] pub deler: Vec<(u32, String)>,
    #[serde(default)] pub proxy_put: String,
    /// ⚠ Delstørrelsen opplastingen FAKTISK ble startet med. Må følge tilstanden, ikke leses fra
    /// konstanten: endrer vi DEL_BYTES, ville en gjenopptatt opplasting fra forrige versjon fått
    /// delene skrevet på feil forskyvning — korrupt fil, uten feilmelding. 0 = skrevet før dette
    /// feltet fantes, og da var den alltid 64 MB.
    #[serde(default)] pub del_bytes: u64,
    /// Serverens proxy-nøkkel fra tilbudet (F7). Tom i v1 — da bygges den som før.
    #[serde(default)] pub proxy_key: String,
    /// Da `proxy_put` ble mottatt, i ms siden epoch (F8, 0.2.1). Adressen lever 7200 s; er den eldre enn
    /// PROXY_FERSK_MS når kopien skal opp, lenkes kopien etter fullfør i stedet. 0 = ukjent alder (skrevet før feltet
    /// fantes) og regnes som gammel.
    #[serde(default)] pub proxy_tid: u64,
    #[serde(default)] pub satt_sammen: bool,
    /// Sjekksummen regnet i kjøringen som satte sammen fila (tom = ukjent).
    #[serde(default)] pub xxh64: String,
}

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
/// v2-nøkkelen: sti, størrelse, mtime OG mappe.
pub fn tilstand_sti(dir: &Path, sti: &str, bytes: u64, mtime: u64, mappe: &str) -> PathBuf {
    let h = xxhash_rust::xxh64::xxh64(format!("{sti}|{bytes}|{mtime}|{mappe}").as_bytes(), 0);
    dir.join(format!("{h:016x}.json"))
}
/// v1-nøkkelen (før T2): uten mappe. Bare for å lese gamle filer.
pub fn tilstand_sti_v1(dir: &Path, sti: &str, bytes: u64, mtime: u64) -> PathBuf {
    let h = xxhash_rust::xxh64::xxh64(format!("{sti}|{bytes}|{mtime}").as_bytes(), 0);
    dir.join(format!("{h:016x}.json"))
}
pub fn resume_les(p: &Path) -> Option<Resume> { std::fs::read(p).ok().and_then(|b| serde_json::from_slice(&b).ok()) }
pub fn resume_skriv(p: &Path, r: &Resume) { if let Ok(b) = serde_json::to_vec(r) { let tmp = p.with_extension("tmp"); if std::fs::write(&tmp, b).is_ok() { let _ = std::fs::rename(&tmp, p); } } }

fn mtime_ms(m: &std::fs::Metadata) -> u64 { m.modified().ok().and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok()).map(|d| d.as_millis() as u64).unwrap_or(0) }

/// Tilstanden for denne opplastingen: v2 hvis den finnes, ellers v1 flyttet til v2. (Tom = ny opplasting.)
fn les_tilstand(dir: &Path, sti: &str, navn: &str, bytes: u64, mtime: u64, mappe: &str) -> (Resume, PathBuf) {
    let _ = std::fs::create_dir_all(dir);
    let p2 = tilstand_sti(dir, sti, bytes, mtime, mappe);
    if let Some(r) = resume_les(&p2).filter(|r| r.versjon >= TILSTAND_VERSJON && r.bytes == bytes && r.mappe_id == mappe && r.sti == sti) {
        return (r, p2);
    }
    // ⚠ v1 (før T2) ligger under nøkkelen UTEN mappe, og gjelder bare når mappen stemmer — samme filter som før. Den
    // flyttes til v2 her, så en opplasting som var i gang da appen oppdaterte seg, fortsetter der den slapp.
    let p1 = tilstand_sti_v1(dir, sti, bytes, mtime);
    if let Some(mut r) = resume_les(&p1).filter(|r| r.versjon < TILSTAND_VERSJON && r.bytes == bytes && r.mappe_id == mappe) {
        r.versjon = TILSTAND_VERSJON;
        r.sti = sti.to_string();
        r.navn = navn.to_string();
        r.sist_endret = mtime;
        resume_skriv(&p2, &r);
        if p2.exists() { let _ = std::fs::remove_file(&p1); }
        log::info!("gjenopptar {navn}: tilstand fra en tidligere versjon flyttet til v2");
        return (r, p2);
    }
    (Resume::default(), p2)
}

/// Påbegynte opplastinger i `valg.tilstand_dir` (v2). v1-filer har ikke stien og kan ikke tilbys; de fortsettes når fila
/// lastes opp igjen. Synk-minnet (`synk-*.json`) og andre filer hoppes over.
pub fn uferdige(valg: &Valg) -> Vec<Uferdig> {
    let mut ut: Vec<Uferdig> = alle_v2(&valg.tilstand_dir).into_iter().map(|(_, r)| {
        let del = if r.del_bytes > 0 { r.del_bytes } else { DEL_BYTES_GAMMEL };
        let sendt = if r.satt_sammen { r.bytes } else { r.deler.iter().map(|(nr, _)| del_omrade(*nr, del, r.bytes).1).sum::<u64>().min(r.bytes) };
        Uferdig { sti: PathBuf::from(&r.sti), navn: r.navn, mappe_id: r.mappe_id, bytes: r.bytes, sendt, sist_endret: r.sist_endret }
    }).collect();
    ut.sort_by(|a, b| a.sti.cmp(&b.sti).then(a.mappe_id.cmp(&b.mappe_id)));
    ut
}

fn alle_v2(dir: &Path) -> Vec<(PathBuf, Resume)> {
    let Ok(rd) = std::fs::read_dir(dir) else { return vec![] };
    rd.filter_map(|e| e.ok()).map(|e| e.path())
        .filter(|p| p.extension().and_then(|x| x.to_str()) == Some("json") && !p.file_name().and_then(|n| n.to_str()).unwrap_or("").starts_with("synk-"))
        .filter_map(|p| resume_les(&p).filter(|r| r.versjon >= TILSTAND_VERSJON && !r.sti.is_empty() && !r.original_key.is_empty()).map(|r| (p, r)))
        .collect()
}

// ── DELENE ──────────────────────────────────────────────────────────────────────────────────────

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

/// Hva lageret sier om en påbegynt opplasting.
pub(crate) enum MpStatus { Deler { deler: Vec<(u32, u64, String)>, del_bytes: u64 }, Ferdig, Borte, Ukjent(String) }

pub(crate) async fn multipart_status(k: &Klient, r: &Resume, avbryt: &Stopp) -> MpStatus {
    match portal_post(k, &serde_json::json!({ "action": "multipart-status", "originalKey": r.original_key, "uploadId": r.upload_id }), Some(avbryt)).await {
        // 410 = multiparten er borte hos lageret; 404 = portalen kjenner den ikke lenger (feiekosten
        // ryddet raden etter 72 stille timer). Begge betyr: ingenting å fortsette på.
        Ok((410, _)) | Ok((404, _)) => MpStatus::Borte,
        Ok((s, d)) if (200..300).contains(&s) => {
            if d["ferdig"].as_bool().unwrap_or(false) { return MpStatus::Ferdig; }
            let deler = d["deler"].as_array().map(|a| a.iter().filter_map(|x| Some((x["nr"].as_u64()? as u32, x["bytes"].as_u64().unwrap_or(0), x["etag"].as_str()?.trim_matches('"').to_string()))).collect()).unwrap_or_default();
            MpStatus::Deler { deler, del_bytes: d["delBytes"].as_u64().unwrap_or(0) }
        }
        Ok((s, d)) => MpStatus::Ukjent(format!("{s} {}", d["error"].as_str().unwrap_or(""))),
        Err(e) => MpStatus::Ukjent(e.to_string()),
    }
}

pub(crate) async fn signer_deler(k: &Klient, upload_id: &str, original_key: &str, numre: &[u32], avbryt: &Stopp) -> Result<HashMap<u32, String>, Feil> {
    let (_, d) = portal_post(k, &serde_json::json!({ "action": "multipart-deler", "originalKey": original_key, "uploadId": upload_id, "deler": numre }), Some(avbryt)).await?;
    Ok(d["deler"].as_array().map(|a| a.iter().filter_map(|x| Some((x["nr"].as_u64()? as u32, x["url"].as_str()?.to_string()))).collect()).unwrap_or_default())
}

// ── LØYPA ───────────────────────────────────────────────────────────────────────────────────────

/// Det én opplasting trenger underveis.
struct Lop<'a, K> {
    app: &'a K,
    k: &'a Klient,
    fil: &'a Fil,
    valg: &'a Valg,
    avbryt: &'a Stopp,
    /// Id-en framdriften meldes under: stien, som før.
    id: String,
    bytes: u64,
    /// Filas mtime (ms) — i nøkkelen.
    mtime: u64,
    /// `sistEndret` til serveren.
    sist: u64,
    struper: Arc<Struper>,
    tsti: PathBuf,
}

/// Last opp `fil` til skapet og gi fila tilbake.
///
/// Store filer (over `valg.del_grense`) går i deler med gjenopptak; små i én PUT. Feilene er typet (`Feil`), og teksten
/// er den samme som før T2.
pub async fn last_opp<K: Melder + Etterarbeid>(app: &K, k: &Klient, fil: &Fil, valg: &Valg, avbryt: &Stopp) -> Result<Opplastet, Feil> {
    let id = fil.sti.to_string_lossy().to_string();
    let meta = tokio::fs::metadata(&fil.sti).await;
    let (bytes, mtime) = match (&fil.kilde, meta) {
        // En kilde trenger ingen fil på `sti` — da er mtime ukjent (0) med mindre fila finnes.
        (Some(kl), m) => (kl.lengde(), m.ok().map(|m| mtime_ms(&m)).unwrap_or(0)),
        (None, Ok(m)) => (m.len(), mtime_ms(&m)),
        (None, Err(e)) => return Err(disk(e)),
    };
    let (r, tsti) = les_tilstand(&valg.tilstand_dir, &id, &fil.navn, bytes, mtime, &fil.mappe_id);
    let lop = Lop {
        app, k, fil, valg, avbryt, id, bytes, mtime,
        sist: fil.sist_endret_ms.unwrap_or(mtime),
        struper: valg.struper.clone().unwrap_or_else(|| Struper::ny(Arc::new(AtomicU64::new(0)))),
        tsti,
    };
    let (r, xxh) = if r.satt_sammen && !r.original_key.is_empty() {
        // Bytene er alt oppe (et tidligere forsøk døde eller fikk nei før fullfør) — bare fullfør gjenstår.
        log::info!("{}: bytene ligger alt i lageret — fullfører", fil.navn);
        meld(app, &lop.id, 0, bytes, "kobler");
        let x = if r.xxh64.is_empty() { None } else { Some(r.xxh64.clone()) };
        (r, x)
    } else if bytes > valg.del_grense {
        last_opp_multipart(&lop, r).await?
    } else {
        last_opp_enkel(&lop).await?
    };
    // ⚠ F8: tilbudets adresse lever 7200 s. Ble den gitt for lenge siden (gjenopptak dagen etter, eller 100 GB på
    // 93 Mbit/s ≈ 2,4 t), ville PUT-en feilet, og fila fått plakat, men aldri noen avspillingskopi: med plakaten i fullfør
    // starter ikke serveren Stream heller. Da sendes ingenting til den gamle adressen; kopien lenkes etter fullfør.
    let gammel = !r.proxy_put.is_empty() && !proxy_fersk(r.proxy_tid, naa_ms());
    let proxy = if gammel {
        let alder = if r.proxy_tid == 0 { "ukjent alder".to_string() } else { format!("{} min", naa_ms().saturating_sub(r.proxy_tid) / 60_000) };
        log::info!("{}: proxy-adressen fra tilbudet er for gammel ({alder}) — avspillingskopien lenkes med en ny adresse etter fullfør", fil.navn);
        None
    } else {
        app.avspillingskopi(k.opp(), k.portal(), &lop.id, &fil.navn, &r.proxy_put, bytes).await
    };
    let (ut, mut status) = fullfor_opplasting(&lop, &r, xxh, proxy).await?;
    if gammel {
        if ut.allerede {
            log::info!("{}: fila fantes i skapet fra før — ingen avspillingskopi å lenke", fil.navn);
        } else if ut.id.is_empty() {
            log::warn!("{}: fullfør ga ingen fil-id — avspillingskopien kan lenkes for hånd", fil.navn);
        } else {
            // Best effort: opplastingen ER ferdig. En feil her logges, og kopien kan lenkes for hånd («Lenk lokal proxy …»).
            match lenk_avspillingskopi(&lop, &ut.id).await {
                Ok(n) => {
                    log::info!("{}: avspillingskopien er lenket ({n} B)", fil.navn);
                    // Det finnes en kopi likevel (en lokal proxy til en fil som ikke kan leses): som når adressen er fersk.
                    if status == "utenProxy" { status = "ferdig"; }
                }
                Err(e) => {
                    log::warn!("{}: fikk ikke lenket avspillingskopien ({e}) — fila er i skapet, og kopien kan lenkes for hånd", fil.navn);
                    // Vises som «uten avspillingskopi» (høyreklikk for å lenke en proxy), ikke som en ferdig fil med kopi.
                    status = "utenProxy";
                }
            }
        }
    }
    // Sluttstatusen først NÅ, så en fil som er meldt ferdig, ikke går tilbake til «proxy» mens kopien lenkes.
    let _ = app.framdrift(Framdrift { id: lop.id.clone(), hentet: bytes, total: bytes, status: status.into(), feil: None });
    Ok(ut)
}

/// F8: avspillingskopien til en fil som ALT er i skapet, med en fersk adresse — samme to kall som «Lenk lokal proxy …»
/// i Transfer. Nøkkelen er serverens egen (`proxyNokkel` av raden), så bare id-en og størrelsen sendes. Gir størrelsen.
async fn lenk_avspillingskopi<K: Melder + Etterarbeid>(lop: &Lop<'_, K>, id: &str) -> Result<u64, String> {
    let (app, k, fil, bytes) = (lop.app, lop.k, lop.fil, lop.bytes);
    // ⚠ Adressen er skrive-legitimasjon: den skal aldri ut i en feiltekst eller loggen (nettfeil stripper URL-er).
    let (st, d) = portal_post(k, &serde_json::json!({ "action": "proxy-lenk-start", "id": id }), Some(lop.avbryt)).await.map_err(|e| e.to_string())?;
    let put = match d["proxyPutUrl"].as_str() {
        Some(u) if (200..300).contains(&st) && !u.is_empty() => u.to_string(),
        _ => return Err(format!("proxy-lenk-start: {}", d["error"].as_str().map(String::from).unwrap_or_else(|| format!("Portalen svarte {st}")))),
    };
    let n = app.avspillingskopi(k.opp(), k.portal(), &lop.id, &fil.navn, &put, bytes).await.ok_or("ble ingen avspillingskopi")?;
    let (st, d) = portal_post(k, &serde_json::json!({ "action": "proxy-lenk-fullfor", "id": id, "proxyStorrelse": n }), Some(lop.avbryt)).await.map_err(|e| e.to_string())?;
    if !(200..300).contains(&st) || !d["ok"].as_bool().unwrap_or(false) {
        return Err(format!("proxy-lenk-fullfor: {}", d["error"].as_str().map(String::from).unwrap_or_else(|| format!("Portalen svarte {st}"))));
    }
    Ok(n)
}

/// Svaret fra presign/multipart-start når `versjonAv` er avvist FØR bytene går: ingenting er laget hos serveren.
fn forsjekk_avslag(lop: &Lop<'_, impl Melder>, kode: &str, d: &serde_json::Value) -> Feil {
    let _ = std::fs::remove_file(&lop.tsti);
    Feil::VersjonAv { kode: kode.to_string(), ryddet: true, id: None, tekst: d["error"].as_str().unwrap_or("Kunne ikke lage en ny versjon av fila").to_string() }
}

/// Feltene presign og multipart-start har til felles utover det de alltid sendte: tags og versjonAv, bare når de er satt.
fn legg_til_start(body: &mut serde_json::Value, fil: &Fil) {
    if !fil.tags.is_empty() { body["tags"] = serde_json::json!(fil.tags); }
    if let Some(v) = fil.versjon_av.as_deref().filter(|v| !v.is_empty()) { body["versjonAv"] = serde_json::json!(v); }
}

/// Starter en fersk multipart og gir resume-tilstanden tilbake.
/// Egen funksjon fordi den kalles to steder: ved ny opplasting, og når en
/// gjenopptatt opplasting viser seg å være død på serveren (30/8).
async fn multipart_start<K: Melder>(lop: &Lop<'_, K>) -> Result<Resume, Feil> {
    let fil = lop.fil;
    // Delstørrelsen settes én gang, her, og følger opplastingen til den er ferdig — både i vår egen
    // resume-fil OG hos serveren (`delBytes` → opplastinger_pending.DelBytes). Uten den kan ikke
    // nettleseren gjenoppta en opplasting Transfer har begynt på, fordi den ikke vet hvor delegrensene går.
    let del = delstorrelse(lop.bytes);
    let mut body = serde_json::json!({ "action": "multipart-start", "filnavn": fil.navn, "mimeType": fil.mime, "filstorrelse": lop.bytes, "sistEndret": lop.sist, "mappeId": mappe_json(&fil.mappe_id), "delBytes": del });
    legg_til_start(&mut body, fil);
    let (st, d) = portal_post(lop.k, &body, Some(lop.avbryt)).await?;
    if st == 401 || st == 403 { return Err(Feil::IkkeTilgang("Ikke tilgang — logg inn på nytt".into())); }
    if let Some(kode) = d["versjonAvFeil"].as_str() { return Err(forsjekk_avslag(lop, kode, &d)); }
    let (uid, key) = match (d["uploadId"].as_str(), d["originalKey"].as_str()) {
        (Some(u), Some(kk)) => (u.to_string(), kk.to_string()),
        _ => {
            let tekst = d["error"].as_str().unwrap_or("multipart-start feilet").to_string();
            return Err(if d["kvoteSperre"].as_bool().unwrap_or(false) { Feil::KvoteFull(tekst) } else { Feil::server(st, tekst) });
        }
    };
    // Serveren tilbyr en presignert URL for avspillingsproxyen (kun video), og nøkkelen den skal ha (F7).
    // Begge lagres i resume-fila så en gjenopptatt opplasting ikke mister dem — og når adressen kom (F8): den lever 7200 s.
    let proxy_put = d["proxyPutUrl"].as_str().unwrap_or("").to_string();
    let proxy_tid = if proxy_put.is_empty() { 0 } else { naa_ms() };
    Ok(Resume {
        versjon: TILSTAND_VERSJON, sti: lop.id.clone(), navn: fil.navn.clone(), upload_id: uid, original_key: key,
        mappe_id: fil.mappe_id.clone(), bytes: lop.bytes, sist_endret: lop.mtime, deler: vec![],
        proxy_put, del_bytes: del,
        proxy_key: d["proxyKey"].as_str().unwrap_or("").to_string(), proxy_tid, satt_sammen: false, xxh64: String::new(),
    })
}

/// Leser bytene til delene: fila (seek + read, som før) eller kilden (i en blokkerende tråd).
enum Leser { Fil(tokio::fs::File), Kilde(Arc<dyn Kilde>) }
impl Leser {
    async fn ny(fil: &Fil) -> Result<Leser, Feil> {
        Ok(match &fil.kilde { Some(k) => Leser::Kilde(k.clone()), None => Leser::Fil(tokio::fs::File::open(&fil.sti).await.map_err(disk)?) })
    }
    async fn les(&mut self, start: u64, len: u64) -> Result<Vec<u8>, Feil> {
        use tokio::io::{AsyncReadExt, AsyncSeekExt};
        match self {
            Leser::Fil(f) => {
                f.seek(std::io::SeekFrom::Start(start)).await.map_err(disk)?;
                let mut buf = vec![0u8; len as usize];
                f.read_exact(&mut buf).await.map_err(disk)?;
                Ok(buf)
            }
            Leser::Kilde(k) => {
                let k = k.clone();
                tokio::task::spawn_blocking(move || { let mut buf = vec![0u8; len as usize]; k.les(start, &mut buf).map(|_| buf) })
                    .await.map_err(|e| Feil::Disk(format!("{e}")))?.map_err(disk)
            }
        }
    }
}

/// Framdriften for én fil med flere deler i lufta: ferdige deler + det som er sendt av hver del som er underveis.
///
/// ⚠ Hvert delforsøk har sin EGEN teller i `i_lufta`. Ryker forsøket, fjernes telleren — en strøm som lever litt for
/// lenge, teller da på en teller ingen leser, og summen kryper aldri oppover av seg selv.
struct Framdriften { ferdig: AtomicU64, i_lufta: std::sync::Mutex<HashMap<u32, Arc<AtomicU64>>> }
impl Framdriften {
    fn total(&self) -> u64 {
        self.ferdig.load(Ordering::Relaxed) + self.i_lufta.lock().map(|m| m.values().map(|v| v.load(Ordering::Relaxed)).sum::<u64>()).unwrap_or(0)
    }
}

/// Det leseren og senderne deler i ett gjennomløp.
struct Felles {
    upload_id: String,
    original_key: String,
    tilstand: std::sync::Mutex<Resume>,
    urler: std::sync::Mutex<HashMap<u32, String>>,
    fram: Arc<Framdriften>,
    feil: std::sync::Mutex<Option<Feil>>,
    omstart: AtomicBool,
}
impl Felles {
    fn stopp(&self) -> bool { self.omstart.load(Ordering::Relaxed) || self.feil.lock().map(|f| f.is_some()).unwrap_or(true) }
    /// Den FØRSTE feilen vinner — det er den som forklarer hva som skjedde.
    fn sett_feil(&self, e: Feil) { if let Ok(mut f) = self.feil.lock() { if f.is_none() { *f = Some(e); } } }
}

enum Gjennomlop { Ferdig(Option<String>), Omstart }

async fn last_opp_multipart<K: Melder>(lop: &Lop<'_, K>, inn: Resume) -> Result<(Resume, Option<String>), Feil> {
    let (app, k, avbryt, bytes, navn) = (lop.app, lop.k, lop.avbryt, lop.bytes, lop.fil.navn.as_str());
    let mut r = inn;
    // Rundturen til portalen (multipart-start, og siden signering av deler) tar
    // et lite oyeblikk der ingen bytes gaar. Si det, i stedet for a staa pa 0.
    meld(app, &lop.id, 0, bytes, "kobler");
    // ⚠ LAGERET ER FASIT VED GJENOPPTAK (0.3.0). Før stolte vi blindt på vår egen bokføring: ble en del
    // sendt uten at vi rakk å skrive den ned (appen døde i sekundet mellom), visste vi ikke om den, og
    // en ødelagt resume-fil betød hele fila på nytt. Nå spør vi lageret hvilke deler som faktisk ligger
    // der — samme spørsmål nettleseren stiller — og tar bare med deler med nøyaktig riktig lengde.
    if !r.upload_id.is_empty() {
        match multipart_status(k, &r, avbryt).await {
            MpStatus::Deler { deler, del_bytes } => {
                if r.del_bytes == 0 && del_bytes > 0 { r.del_bytes = del_bytes; }
                let del = if r.del_bytes > 0 { r.del_bytes } else { DEL_BYTES_GAMMEL };
                let lokalt = r.deler.len();
                r.deler = gyldige_deler(&deler, del, bytes);
                log::info!("gjenopptar {navn}: lageret har {} ferdige deler (lokal bokføring: {lokalt})", r.deler.len());
                resume_skriv(&lop.tsti, &r);
            }
            MpStatus::Ferdig => {
                // Satt sammen alt — appen døde mellom sammensetting og fullfør. Bare fullfør gjenstår.
                log::info!("gjenopptar {navn}: fila er allerede satt sammen hos lageret");
                r.satt_sammen = true;
                resume_skriv(&lop.tsti, &r);
                let x = if r.xxh64.is_empty() { None } else { Some(r.xxh64.clone()) };
                return Ok((r, x));
            }
            MpStatus::Borte => {
                log::warn!("{navn}: opplastingen finnes ikke lenger hos lageret — begynner på nytt");
                r = Resume::default();
            }
            MpStatus::Ukjent(e) => log::warn!("{navn}: fikk ikke lest status hos lageret ({e}) — bruker lokal bokføring"),
        }
    }
    if r.upload_id.is_empty() {
        r = multipart_start(lop).await?;
        resume_skriv(&lop.tsti, &r);
    }
    let mut leser = Leser::ny(lop.fil).await?;
    // ⚠ DØD OPPLASTING (30/8): kjenner ikke serveren opplastingen igjen, er
    // resume-tilstanden verdiløs — og før dette var fila da PERMANENT brekt:
    // hvert nytt forsøk leste den samme døde nøkkelen fra resume-fila og ga
    // opp med en gang. Nå starter vi friskt i stedet. Én gang, så en server
    // som avviser alt ikke blir en evig løkke.
    let mut omstart_brukt = false;
    let xxh = loop {
        match send_deler(lop, &mut r, &mut leser, !omstart_brukt).await? {
            Gjennomlop::Ferdig(x) => break x,
            Gjennomlop::Omstart => {
                omstart_brukt = true;
                log::warn!("{navn}: portalen kjente ikke opplastingen igjen — begynner på nytt");
                meld(app, &lop.id, 0, bytes, "kobler");
                r = multipart_start(lop).await?;
                resume_skriv(&lop.tsti, &r);
                // Ny opplasting = alle deler på nytt, så vi ser hele fila igjen — og en fersk hasher (et nytt
                // gjennomløp lager sin egen; den gamle kan ha sett deler fra bolkene før).
            }
        }
    };
    // Sett sammen. R2 bruker maalbar tid pa a lime sammen 100+ deler — det er
    // her «star lenge pa 100 %» begynner.
    meld(app, &lop.id, bytes, bytes, "setter");
    // Stigende rekkefølge: lageret krever det, og etter et gjenopptak kan bokføringen ha dem hulter til bulter.
    r.deler.sort_by_key(|(n, _)| *n);
    let deler: Vec<serde_json::Value> = r.deler.iter().map(|(n, e)| serde_json::json!({ "nr": n, "etag": e })).collect();
    let (st, d) = portal_post(k, &serde_json::json!({ "action": "multipart-fullfor", "originalKey": r.original_key, "uploadId": r.upload_id, "deler": deler }), Some(avbryt)).await?;
    if !d["ok"].as_bool().unwrap_or(false) {
        let tekst = d["error"].as_str().unwrap_or("Kunne ikke sette sammen fila").to_string();
        // 410 = byte-vakta hos serveren slo til (fila ble ikke hel) og har slettet den. Resume-tilstanden
        // er da verdiløs, og neste forsøk skal begynne på nytt.
        if st == 410 { let _ = std::fs::remove_file(&lop.tsti); return Err(Feil::Borte(tekst)); }
        return Err(Feil::server(st, tekst));
    }
    // Bytene ligger hele i lageret. Tilstanden står til fullfør har svart (se `Resume`), med sjekksummen.
    r.satt_sammen = true;
    r.xxh64 = xxh.clone().unwrap_or_default();
    resume_skriv(&lop.tsti, &r);
    Ok((r, xxh))
}

/// Ett gjennomløp over delene som mangler: ÉN leser leser dem i rekkefølge og hasher dem (så xxh64 blir riktig og
/// lesingen sekvensiell), og `valg.deler_samtidig` sendere PUT-er dem. Med én sender leses neste del først når forrige
/// er oppe — samme rekkefølge som før T2. Med flere ligger høyst `I_LUFTA` i minnet.
///
/// Hver ferdig del bokføres i tilstandsfila med en gang, så et gjenopptak (eller lageret, som er fasiten) vet om den.
async fn send_deler<K: Melder>(lop: &Lop<'_, K>, r: &mut Resume, leser: &mut Leser, omstart_lov: bool) -> Result<Gjennomlop, Feil> {
    let (app, k, avbryt, bytes) = (lop.app, lop.k, lop.avbryt, lop.bytes);
    // ⚠ REGN MED OPPLASTINGENS EGEN DELSTØRRELSE, ikke konstanten. En resume-fil fra en tidligere
    // versjon ble skrevet med 64 MB; leser vi den med dagens tall, havner delene på feil
    // forskyvning og fila blir korrupt uten at noe sier fra. 0 = fil fra før feltet fantes.
    // (Regnes for hvert gjennomløp: en omstart kan gi en ny opplasting med en annen delstørrelse.)
    let del = if r.del_bytes > 0 { r.del_bytes } else { DEL_BYTES_GAMMEL };
    let antall = bytes.div_ceil(del) as u32;
    let ferdige: HashSet<u32> = r.deler.iter().map(|(n, _)| *n).collect();
    // Finnes det alt ferdige deler, har vi IKKE lest hele fila i denne kjøringen — da er en
    // helfils-hash umulig, og vi lar være å påstå en.
    let hel = ferdige.is_empty();
    let hentet = ferdige.iter().map(|n| del_omrade(*n, del, bytes).1).sum::<u64>().min(bytes);
    meld(app, &lop.id, hentet, bytes, "laster");
    let mangler: Vec<u32> = (1..=antall).filter(|n| !ferdige.contains(n)).collect();

    let sendere = lop.valg.deler_samtidig.clamp(1, 16);
    let plasser = if sendere == 1 { 1 } else { ((I_LUFTA / del.max(1)) as usize).max(sendere) };
    let sem = Arc::new(tokio::sync::Semaphore::new(plasser));
    let (tx, rx) = tokio::sync::mpsc::channel::<(u32, Arc<Vec<u8>>, tokio::sync::OwnedSemaphorePermit)>(plasser);
    let rx = tokio::sync::Mutex::new(rx);
    let felles = Felles {
        upload_id: r.upload_id.clone(),
        original_key: r.original_key.clone(),
        tilstand: std::sync::Mutex::new(std::mem::take(r)),
        urler: std::sync::Mutex::new(HashMap::new()),
        fram: Arc::new(Framdriften { ferdig: AtomicU64::new(hentet), i_lufta: std::sync::Mutex::new(HashMap::new()) }),
        feil: std::sync::Mutex::new(None),
        omstart: AtomicBool::new(false),
    };

    // LESEREN. Signerer delene i bolker på 20 (URL-ene lever 1 t) i det den kommer til dem, leser og hasher i rekkefølge.
    let felles_ref = &felles;
    let lesing = async move {
        let mut hasher = if hel { Some(xxhash_rust::xxh64::Xxh64::new(0)) } else { None };
        'bolker: for bolk in mangler.chunks(20) {
            if felles_ref.stopp() { break; }
            if felles_ref.fram.total() == 0 { meld(app, &lop.id, 0, bytes, "kobler"); }
            match signer_deler(k, &felles_ref.upload_id, &felles_ref.original_key, bolk, avbryt).await {
                Err(e) => { felles_ref.sett_feil(e); break; }
                Ok(u) if u.is_empty() => {
                    if omstart_lov { felles_ref.omstart.store(true, Ordering::Relaxed); }
                    else { felles_ref.sett_feil(Feil::server(0, "Kunne ikke signere deler — opplastingen kan være utløpt; prøv igjen")); }
                    break;
                }
                Ok(u) => { if let Ok(mut m) = felles_ref.urler.lock() { m.extend(u); } }
            }
            for nr in bolk {
                let Ok(plass) = sem.clone().acquire_owned().await else { break 'bolker };
                if felles_ref.stopp() { break 'bolker; }
                if avbryt.av() { felles_ref.sett_feil(Feil::Avbrutt); break 'bolker; }
                let (start, len) = del_omrade(*nr, del, bytes);
                let buf = match leser.les(start, len).await { Ok(b) => b, Err(e) => { felles_ref.sett_feil(e); break 'bolker; } };
                // Hash HER, ikke i forsøks-løkka: delen leses én gang, men kan sendes flere. Delene leses i
                // stigende rekkefølge (1..n), så dette gir samme sum som ett gjennomløp av hele fila.
                if let Some(h) = hasher.as_mut() { h.update(&buf); }
                if tx.send((*nr, Arc::new(buf), plass)).await.is_err() { break 'bolker; }
            }
        }
        drop(tx);
        hasher.map(|h| format!("{:016x}", h.digest()))
    };

    // SENDERNE. Tar neste leste del og PUT-er den med omforsøk. Er gjennomløpet stoppet, tømmes kanalen uten å sende,
    // så leseren aldri blir stående og vente på en plass.
    let rx_ref = &rx;
    let sender = move || async move {
        loop {
            let neste = { rx_ref.lock().await.recv().await };
            let Some((nr, buf, plass)) = neste else { break };
            if felles_ref.stopp() { drop(plass); continue; }
            match send_del(lop, felles_ref, nr, buf, antall).await {
                Ok(etag) => {
                    if let Ok(mut t) = felles_ref.tilstand.lock() { t.deler.push((nr, etag)); resume_skriv(&lop.tsti, &t); }
                    meld(app, &lop.id, felles_ref.fram.total().min(bytes), bytes, "laster");
                }
                Err(e) => felles_ref.sett_feil(e),
            }
            drop(plass);
        }
    };
    let (hash, _) = tokio::join!(lesing, futures_util::future::join_all((0..sendere).map(|_| sender())));

    *r = felles.tilstand.into_inner().unwrap_or_default();
    if let Some(e) = felles.feil.into_inner().unwrap_or(None) {
        return Err(if avbryt.av() { Feil::Avbrutt } else { e });
    }
    if avbryt.av() { return Err(Feil::Avbrutt); }
    if felles.omstart.load(Ordering::Relaxed) { return Ok(Gjennomlop::Omstart); }
    Ok(Gjennomlop::Ferdig(hash))
}

/// Én del, med omforsøk: ny signatur ved 403, stoppvakt, venting på nettet og økende pauser.
async fn send_del<K: Melder>(lop: &Lop<'_, K>, felles: &Felles, nr: u32, buf: Arc<Vec<u8>>, antall: u32) -> Result<String, Feil> {
    let (app, k, avbryt, bytes, navn) = (lop.app, lop.k, lop.avbryt, lop.bytes, lop.fil.navn.as_str());
    let len = buf.len() as u64;
    let mut siste_feil = String::new();
    let mut ny_signatur_brukt = false;
    let mut n = 0u32;
    while n < DEL_FORSOK {
        // Utløpt signatur (403) ga før opp hele fila. Nå hentes en ny for akkurat denne delen.
        let lagret = felles.urler.lock().ok().and_then(|m| m.get(&nr).cloned());
        let url = match lagret {
            Some(u) => u,
            None => match signer_deler(k, &felles.upload_id, &felles.original_key, &[nr], avbryt).await?.remove(&nr) {
                Some(u) => { if let Ok(mut m) = felles.urler.lock() { m.insert(nr, u.clone()); } u }
                None => { siste_feil = "fikk ikke ny signatur for delen".into(); break; }
            },
        };
        // ⚠ FRAMDRIFT UNDERVEIS (28/8, Vegard: «går fra 3.8 MB/s til 22 MB/s
        // hele tiden»): før meldte vi først NÅR en 64 MB-del var ferdig. Ved
        // ~10 MB/s betyr det at telleren står bom stille i ~6 sekunder og så
        // hopper 64 MB — og en fartsmåler som sampler hvert sekund ser
        // vekselvis 0 og 64 MB/s. Nå strømmes delen i 256 kB-biter med
        // teller, akkurat som én-PUT-løypa, så tallet blir ekte.
        let sendt = Arc::new(AtomicU64::new(0));
        if let Ok(mut m) = felles.fram.i_lufta.lock() { m.insert(nr, sendt.clone()); }
        let (app2, id2, fram2, sendt2) = (app.clone(), lop.id.clone(), felles.fram.clone(), sendt.clone());
        let mut sist_meldt = std::time::Instant::now();
        // Ved omforsøk starter delen på nytt fra null — ærlig, for bytene MÅ sendes om igjen.
        // ⚠ STRUPING (31/8): bokføringen lå FØR PUT-en og meldte hele
        // 64 MB-delen på én gang, mens Struper sover maks 2 s per kall.
        // Taket ble dermed 64 MB / 2 s ≈ 32 MB/s uansett hva brukeren
        // satte — altså ingen struping i praksis på store filer, som er
        // nettopp de man vil dempe. Nå meldes hver 256 kB-bit, der
        // søvnen er brøkdelen av et sekund og strupingen faktisk biter.
        let strom = futures_util::stream::unfold((0usize, buf.clone(), lop.struper.clone()), |(pos, b, s)| async move {
            if pos >= b.len() { return None; }
            let slutt = (pos + (1 << 18)).min(b.len());
            s.tell((slutt - pos) as u64).await;
            let bit = bytes::Bytes::copy_from_slice(&b[pos..slutt]);
            Some((Ok::<bytes::Bytes, std::io::Error>(bit), (slutt, b, s)))
        }).inspect(move |r: &Result<bytes::Bytes, std::io::Error>| {
            if let Ok(bit) = r {
                sendt2.fetch_add(bit.len() as u64, Ordering::Relaxed);
                if sist_meldt.elapsed().as_millis() > 150 {
                    sist_meldt = std::time::Instant::now();
                    meld(&app2, &id2, fram2.total().min(bytes), bytes, "laster");
                }
            }
        });
        let svar = med_stoppvakt(&sendt, Some(avbryt), k.opp().put(&url).header(reqwest::header::CONTENT_LENGTH, len).body(reqwest::Body::wrap_stream(strom)).send()).await;
        // Forsøket er over: telleren ut av summen. Lyktes det, teller delen som ferdig i stedet.
        if let Ok(mut m) = felles.fram.i_lufta.lock() { m.remove(&nr); }
        if avbryt.av() { return Err(Feil::Avbrutt); }
        match svar {
            Some(Ok(resp)) if resp.status().is_success() => {
                let etag = resp.headers().get("etag").and_then(|v| v.to_str().ok()).map(|s| s.trim_matches('"').to_string());
                match etag {
                    Some(e) => { felles.fram.ferdig.fetch_add(len, Ordering::Relaxed); return Ok(e); }
                    None => break,
                }
            }
            Some(Ok(resp)) if resp.status().as_u16() == 403 && !ny_signatur_brukt => {
                // Signaturen har gått ut (URL-ene lever en time — en lang pause gjør det). Ny
                // signatur med én gang: det er ikke nettet som feiler, så ingen pause.
                ny_signatur_brukt = true;
                if let Ok(mut m) = felles.urler.lock() { m.remove(&nr); }
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
        let h = felles.fram.total().min(bytes);
        let (a, id) = (app.clone(), lop.id.clone());
        vent_paa_nett(k.portal(), Some(avbryt), &move || meld(&a, &id, h, bytes, "venterNett")).await;
        meld(app, &lop.id, felles.fram.total().min(bytes), bytes, "nyttForsok");
        sov(pause_foer_forsok(n - 1), Some(avbryt)).await;
        if avbryt.av() { return Err(Feil::Avbrutt); }
    }
    Err(Feil::Nett(format!("Del {nr} av {antall} feilet ({siste_feil}) — prøv igjen, den fortsetter fra del {nr}")))
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

/// Som `fil_strom`, for en `Kilde`: 1 MiB om gangen, lest i en blokkerende tråd.
fn kilde_strom(kilde: Arc<dyn Kilde>, struper: Arc<Struper>, avbryt: Stopp, hash: Arc<std::sync::Mutex<xxhash_rust::xxh64::Xxh64>>, mut tell: impl FnMut(u64) + Send + 'static) -> impl futures_util::Stream<Item = Result<bytes::Bytes, std::io::Error>> + Send + 'static {
    let lengde = kilde.lengde();
    futures_util::stream::unfold((0u64, kilde, struper, avbryt, hash), move |(pos, kilde, struper, avbryt, hash)| async move {
        if pos >= lengde { return None; }
        if avbryt.av() { return Some((Err(std::io::Error::other("Avbrutt")), (lengde, kilde, struper, avbryt, hash))); }
        let n = (lengde - pos).min(1 << 20);
        let k2 = kilde.clone();
        let lest = tokio::task::spawn_blocking(move || { let mut buf = vec![0u8; n as usize]; k2.les(pos, &mut buf).map(|_| buf) }).await;
        match lest {
            Ok(Ok(buf)) => {
                if let Ok(mut h) = hash.lock() { h.update(&buf); }
                struper.tell(n).await;
                Some((Ok(bytes::Bytes::from(buf)), (pos + n, kilde, struper, avbryt, hash)))
            }
            Ok(Err(e)) => Some((Err(e), (lengde, kilde, struper, avbryt, hash))),
            Err(e) => Some((Err(std::io::Error::other(format!("{e}"))), (lengde, kilde, struper, avbryt, hash))),
        }
    }).inspect(move |r| { if let Ok(b) = r { tell(b.len() as u64); } })
}

async fn last_opp_enkel<K: Melder>(lop: &Lop<'_, K>) -> Result<(Resume, Option<String>), Feil> {
    let (app, k, avbryt, bytes, fil) = (lop.app, lop.k, lop.avbryt, lop.bytes, lop.fil);
    let (navn, mime) = (fil.navn.as_str(), fil.mime.as_str());
    // 1) presign
    meld(app, &lop.id, 0, bytes, "kobler");
    let mut body = serde_json::json!({ "action": "presign", "filnavn": navn, "mimeType": mime, "filstorrelse": bytes, "sistEndret": lop.sist, "mappeId": mappe_json(&fil.mappe_id) });
    legg_til_start(&mut body, fil);
    let (st, d) = portal_post(k, &body, Some(avbryt)).await?;
    if st == 401 || st == 403 { return Err(Feil::IkkeTilgang("Ikke tilgang — logg inn på nytt (API-nøkler kan ikke laste opp)".into())); }
    if let Some(kode) = d["versjonAvFeil"].as_str() { return Err(forsjekk_avslag(lop, kode, &d)); }
    if d["kvoteSperre"].as_bool().unwrap_or(false) { return Err(Feil::KvoteFull(d["error"].as_str().unwrap_or("Lagringen er full").to_string())); }
    let (url, key) = match (d["uploadUrl"].as_str(), d["originalKey"].as_str()) { (Some(u), Some(k)) => (u.to_string(), k.to_string()), _ => return Err(Feil::server(st, d["error"].as_str().unwrap_or("presign feilet"))) };
    let proxy_put = d["proxyPutUrl"].as_str().unwrap_or("").to_string();
    let proxy_key = d["proxyKey"].as_str().unwrap_or("").to_string();
    // F8: da tilbudet kom — ikke da PUT-en var ferdig. Adressen lever 7200 s fra signeringen.
    let proxy_tid = if proxy_put.is_empty() { 0 } else { naa_ms() };
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
        let app2 = app.clone(); let id = lop.id.clone();
        let sendt = Arc::new(AtomicU64::new(0)); let sendt2 = sendt.clone();
        let mut sist_meldt = std::time::Instant::now();
        let tell = move |b: u64| {
            let t = sendt2.fetch_add(b, Ordering::Relaxed) + b;
            if sist_meldt.elapsed().as_millis() > 150 || t == bytes { sist_meldt = std::time::Instant::now(); meld(&app2, &id, t, bytes, "laster"); }
        };
        let kropp = match &fil.kilde {
            Some(kl) => reqwest::Body::wrap_stream(kilde_strom(kl.clone(), lop.struper.clone(), avbryt.clone(), hasher.clone(), tell)),
            None => {
                let f = tokio::fs::File::open(&fil.sti).await.map_err(disk)?;
                reqwest::Body::wrap_stream(fil_strom(f, lop.struper.clone(), avbryt.clone(), Some(hasher.clone()), tell))
            }
        };
        let svar = med_stoppvakt(&sendt, Some(avbryt), k.opp().put(&url).header(reqwest::header::CONTENT_TYPE, mime).header(reqwest::header::CONTENT_LENGTH, bytes).body(kropp).send()).await;
        if avbryt.av() {
            crate::forkast_nokler(k, &[key.clone()]).await;
            return Err(Feil::Avbrutt);
        }
        let (feil, gi_opp) = match svar {
            Some(Ok(r)) if r.status().is_success() => break hasher.lock().ok().map(|h| format!("{:016x}", h.digest())),
            // 403 = URL-en har gått ut: gi opp HER, så filforsøket henter en ny signering.
            Some(Ok(r)) => { let s = r.status().as_u16(); (format!("Lageret svarte {s}"), !forbigaaende(s)) }
            Some(Err(e)) => (nettfeil(e), false),
            None => ("forbindelsen sto stille — ingenting beveget seg på ett minutt".to_string(), false),
        };
        if gi_opp || n + 1 >= ENKEL_FORSOK {
            crate::forkast_nokler(k, &[key.clone()]).await;
            return Err(Feil::Nett(feil));
        }
        log::warn!("opp {navn} forsøk {} feilet: {feil}", n + 1);
        let (a, id) = (app.clone(), lop.id.clone());
        vent_paa_nett(k.portal(), Some(avbryt), &move || meld(&a, &id, 0, bytes, "venterNett")).await;
        meld(app, &lop.id, 0, bytes, "nyttForsok");
        sov(pause_foer_forsok(n), Some(avbryt)).await;
        n += 1;
    };
    // Bytene er oppe. Tilstanden skrives nå (T2), så et nei på fullfør som lar alt stå («midlertidig»), eller et
    // nettbrudd der, fullføres med SAMME nøkkel neste gang — i stedet for en ny opplasting ved siden av den første.
    let r = Resume {
        versjon: TILSTAND_VERSJON, sti: lop.id.clone(), navn: navn.to_string(), upload_id: String::new(), original_key: key,
        mappe_id: fil.mappe_id.clone(), bytes, sist_endret: lop.mtime, deler: vec![], proxy_put, del_bytes: 0, proxy_key,
        proxy_tid, satt_sammen: true, xxh64: xxh.clone().unwrap_or_default(),
    };
    resume_skriv(&lop.tsti, &r);
    Ok((r, xxh))
}

/// Proxy-nøkkelen slik Transfer bygde den før T2 — bare for en opplasting der serveren ikke oppga sin (en tilstand fra
/// før T2, eller en eldre server).
///
/// ⚠ MÅ være bit for bit lik serverens `proxyNokkel` — den avviser en nøkkel den ikke selv ville laget. Reservefallet
/// bruker det TRIMMEDE navnet; `unwrap_or(key)` ville sneket «originals/» inn igjen.
fn proxy_nokkel_reserve(key: &str) -> String {
    let basis = key.trim_start_matches("originals/");
    let basis = basis.rsplit_once('.').map(|(a, _)| a).unwrap_or(basis);
    format!("proxy/{basis}.mp4")
}

/// Fullfør-steget (server: thumb/EXIF/dedup for bilder; video: poster/sprite fra appens etterarbeid).
///
/// Gir fila og sluttstatusen («ferdig», «utenProxy», «hoppet»). Den meldes av `last_opp` (F8: etter en ev. lenking av
/// avspillingskopien; ellers rett etter, som før).
async fn fullfor_opplasting<K: Melder + Etterarbeid>(lop: &Lop<'_, K>, r: &Resume, xxh: Option<String>, proxy: Option<u64>) -> Result<(Opplastet, &'static str), Feil> {
    let (app, k, fil, bytes) = (lop.app, lop.k, lop.fil, lop.bytes);
    let (navn, key) = (fil.navn.as_str(), r.original_key.as_str());
    let mut body = serde_json::json!({ "action": "fullfor", "originalKey": key, "filnavn": navn, "mimeType": fil.mime, "filstorrelse": bytes, "sistEndret": lop.sist, "mappeId": mappe_json(&fil.mappe_id) });
    // Sjekksummen: den regnet over bytene som ble sendt, ellers den kjente (F3: også når opplastingen er gjenopptatt).
    let kjent = fil.kjent_xxh64.as_deref().filter(|x| !x.is_empty()).map(|x| x.to_ascii_lowercase());
    if let (Some(x), Some(kj)) = (&xxh, &kjent) { if x != kj { log::warn!("{navn}: sjekksummen av bytene som ble sendt ({x}) er ikke den kjente ({kj}) — sender den som ble sendt"); } }
    let xxh = xxh.or(kjent);
    if let Some(x) = &xxh { body["xxh64"] = serde_json::json!(x); }
    if !fil.tags.is_empty() { body["tags"] = serde_json::json!(fil.tags); }
    if let Some(v) = fil.versjon_av.as_deref().filter(|v| !v.is_empty()) { body["versjonAv"] = serde_json::json!(v); }
    if let Some(n) = fil.utgavenotat.as_deref().filter(|n| !n.is_empty()) { body["utgavenotat"] = serde_json::json!(n); }
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
    if proxy.is_none() && app.ulesbar_video(&lop.id, navn).await {
        body["proxyUlesbar"] = serde_json::json!(true); uten_proxy = true;
    }
    if let Some(n) = proxy {
        // F7 (T2): nøkkelen serveren tilbød sammen med adressen. Bare uten den (tilstand fra før T2) bygges den her.
        let pk = if r.proxy_key.is_empty() { proxy_nokkel_reserve(key) } else { r.proxy_key.clone() };
        body["proxyKey"] = serde_json::json!(pk);
        body["proxyStorrelse"] = serde_json::json!(n);
    }
    // Ekstra felt fra appen — aldri over noe craten selv har satt.
    for (n, v) in &fil.felt { if body.get(n).is_none() { body[n] = v.clone(); } }
    // Serveren lager thumb/EXIF/dedup her; for video kjorer ffmpeg LOKALT forst
    // (poster + sprite). Begge deler skjer ETTER at siste byte er sendt — uten
    // status sto appen bare stille pa 100 % (Vegard 28/8).
    let _ = app.framdrift(Framdrift { id: lop.id.clone(), hentet: bytes, total: bytes, status: "fullfor".into(), feil: None });
    app.videofelt(&lop.id, navn, bytes, &mut body).await;
    // Med omforsøk (0.3.0): bytene ligger alt i lageret, så et nett-hikk her skal ikke koste en hel
    // ny opplasting. Serveren er idempotent på fullfør («allerede»).
    let (st, d) = portal_post(k, &body, None).await?;
    if !(200..300).contains(&st) {
        if let Some(kode) = d["versjonAvFeil"].as_str() { return Err(versjon_av_avslag(lop, kode, &d, key).await); }
        let tekst = format!("fullfør: {}", d["error"].as_str().unwrap_or("feilet"));
        // 404/410 = portalen kjenner ikke opplastingen lenger (ryddet, eller bytene manglet). Tilstanden er verdiløs, og
        // neste forsøk skal begynne på nytt.
        if st == 404 || st == 410 { let _ = std::fs::remove_file(&lop.tsti); return Err(Feil::Borte(tekst)); }
        return Err(Feil::server(st, tekst));
    }
    let _ = std::fs::remove_file(&lop.tsti);
    // «utenProxy» er en FERDIG tilstand — fila ER lastet opp, og originalen er
    // trygt i skapet. Den sier bare at det mangler noe å spille av, og hva man
    // gjør med det. Derfor teller den som fullført alle steder i UI-et.
    let allerede = d["allerede"].as_bool().unwrap_or(false);
    let status = if allerede { "hoppet" }
        else if uten_proxy { "utenProxy" } else { "ferdig" };
    Ok((Opplastet {
        id: d["id"].as_str().unwrap_or("").to_string(),
        allerede,
        original_key: key.to_string(),
        xxh64: xxh,
        versjon_av: d["versjonAv"].as_str().filter(|v| !v.is_empty()).map(String::from),
    }, status))
}

/// Nei på `versjonAv` ved fullfør (P1-kontrakten, portal ea77e32a):
/// - `midlertidig` (503): alt står — tilstanden også, så et nytt forsøk fullfører den samme opplastingen.
/// - alt annet er ENDELIG: tilstanden slettes, og et nytt forsøk på samme versjonAv gir samme svar.
///   - med `id`: feiekosten rakk å lage raden, og den står alene. Bytene ER i skapet: ingen `avbryt`.
///   - `ryddet` (true = noe ble fjernet hos serveren, false = ingenting å fjerne eller ryddingen feilet): serverens sak.
///   - `ryddet` mangler helt (portal fra før P1): vi sier `avbryt` selv.
async fn versjon_av_avslag<K: Melder>(lop: &Lop<'_, K>, kode: &str, d: &serde_json::Value, key: &str) -> Feil {
    let tekst = d["error"].as_str().unwrap_or("Kunne ikke lage en ny versjon av fila").to_string();
    let id = d["id"].as_str().filter(|i| !i.is_empty()).map(String::from);
    if kode == "midlertidig" { return Feil::VersjonAv { kode: kode.to_string(), ryddet: false, id, tekst }; }
    let ryddet = if id.is_some() { false }
        else if let Some(r) = d["ryddet"].as_bool() { r }
        else { avbryt_nokler(lop.k, &[key.to_string()]).await };
    let _ = std::fs::remove_file(&lop.tsti);
    log::warn!("{}: ny versjon avvist ({kode}){}", lop.fil.navn, if ryddet { " — ryddet" } else { "" });
    Feil::VersjonAv { kode: kode.to_string(), ryddet, id, tekst }
}

/// `action: avbryt` for nøklene. Gir om portalen svarte ja.
async fn avbryt_nokler(k: &Klient, nokler: &[String]) -> bool {
    match k.api().post(format!("{}/api/rawskap/opplasting", k.portal())).json(&serde_json::json!({ "action": "avbryt", "originalKeys": nokler })).send().await {
        Ok(r) => r.status().is_success(),
        Err(_) => false,
    }
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
/// Transfers kø kjenner ikke mappen en fil endte i (undermappene lages underveis), så her gjelder ALLE tilstander for
/// fila (sti + størrelse + endret-tid), uansett mappe — og v1-fila fra før T2. `forkast` tar én bestemt mappe.
///
/// ⚠ Pause og «Avbryt» skal IKKE kalle dette — der er resume-tilstanden selve poenget.
pub async fn forkast_opplasting(k: &Klient, valg: &Valg, filer: &[OppFil]) -> Result<u32, Feil> {
    let v2 = alle_v2(&valg.tilstand_dir);
    let mut funnet: Vec<(PathBuf, String)> = Vec::new();
    for f in filer {
        // Samme nøkkel som opplastingen: sti + størrelse + endret-tid (ms). Finnes ikke fila lenger,
        // finner vi heller ikke resume-fila — da tar feiekosten den, som før.
        let Ok(m) = tokio::fs::metadata(&f.sti).await else { continue };
        let (len, mtime) = (m.len(), mtime_ms(&m));
        let p1 = tilstand_sti_v1(&valg.tilstand_dir, &f.sti, len, mtime);
        if let Some(r) = resume_les(&p1).filter(|r| r.versjon < TILSTAND_VERSJON) { funnet.push((p1, r.original_key)); }
        for (p, r) in v2.iter().filter(|(_, r)| r.sti == f.sti && r.bytes == len && r.sist_endret == mtime) { funnet.push((p.clone(), r.original_key.clone())); }
    }
    forkast_funnet(k, funnet).await
}

/// «Fjern» for filer med kjent mappe (rawMotion): bare tilstanden for akkurat denne fila til akkurat denne mappen.
pub async fn forkast(k: &Klient, valg: &Valg, filer: &[Fil]) -> Result<u32, Feil> {
    let mut funnet: Vec<(PathBuf, String)> = Vec::new();
    for f in filer {
        let sti = f.sti.to_string_lossy().to_string();
        let meta = tokio::fs::metadata(&f.sti).await.ok();
        let mtime = meta.as_ref().map(mtime_ms).unwrap_or(0);
        let len = match (&f.kilde, &meta) { (Some(kl), _) => kl.lengde(), (None, Some(m)) => m.len(), (None, None) => continue };
        let p2 = tilstand_sti(&valg.tilstand_dir, &sti, len, mtime, &f.mappe_id);
        if let Some(r) = resume_les(&p2).filter(|r| r.versjon >= TILSTAND_VERSJON && r.mappe_id == f.mappe_id) { funnet.push((p2, r.original_key)); }
        let p1 = tilstand_sti_v1(&valg.tilstand_dir, &sti, len, mtime);
        if let Some(r) = resume_les(&p1).filter(|r| r.versjon < TILSTAND_VERSJON && r.mappe_id == f.mappe_id) { funnet.push((p1, r.original_key)); }
    }
    forkast_funnet(k, funnet).await
}

async fn forkast_funnet(k: &Klient, funnet: Vec<(PathBuf, String)>) -> Result<u32, Feil> {
    let nokler: Vec<String> = funnet.iter().map(|(_, n)| n.clone()).filter(|n| !n.is_empty()).collect();
    if !nokler.is_empty() {
        for bolk in nokler.chunks(500) {
            let r = k.api().post(format!("{}/api/rawskap/opplasting", k.portal())).json(&serde_json::json!({ "action": "avbryt", "originalKeys": bolk })).send().await.map_err(|e| Feil::Nett(nettfeil(e)))?;
            if !r.status().is_success() { return Err(Feil::server(r.status().as_u16(), format!("Portalen svarte {}", r.status().as_u16()))); }
        }
        log::info!("forkastet {} påbegynte opplastinger", nokler.len());
    }
    for (s, _) in &funnet { let _ = std::fs::remove_file(s); }
    Ok(nokler.len() as u32)
}

/// OPPLASTINGSØKT (5/10, Vegard: «jeg må passe på at denne blir ferdig før jeg sender den»): meld portalen
/// FASITEN for en opplastingsjobb (antall + bytes + mappe), fremdrift underveis og slutten — så den vet når ALT
/// er oppe og kan sende en ventende deling (lib/opplasting-okt.ts i portalen). Bare de tre okt-handlingene slippes
/// gjennom; en feil her skal aldri stoppe selve opplastingen (JS ignorerer den).
pub async fn opplasting_okt(k: &Klient, data: serde_json::Value) -> Result<serde_json::Value, Feil> {
    let handling = data["action"].as_str().unwrap_or("");
    if !matches!(handling, "okt-start" | "okt-framdrift" | "okt-ferdig") { return Err(Feil::server(0, "ukjent handling")); }
    let r = k.api().post(format!("{}/api/rawskap/opplasting", k.portal())).json(&data).send().await.map_err(|e| Feil::Nett(nettfeil(e)))?;
    let st = r.status().as_u16();
    let d: serde_json::Value = r.json().await.unwrap_or(serde_json::json!({}));
    if !(200..300).contains(&st) { return Err(Feil::server(st, d["error"].as_str().unwrap_or("økt-kallet feilet"))); }
    Ok(d)
}
