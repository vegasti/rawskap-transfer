// INNLESING FRA MINNEKORT (T4, 24/10-2026): én trygg kopi fra kortet til arkivet, og en valgfri
// reservekopi. Transfer bruker den i «Minnekort», og rawLight skal bruke den i «Importer fra
// minnekort» (T5). Begge appene gir da fotografen det samme løftet: «Kortet er kopiert og kontrollert
// — trygt å formatere». Det løftet gis bare når det er sant.
//
// Reglene:
//  - Aldri overskrive. Kopien skrives til `<navn>.rawskap-<n>.part`, åpnet med `create_new`, og den
//    får sitt endelige navn med en flytting som feiler hvis navnet er tatt (Windows: MoveFileExW uten
//    REPLACE_EXISTING, Mac: renamex_np med RENAME_EXCL, ellers en hard lenke). `std::fs::rename`
//    brukes aldri mot et endelig navn, fordi den erstatter.
//  - Samme navn og samme xxh64 gir `Fantes`, og da skrives ingenting. Samme navn og annet innhold
//    (to kameraer med hver sin DSC0001.ARW) gir neste ledige navn: `DSC0001-1.ARW`, `-2` osv.
//  - Aldri slette fra kortet. Ingenting her åpner en kildefil for skriving.
//  - Hver kopi skrives helt til disk (`sync_all`) før den kontrolleres.
//  - `mtime` følger med kopien (F2). Skapet bruker filas tid som opptaksdato for video.
//  - xxh64 per fil, regnet mens kortet leses. Kortet leses én gang. Unntaket er når et mål har en fil
//    med samme navn og størrelse, men annet innhold: da leses kildefila en gang til for det målet, og
//    begge lesingene må gi samme xxh64.
//  - Kontroll: kopien leses tilbake etter sync og sammenlignes. På Windows skjer det utenom OS-ets
//    filbuffer (FILE_FLAG_NO_BUFFERING). Hva hver vei faktisk dekker, står ved `Kontroll`.
//  - Et avbrutt løp etterlater aldri en halv fil under det endelige navnet: `.part` slettes, og
//    omdøpingen skjer først etter kontrollen.
//  - Kvittering: et manifest i `<mål>/.rawskap-innlesing/<tid>.json` i hvert mål.
//  - `trygt_aa_formatere` er sann bare når hver fil er kontrollert i ALLE målene, ingen av målene
//    ligger på samme volum som kilden, og kvitteringen er skrevet i alle målene.
//
// Blokkerende: kjør `last_inn` i en egen tråd (`spawn_blocking`).
//
// Reglene og navnemønsteret (-1, -2 …) er lånt fra LightCraft-motoren (vegasti/rawlight,
// crates/engine/src/import_move.rs: `copy_new`, `copy_verified` og `fill`). Den koden er
// Copyright (c) 2026 ArtCraft Team and the LightCraft contributors, lisensiert under MIT eller
// Apache-2.0 etter eget valg. Den er skrevet om her av Raw Studios AS: `.part` og omdøping i stedet
// for å skrive rett til det endelige navnet, xxh64 i stedet for SipHash128, flere mål og
// kvitteringen. Se crates/skapflyt/NOTICE.
#[path = "kort.rs"]
pub mod kort;
pub use kort::{dcim_i, kort, kort_i, Kort};

use crate::{Framdrift, Melder, Stopp};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Blokkstørrelsen for lesing og skriving. Kort og NAS liker store, sekvensielle biter.
const BLOKK: usize = 4 << 20;
/// Ubuffret lesing på Windows krever at bufferen og lengden følger sektorstørrelsen. 64 KiB dekker
/// alle sektorstørrelser som finnes i praksis (512 og 4096).
#[cfg_attr(not(windows), allow(dead_code))]
const JUSTERING: usize = 64 << 10;

/// Én fil på kortet.
#[derive(Clone, Debug)]
pub struct Kildefil {
    pub sti: PathBuf,
    /// Stien under kortets rot med `/` som skille («DCIM/100MSDCF/DSC0001.ARW»). Standardplasseringen
    /// legger kopien her under målet.
    pub relativ: String,
    /// Størrelsen da kortet ble lest. Gir kortet noe annet under kopieringen, feiler fila.
    pub bytes: u64,
    /// Filas `mtime`. Kopien får tiden fila har når den åpnes, så dette feltet er til `plassering`
    /// (f.eks. «Etter dato» med mtime som reserve).
    pub endret: SystemTime,
}

impl Kildefil {
    /// Les størrelse og tid fra disken.
    pub fn fra_sti(sti: impl Into<PathBuf>, relativ: impl Into<String>) -> io::Result<Kildefil> {
        let sti = sti.into();
        let m = fs::metadata(&sti)?;
        Ok(Kildefil { relativ: relativ.into(), bytes: m.len(), endret: m.modified().unwrap_or(UNIX_EPOCH), sti })
    }
}

/// Hvor en fil havner under hvert mål: en relativ sti med filnavnet til slutt. Absolutte stier og
/// `..` avvises per fil.
pub type Plassering = Box<dyn Fn(&Kildefil) -> PathBuf + Send + Sync>;

/// Standardplasseringen: som på kortet (`relativ`). Transfer bruker den. rawLights «Etter dato»
/// (ÅÅÅÅ/MM/DD) er en lukning hos rawLight, ikke her.
pub fn som_paa_kortet() -> Plassering {
    Box::new(|f: &Kildefil| PathBuf::from(&f.relativ))
}

pub struct Valg {
    /// Målrøttene: 1 = arkivet, 2 = arkivet og reservekopien. Mappene lages hvis de mangler.
    pub maal: Vec<PathBuf>,
    pub plassering: Plassering,
    /// Filer som kopieres samtidig (1–8). Kort leses best i rekkefølge, så 1 er standard.
    pub samtidig: usize,
    /// Appen og versjonen, til kvitteringen («RawskapTransfer/0.3.7»).
    pub app: String,
}

impl Valg {
    /// Som på kortet, én fil om gangen.
    pub fn ny(maal: Vec<PathBuf>, app: impl Into<String>) -> Valg {
        Valg { maal, plassering: som_paa_kortet(), samtidig: 1, app: app.into() }
    }
}

/// Hva som skjedde med én fil i ett mål.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Utfall {
    /// Kopiert, skrevet til disk, kontrollert og gitt et ledig navn.
    Kopiert,
    /// Fila fantes alt i målet, med samme navn (eller et `-N`-navn) og samme xxh64. Ingenting ble skrevet.
    Fantes,
    /// Stoppet før den var ferdig. Ingen halv fil ligger igjen under det endelige navnet.
    Avbrutt,
    Feil(String),
}

impl Utfall {
    /// Ligger fila kontrollert i målet?
    pub fn kontrollert(&self) -> bool {
        matches!(self, Utfall::Kopiert | Utfall::Fantes)
    }
}

/// Hvordan kopien ble lest tilbake.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Kontroll {
    /// Windows, FILE_FLAG_NO_BUFFERING: lesingen gikk utenom OS-ets filbuffer og helt til
    /// lagringsenheten. Enhetens egen hurtigbuffer kan likevel ha svart, og på en NAS kan serverens
    /// minne ha svart. `sync_all` har bedt begge om å skrive til platen eller flashen først.
    ForbiBufferen,
    /// Mac (F_NOCACHE) og Linux (POSIX_FADV_DONTNEED etter sync): OS-et er bedt om å ikke svare fra
    /// bufferen. Det er et råd og ingen garanti. Det er ikke målt på Mac.
    BedtForbiBufferen,
    /// Vanlig lesing, som kan ha kommet fra OS-ets buffer. Den fanger feil i kopieringen og i
    /// skrivingen, men ikke en disk som ikke tok vare på det den fikk.
    Buffret,
}

/// Én kopi av én fil.
#[derive(Clone, Debug)]
pub struct Kopi {
    /// Målroten (som i `Valg::maal`).
    pub maal: PathBuf,
    /// Kopien, når utfallet er `Kopiert` eller `Fantes`.
    pub sti: Option<PathBuf>,
    pub utfall: Utfall,
    /// Hvordan kopien ble lest tilbake, når den ble det.
    pub kontroll: Option<Kontroll>,
}

/// Én fil, med én kopi per mål.
#[derive(Clone, Debug)]
pub struct Innlest {
    pub kilde: PathBuf,
    pub relativ: String,
    pub bytes: u64,
    /// xxh64 av kortets bytes (16 hex-tegn), når fila ble lest.
    pub xxh64: Option<String>,
    pub kopier: Vec<Kopi>,
}

impl Innlest {
    /// Ligger fila kontrollert i alle målene?
    pub fn kontrollert(&self) -> bool {
        !self.kopier.is_empty() && self.kopier.iter().all(|k| k.utfall.kontrollert())
    }
    /// Feilen å vise for fila: den første kopien som ikke kom i mål («Avbrutt» ved avbrudd).
    pub fn feil(&self) -> Option<String> {
        self.kopier.iter().find_map(|k| match &k.utfall {
            Utfall::Feil(e) => Some(e.clone()),
            _ => None,
        }).or_else(|| self.kopier.iter().any(|k| k.utfall == Utfall::Avbrutt).then(|| "Avbrutt".to_string()))
    }
}

pub struct Rapport {
    /// I samme rekkefølge som filene som ble gitt inn.
    pub filer: Vec<Innlest>,
    /// Kvitteringene som ble skrevet, én per mål.
    pub manifest: Vec<PathBuf>,
    /// Sann bare når hver fil er kontrollert i alle målene, ingen mål ligger på samme volum som
    /// kilden og kvitteringen er skrevet i alle målene. Det gjelder filene som ble gitt inn: kalleren
    /// skal gi hele kortet.
    pub trygt_aa_formatere: bool,
    /// Hvorfor det ikke er trygt, når filene ellers er i orden (samme disk, kvittering som mangler).
    pub ikke_trygt: Option<String>,
}

/// Kopier `filer` til hvert mål i `valg` og kontroller hver kopi. Framdriften meldes per fil med
/// kildestien som id: «laster» (hentet = bytes lest fra kortet), «hash» (kontrollen), «verifisert»
/// eller «feil» (med «Avbrutt» ved avbrudd).
///
/// `s` gjelder hele løpet (`global`) og enkeltfiler (`sett`, med kildestien som id). Feltet `id` i
/// `s` brukes ikke.
///
/// `Err` betyr at løpet ikke kunne starte: ingen mål, samme mål to ganger, eller et mål som ikke kan
/// lages. Alt annet står per fil i rapporten.
pub fn last_inn<M: Melder>(filer: &[Kildefil], valg: &Valg, m: &M, s: &Stopp) -> Result<Rapport, String> {
    if valg.maal.is_empty() {
        return Err("Velg et mål for kopien".into());
    }
    for r in &valg.maal {
        fs::create_dir_all(r).map_err(|e| format!("Fikk ikke laget {}: {e}", r.display()))?;
    }
    let ekte: Vec<PathBuf> = valg.maal.iter().map(|r| fs::canonicalize(r).unwrap_or_else(|_| r.clone())).collect();
    for i in 0..ekte.len() {
        for j in 0..i {
            if ekte[i] == ekte[j] {
                return Err(format!("Reservekopien kan ikke ligge i samme mappe som arkivet ({})", valg.maal[i].display()));
            }
        }
    }
    let start = SystemTime::now();

    let n = valg.samtidig.clamp(1, 8).min(filer.len().max(1));
    let resultater: Vec<Innlest> = if n == 1 {
        filer.iter().map(|f| les_inn_en(f, valg, m, s)).collect()
    } else {
        let neste = AtomicUsize::new(0);
        let ut: Mutex<Vec<Option<Innlest>>> = Mutex::new((0..filer.len()).map(|_| None).collect());
        std::thread::scope(|sc| {
            for _ in 0..n {
                sc.spawn(|| loop {
                    let i = neste.fetch_add(1, Ordering::Relaxed);
                    let Some(f) = filer.get(i) else { break };
                    let r = les_inn_en(f, valg, m, s);
                    if let Ok(mut v) = ut.lock() { v[i] = Some(r); }
                });
            }
        });
        ut.into_inner().unwrap_or_else(|e| e.into_inner()).into_iter().flatten().collect()
    };

    // Ligger et mål på samme volum som kilden, forsvinner kopien når kortet formateres.
    let mut ikke_trygt: Option<String> = None;
    let mut kildevolum: HashMap<PathBuf, Option<String>> = HashMap::new();
    for f in filer {
        let d = f.sti.parent().map(Path::to_path_buf).unwrap_or_default();
        kildevolum.entry(d.clone()).or_insert_with(|| volum(&d));
    }
    for (i, r) in valg.maal.iter().enumerate() {
        let mv = volum(r);
        for kv in kildevolum.values() {
            match (&mv, kv) {
                (Some(a), Some(b)) if a == b => {
                    ikke_trygt = Some(format!("{} ligger på samme disk som kilden", if i == 0 { "Arkivmålet".to_string() } else { format!("Mål {} ({})", i + 1, r.display()) }));
                }
                (Some(_), Some(_)) => {}
                _ => { ikke_trygt.get_or_insert_with(|| format!("Fikk ikke sjekket at {} ligger på en annen disk enn kilden", r.display())); }
            }
        }
    }

    // Kvitteringen i hvert mål, også når noe feilet: den sier hva som ble gjort.
    let slutt = SystemTime::now();
    let filer_ok = !resultater.is_empty() && resultater.iter().all(|f| f.kontrollert() && f.kopier.len() == valg.maal.len());
    let mut manifest = Vec::new();
    let mut manifest_feil: Option<String> = None;
    for (i, r) in valg.maal.iter().enumerate() {
        let trygt_her = filer_ok && ikke_trygt.is_none();
        let json = manifest_json(&resultater, valg, i, start, slutt, trygt_her);
        match skriv_manifest(r, &json, start) {
            Ok(p) => manifest.push(p),
            Err(e) => { manifest_feil.get_or_insert(e); }
        }
    }
    if let Some(e) = manifest_feil { ikke_trygt.get_or_insert(e); }
    let trygt_aa_formatere = filer_ok && ikke_trygt.is_none();
    Ok(Rapport { filer: resultater, manifest, trygt_aa_formatere, ikke_trygt })
}

/// Ett mål for én fil, mens fila behandles.
struct Spor {
    rot: PathBuf,
    dir: PathBuf,
    /// Filer i målet med fila sitt navn (eller et `-N`-navn) og samme størrelse.
    kandidater: Vec<PathBuf>,
    /// xxh64 av filer i målet som alt er lest i denne runden.
    kjente: HashMap<PathBuf, String>,
    skriving: Option<Skriving>,
    utfall: Option<Utfall>,
    sti: Option<PathBuf>,
    kontroll: Option<Kontroll>,
}

struct Skriving {
    part: PathBuf,
    fil: Option<File>,
    skrevet: u64,
    feil: Option<String>,
}

fn les_inn_en<M: Melder>(fil: &Kildefil, valg: &Valg, m: &M, s: &Stopp) -> Innlest {
    let id = fil.sti.to_string_lossy().to_string();
    let stopp = Stopp { global: s.global.clone(), sett: s.sett.clone(), id: id.clone() };
    let meld = |hentet: u64, status: &str, feil: Option<String>| {
        m.framdrift(Framdrift { id: id.clone(), hentet, total: fil.bytes, status: status.into(), feil });
    };
    let mut spor: Vec<Spor> = valg.maal.iter().map(|r| Spor { rot: r.clone(), dir: r.clone(), kandidater: Vec::new(), kjente: HashMap::new(), skriving: None, utfall: None, sti: None, kontroll: None }).collect();
    let ferdig = |mut spor: Vec<Spor>, xxh64: Option<String>| -> Innlest {
        // Ingen `.part` overlever fila, uansett vei hit: det som ble ferdig, har fått sitt navn.
        for sp in spor.iter_mut() { forkast_part(sp); }
        let kopier: Vec<Kopi> = spor.into_iter().map(|sp| Kopi { maal: sp.rot, sti: sp.sti, utfall: sp.utfall.unwrap_or(Utfall::Avbrutt), kontroll: sp.kontroll }).collect();
        let ut = Innlest { kilde: fil.sti.clone(), relativ: fil.relativ.clone(), bytes: fil.bytes, xxh64, kopier };
        match ut.feil() {
            None => meld(fil.bytes, "verifisert", None),
            Some(e) => {
                if e != "Avbrutt" { log::warn!("innlesing feilet: {}: {e}", fil.relativ); }
                meld(0, "feil", Some(e));
            }
        }
        ut
    };
    let alle = |spor: &mut Vec<Spor>, u: Utfall| { for sp in spor.iter_mut().filter(|sp| sp.utfall.is_none()) { sp.utfall = Some(u.clone()); } };

    if stopp.av() {
        alle(&mut spor, Utfall::Avbrutt);
        return ferdig(spor, None);
    }
    meld(0, "laster", None);

    // Plasseringen: relativ, uten `..`, med et filnavn til slutt.
    let rel = (valg.plassering)(fil);
    let (deler, navn) = match del_opp(&rel) {
        Some(x) => x,
        None => {
            alle(&mut spor, Utfall::Feil(format!("Ugyldig plassering for {}: {}", fil.relativ, rel.display())));
            return ferdig(spor, None);
        }
    };

    // Hvert mål: mappa og filene som har navnet fra før.
    for sp in spor.iter_mut() {
        for d in &deler { sp.dir.push(d); }
        if let Err(e) = fs::create_dir_all(&sp.dir) {
            sp.utfall = Some(Utfall::Feil(format!("Fikk ikke laget {}: {e}", sp.dir.display())));
            continue;
        }
        for k in 0..MAKS_NAVN {
            let c = sp.dir.join(navn_nr(&navn, k));
            match fs::metadata(&c) {
                Ok(md) if md.is_file() && md.len() == fil.bytes => {
                    if samme_fil(&c, &fil.sti) {
                        sp.utfall = Some(Utfall::Feil(format!("Målet er selve kildefila ({}). Velg et mål utenfor kortet.", c.display())));
                        break;
                    }
                    sp.kandidater.push(c);
                }
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => break,
                Err(e) => { sp.utfall = Some(Utfall::Feil(format!("Fikk ikke lest {}: {e}", c.display()))); break; }
            }
        }
    }

    // Første lesing av kortet: hash, og skriv til målene som ikke har noen kandidat.
    let mtime = fs::metadata(&fil.sti).and_then(|md| md.modified()).unwrap_or(fil.endret);
    for sp in spor.iter_mut().filter(|sp| sp.utfall.is_none() && sp.kandidater.is_empty()) {
        aapne_part(sp, &navn);
    }
    let forste: Vec<bool> = spor.iter().map(|sp| sp.skriving.is_some()).collect();
    let xxh64: Option<String> = match kopier_kilde(fil, &mut spor, &forste, &stopp, &meld) {
        Ok(h) => Some(h),
        Err(Stans::Avbrutt) => { alle(&mut spor, Utfall::Avbrutt); return ferdig(spor, None); }
        Err(Stans::Feil(e)) => { alle(&mut spor, Utfall::Feil(e)); return ferdig(spor, None); }
    };
    let hash = xxh64.clone().unwrap_or_default();

    // Mål med kandidater: finnes fila alt (samme xxh64), skrives ingenting.
    meld(fil.bytes, "hash", None);
    let mut andre: Vec<bool> = vec![false; spor.len()];
    for (i, sp) in spor.iter_mut().enumerate() {
        if sp.utfall.is_some() || sp.kandidater.is_empty() { continue; }
        for c in sp.kandidater.clone() {
            match les_kontroll(&c) {
                Ok((h, n, k)) => {
                    if n == fil.bytes && h == hash {
                        sp.utfall = Some(Utfall::Fantes);
                        sp.sti = Some(c.clone());
                        sp.kontroll = Some(k);
                        break;
                    }
                    sp.kjente.insert(c, h);
                }
                Err(e) => { sp.utfall = Some(Utfall::Feil(format!("Fikk ikke lest {}: {e}", c.display()))); break; }
            }
        }
        if sp.utfall.is_none() {
            aapne_part(sp, &navn);
            andre[i] = sp.skriving.is_some();
        }
    }
    // Samme navn, samme størrelse, annet innhold: les kortet en gang til for de målene.
    if andre.iter().any(|x| *x) {
        match kopier_kilde(fil, &mut spor, &andre, &stopp, &meld) {
            Ok(h2) if h2 == hash => {}
            Ok(_) => {
                for (sp, ja) in spor.iter_mut().zip(&andre) {
                    if *ja { forkast_part(sp); sp.utfall = Some(Utfall::Feil("Kortet ga ulike bytes ved to lesinger. Sjekk kortet og kortleseren.".into())); }
                }
            }
            Err(Stans::Avbrutt) => { alle(&mut spor, Utfall::Avbrutt); return ferdig(spor, xxh64); }
            Err(Stans::Feil(e)) => {
                for (sp, ja) in spor.iter_mut().zip(&andre) { if *ja { sp.utfall = Some(Utfall::Feil(e.clone())); } }
            }
        }
    }

    // Hver kopi: til disk, tiden, kontroll og et ledig navn.
    for sp in spor.iter_mut().filter(|sp| sp.utfall.is_none() && sp.skriving.is_some()) {
        if stopp.av() { forkast_part(sp); sp.utfall = Some(Utfall::Avbrutt); continue; }
        match fullfor_kopi(sp, &navn, fil, &hash, mtime) {
            Ok(()) => {}
            Err(e) => { forkast_part(sp); sp.utfall = Some(Utfall::Feil(e)); }
        }
    }
    // Det som står igjen uten utfall, er avbrutt underveis.
    for sp in spor.iter_mut().filter(|sp| sp.utfall.is_none()) { forkast_part(sp); sp.utfall = Some(Utfall::Avbrutt); }
    ferdig(spor, xxh64)
}

/// Hvor mange `-N`-navn som prøves før fila gir opp.
const MAKS_NAVN: u32 = 100_000;

/// `navn` for k = 0, ellers `stamme-k.endelse` (som motorens `copy_new`).
fn navn_nr(navn: &str, k: u32) -> String {
    if k == 0 { return navn.to_string(); }
    match navn.rsplit_once('.') {
        Some((st, e)) if !st.is_empty() => format!("{st}-{k}.{e}"),
        _ => format!("{navn}-{k}"),
    }
}

/// Undermappene og filnavnet i en relativ plassering, eller `None` når den er absolutt, har `..`
/// eller mangler et filnavn.
fn del_opp(rel: &Path) -> Option<(Vec<std::ffi::OsString>, String)> {
    let mut deler: Vec<std::ffi::OsString> = Vec::new();
    for c in rel.components() {
        match c {
            Component::Normal(d) => deler.push(d.to_os_string()),
            Component::CurDir => {}
            _ => return None,
        }
    }
    let navn = deler.pop()?.to_string_lossy().to_string();
    if navn.is_empty() { return None; }
    Some((deler, navn))
}

/// Er `a` og `b` samme fil på disken?
fn samme_fil(a: &Path, b: &Path) -> bool {
    match (fs::canonicalize(a), fs::canonicalize(b)) {
        (Ok(x), Ok(y)) => x == y,
        _ => false,
    }
}

static PART_NR: AtomicU64 = AtomicU64::new(0);

/// Åpne en ny `.part` ved siden av det endelige navnet. Navnet er unikt for prosessen, så to filer
/// med samme navn kan skrives samtidig.
fn aapne_part(sp: &mut Spor, navn: &str) {
    for _ in 0..1000 {
        let nr = PART_NR.fetch_add(1, Ordering::Relaxed);
        let part = sp.dir.join(format!("{navn}.rawskap-{:x}-{nr}.part", std::process::id()));
        match OpenOptions::new().write(true).create_new(true).open(&part) {
            Ok(f) => {
                ikke_behold_i_bufferen(&f);
                sp.skriving = Some(Skriving { part, fil: Some(f), skrevet: 0, feil: None });
                return;
            }
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(e) => { sp.utfall = Some(Utfall::Feil(format!("Fikk ikke skrevet i {}: {e}", sp.dir.display()))); return; }
        }
    }
    sp.utfall = Some(Utfall::Feil(format!("Fant ikke et ledig midlertidig navn i {}", sp.dir.display())));
}

/// Slett `.part` (best mulig: Windows kan holde fila et øyeblikk, f.eks. virusskanneren).
fn forkast_part(sp: &mut Spor) {
    if let Some(w) = sp.skriving.take() {
        drop(w.fil);
        let _ = med_omforsok(|| fs::remove_file(&w.part).or_else(|e| if e.kind() == io::ErrorKind::NotFound { Ok(()) } else { Err(e) }));
    }
}

enum Stans { Avbrutt, Feil(String) }

/// Les kildefila én gang, regn xxh64 og skriv blokkene til sporene i `valgt`. Et mål som feiler
/// (full disk), merkes og slettes, og de andre målene fortsetter. Ved `Err` er `.part` slettet i
/// alle de valgte sporene.
fn kopier_kilde(fil: &Kildefil, spor: &mut [Spor], valgt: &[bool], stopp: &Stopp, meld: &dyn Fn(u64, &str, Option<String>)) -> Result<String, Stans> {
    let r = kopier_kilde_inn(fil, spor, valgt, stopp, meld);
    if r.is_err() {
        for (sp, ja) in spor.iter_mut().zip(valgt) { if *ja { forkast_part(sp); } }
    }
    r
}

fn kopier_kilde_inn(fil: &Kildefil, spor: &mut [Spor], valgt: &[bool], stopp: &Stopp, meld: &dyn Fn(u64, &str, Option<String>)) -> Result<String, Stans> {
    let mut inn = File::open(&fil.sti).map_err(|e| Stans::Feil(format!("Fikk ikke åpnet {}: {e}", fil.sti.display())))?;
    let mut h = xxhash_rust::xxh64::Xxh64::new(0);
    let mut buf = vec![0u8; BLOKK];
    let mut lest: u64 = 0;
    let mut sist = Instant::now();
    loop {
        if stopp.av() {
            for (sp, ja) in spor.iter_mut().zip(valgt) { if *ja { forkast_part(sp); } }
            return Err(Stans::Avbrutt);
        }
        let n = fyll(&mut inn, &mut buf).map_err(|e| Stans::Feil(format!("Fikk ikke lest {}: {e}", fil.sti.display())))?;
        if n == 0 { break; }
        let blokk = &buf[..n];
        h.update(blokk);
        lest += n as u64;
        for (sp, ja) in spor.iter_mut().zip(valgt) {
            if !*ja { continue; }
            let rot = sp.rot.clone();
            let Some(w) = sp.skriving.as_mut() else { continue };
            if w.feil.is_some() { continue; }
            let res = match w.fil.as_mut() {
                Some(f) => uhell_skriv(&rot, w.skrevet, n).and_then(|()| f.write_all(blokk)),
                None => Ok(()),
            };
            match res {
                Ok(()) => { w.skrevet += n as u64; uhell_underveis(&rot, w.skrevet); }
                Err(e) => { w.feil = Some(format!("Fikk ikke skrevet til {}: {e}", rot.display())); }
            }
        }
        // Et mål som feilet, slettes med en gang, så en full disk ikke blir stående full.
        for (sp, ja) in spor.iter_mut().zip(valgt) {
            if *ja {
                if let Some(e) = sp.skriving.as_ref().and_then(|w| w.feil.clone()) { forkast_part(sp); sp.utfall = Some(Utfall::Feil(e)); }
            }
        }
        if sist.elapsed() > Duration::from_millis(150) { sist = Instant::now(); meld(lest, "laster", None); }
    }
    if lest != fil.bytes {
        for (sp, ja) in spor.iter_mut().zip(valgt) { if *ja { forkast_part(sp); } }
        return Err(Stans::Feil(format!("Kortet ga {lest} byte, men fila var {} byte da kortet ble lest. Les inn kortet på nytt.", fil.bytes)));
    }
    Ok(format!("{:016x}", h.digest()))
}

/// Skriv kopien helt til disk, sett tiden, kontroller den og gi den et ledig navn.
fn fullfor_kopi(sp: &mut Spor, navn: &str, fil: &Kildefil, hash: &str, mtime: SystemTime) -> Result<(), String> {
    let rot = sp.rot.clone();
    let w = sp.skriving.as_mut().ok_or("Ingen kopi å fullføre")?;
    let f = w.fil.take().ok_or("Kopien er alt lukket")?;
    let disk = |e: io::Error| format!("Fikk ikke skrevet {} helt til disk: {e}", w.part.display());
    f.sync_all().map_err(disk)?;
    // Tiden etter dataene: på en nettverksdisk kan utsatte skrivinger ellers sette tiden til «nå».
    let _ = f.set_modified(mtime);
    f.sync_all().map_err(disk)?;
    drop(f);
    if let Ok(t) = fs::metadata(&w.part).and_then(|md| md.modified()) {
        if avstand(t, mtime) > Duration::from_secs(2) {
            if let Ok(f) = OpenOptions::new().write(true).open(&w.part) { let _ = f.set_modified(mtime); let _ = f.sync_all(); }
        }
    }
    uhell_etter_skriving(&rot, &w.part);

    let (h, n, k) = les_kontroll(&w.part).map_err(|e| format!("Fikk ikke lest kopien i {} tilbake: {e}", rot.display()))?;
    if n != fil.bytes || h != hash {
        return Err(format!("Kopien i {} er ikke lik kortet (sjekksum ulik). Den er slettet. Prøv igjen.", rot.display()));
    }
    sp.kontroll = Some(k);

    let part = w.part.clone();
    for nr in 0..MAKS_NAVN {
        let c = sp.dir.join(navn_nr(navn, nr));
        match med_omforsok(|| flytt_uten_aa_erstatte(&part, &c)) {
            Ok(()) => {
                synk_mappe(&sp.dir);
                sp.skriving = None;
                sp.sti = Some(c);
                sp.utfall = Some(Utfall::Kopiert);
                return Ok(());
            }
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                // Kommet til siden sist (en annen fil i samme runde): er det den samme fila?
                let lik = match fs::metadata(&c) {
                    Ok(md) if md.is_file() && md.len() == fil.bytes => {
                        let kjent = sp.kjente.get(&c).cloned();
                        let h = kjent.or_else(|| les_kontroll(&c).ok().map(|(h, _, _)| h));
                        h.as_deref() == Some(hash)
                    }
                    _ => false,
                };
                if lik {
                    forkast_part(sp);
                    sp.sti = Some(c);
                    sp.utfall = Some(Utfall::Fantes);
                    return Ok(());
                }
            }
            Err(e) => return Err(format!("Fikk ikke gitt kopien navnet {}: {e}", c.display())),
        }
    }
    Err(format!("Fant ikke et ledig navn for {navn} i {}", sp.dir.display()))
}

fn avstand(a: SystemTime, b: SystemTime) -> Duration {
    a.duration_since(b).or_else(|_| b.duration_since(a)).unwrap_or_default()
}

/// Les til bufferen er full eller fila er slutt.
fn fyll(f: &mut File, buf: &mut [u8]) -> io::Result<usize> {
    let mut n = 0;
    while n < buf.len() {
        match f.read(&mut buf[n..]) {
            Ok(0) => break,
            Ok(k) => n += k,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(n)
}

/// Prøv igjen et par ganger når Windows holder fila (virusskanneren, indeksering): ERROR_ACCESS_DENIED
/// og ERROR_SHARING_VIOLATION. Alt annet, og AlreadyExists, går rett tilbake.
fn med_omforsok<T>(mut f: impl FnMut() -> io::Result<T>) -> io::Result<T> {
    let mut forsok = 0;
    loop {
        match f() {
            Err(e) if cfg!(windows) && matches!(e.raw_os_error(), Some(5) | Some(32)) && forsok < 20 => {
                forsok += 1;
                std::thread::sleep(Duration::from_millis(100));
            }
            r => return r,
        }
    }
}

// ── Kontrollen: les kopien tilbake ───────────────────────────────────────────────────────────

/// xxh64, antall bytes og hvordan fila ble lest.
fn les_kontroll(sti: &Path) -> io::Result<(String, u64, Kontroll)> {
    #[cfg(windows)]
    {
        match les_ubuffret(sti) {
            Ok((h, n)) => return Ok((h, n, Kontroll::ForbiBufferen)),
            // Ugyldig parameter: disken eller nettverksdisken godtar ikke ubuffret lesing. Les vanlig.
            Err(e) if e.raw_os_error() == Some(87) => {}
            Err(e) => return Err(e),
        }
    }
    let mut f = File::open(sti)?;
    let kontroll = be_om_ubuffret(&f);
    let mut h = xxhash_rust::xxh64::Xxh64::new(0);
    let mut buf = vec![0u8; BLOKK];
    let mut n: u64 = 0;
    loop {
        let lest = fyll(&mut f, &mut buf)?;
        if lest == 0 { break; }
        h.update(&buf[..lest]);
        n += lest as u64;
    }
    Ok((format!("{:016x}", h.digest()), n, kontroll))
}

/// Windows: FILE_FLAG_NO_BUFFERING. Bufferen må ligge på en sektorgrense, og hver lesing må be om et
/// helt antall sektorer. Den siste lesingen gir resten av fila.
#[cfg(windows)]
fn les_ubuffret(sti: &Path) -> io::Result<(String, u64)> {
    use std::os::windows::fs::OpenOptionsExt;
    const FILE_FLAG_NO_BUFFERING: u32 = 0x2000_0000;
    const FILE_FLAG_SEQUENTIAL_SCAN: u32 = 0x0800_0000;
    let mut f = OpenOptions::new().read(true).custom_flags(FILE_FLAG_NO_BUFFERING | FILE_FLAG_SEQUENTIAL_SCAN).open(sti)?;
    let lengde = f.metadata()?.len();
    let mut lager = vec![0u8; BLOKK + JUSTERING];
    let fra = lager.as_ptr().align_offset(JUSTERING);
    let buf = &mut lager[fra..fra + BLOKK];
    let mut h = xxhash_rust::xxh64::Xxh64::new(0);
    let mut n: u64 = 0;
    while n < lengde {
        let k = match f.read(buf) {
            Ok(k) => k,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        };
        if k == 0 { break; }
        h.update(&buf[..k]);
        n += k as u64;
    }
    Ok((format!("{:016x}", h.digest()), n))
}

/// Mac og Linux: be OS-et om å ikke svare fra bufferen. Linux: kast fila sine sider fra bufferen (de
/// er rene etter sync). Mac: F_NOCACHE på lesingen.
#[cfg(all(target_os = "linux", target_pointer_width = "64"))]
fn be_om_ubuffret(f: &File) -> Kontroll {
    use std::os::unix::io::AsRawFd;
    extern "C" { fn posix_fadvise(fd: i32, offset: i64, len: i64, advice: i32) -> i32; }
    const POSIX_FADV_DONTNEED: i32 = 4;
    // SAFETY: fd er en åpen fil som lever like lenge som kallet; kallet leser ingen minne fra oss.
    let r = unsafe { posix_fadvise(f.as_raw_fd(), 0, 0, POSIX_FADV_DONTNEED) };
    if r == 0 { Kontroll::BedtForbiBufferen } else { Kontroll::Buffret }
}

#[cfg(target_os = "macos")]
fn be_om_ubuffret(f: &File) -> Kontroll {
    if mac_nocache(f) { Kontroll::BedtForbiBufferen } else { Kontroll::Buffret }
}

#[cfg(not(any(all(target_os = "linux", target_pointer_width = "64"), target_os = "macos")))]
fn be_om_ubuffret(_f: &File) -> Kontroll { Kontroll::Buffret }

#[cfg(target_os = "macos")]
fn mac_nocache(f: &File) -> bool {
    use std::os::unix::io::AsRawFd;
    extern "C" { fn fcntl(fd: i32, cmd: i32, ...) -> i32; }
    const F_NOCACHE: i32 = 48;
    // SAFETY: fd er en åpen fil; F_NOCACHE tar et heltall og rører ikke minne.
    unsafe { fcntl(f.as_raw_fd(), F_NOCACHE, 1i32) != -1 }
}

/// Skrivingen: på Mac skal heller ikke kopien bli liggende i bufferen, ellers kan kontrollen lese
/// den derfra. Andre steder gjør dette ingenting.
fn ikke_behold_i_bufferen(_f: &File) {
    #[cfg(target_os = "macos")]
    { let _ = mac_nocache(_f); }
}

// ── Flytting som aldri erstatter ─────────────────────────────────────────────────────────────

/// Gi `fra` navnet `til`, men bare hvis `til` er ledig (AlreadyExists ellers). Atomisk der
/// plattformen kan det.
#[cfg(windows)]
fn flytt_uten_aa_erstatte(fra: &Path, til: &Path) -> io::Result<()> {
    use std::os::windows::ffi::OsStrExt;
    #[link(name = "kernel32")]
    extern "system" { fn MoveFileExW(eksisterende: *const u16, ny: *const u16, flagg: u32) -> i32; }
    // Uten MOVEFILE_REPLACE_EXISTING: feiler med ERROR_ALREADY_EXISTS når navnet er tatt.
    const MOVEFILE_WRITE_THROUGH: u32 = 0x8;
    let bred = |p: &Path| -> Vec<u16> { lang_sti(p).as_os_str().encode_wide().chain(Some(0)).collect() };
    let (a, b) = (bred(fra), bred(til));
    // SAFETY: begge er nullterminerte UTF-16-strenger som lever gjennom kallet.
    let ok = unsafe { MoveFileExW(a.as_ptr(), b.as_ptr(), MOVEFILE_WRITE_THROUGH) };
    if ok != 0 { Ok(()) } else { Err(io::Error::last_os_error()) }
}

/// Windows-stier over 260 tegn trenger `\\?\`-formen når API-et kalles direkte (std gjør det selv).
#[cfg(windows)]
fn lang_sti(p: &Path) -> PathBuf {
    let s = p.as_os_str().to_string_lossy();
    if s.len() < 240 || s.starts_with(r"\\?\") { return p.to_path_buf(); }
    let s = s.replace('/', "\\");
    if let Some(unc) = s.strip_prefix(r"\\") { PathBuf::from(format!(r"\\?\UNC\{unc}")) } else { PathBuf::from(format!(r"\\?\{s}")) }
}

#[cfg(unix)]
fn flytt_uten_aa_erstatte(fra: &Path, til: &Path) -> io::Result<()> {
    #[cfg(target_os = "macos")]
    {
        use std::os::unix::ffi::OsStrExt;
        extern "C" { fn renamex_np(fra: *const std::os::raw::c_char, til: *const std::os::raw::c_char, flagg: u32) -> i32; }
        const RENAME_EXCL: u32 = 0x4;
        let c = |p: &Path| std::ffi::CString::new(p.as_os_str().as_bytes()).map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "nullbyte i stien"));
        let (a, b) = (c(fra)?, c(til)?);
        // SAFETY: nullterminerte C-strenger som lever gjennom kallet.
        if unsafe { renamex_np(a.as_ptr(), b.as_ptr(), RENAME_EXCL) } == 0 { return Ok(()); }
        let e = io::Error::last_os_error();
        if e.kind() == io::ErrorKind::AlreadyExists { return Err(e); }
        // ENOTSUP/EINVAL: filsystemet kan ikke RENAME_EXCL (f.eks. noen nettverksdisker). Videre under.
    }
    // En hard lenke feiler når navnet er tatt, og er atomisk. Så fjernes `.part`.
    match fs::hard_link(fra, til) {
        Ok(()) => { let _ = fs::remove_file(fra); Ok(()) }
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => Err(e),
        // Filsystemet har ikke harde lenker (FAT/exFAT, noen nettverksdisker): sjekk og flytt. Det er
        // et lite vindu der en annen prosess kan rekke å lage navnet; ikke fra denne craten, som
        // aldri bruker samme `.part` eller samme navn i to tråder samtidig uten å sjekke.
        Err(_) => {
            if fs::symlink_metadata(til).is_ok() { return Err(io::Error::new(io::ErrorKind::AlreadyExists, "navnet er tatt")); }
            fs::rename(fra, til)
        }
    }
}

#[cfg(not(any(windows, unix)))]
fn flytt_uten_aa_erstatte(fra: &Path, til: &Path) -> io::Result<()> {
    if fs::symlink_metadata(til).is_ok() { return Err(io::Error::new(io::ErrorKind::AlreadyExists, "navnet er tatt")); }
    fs::rename(fra, til)
}

/// Gjør omdøpingen varig. Windows: MOVEFILE_WRITE_THROUGH har alt gjort det. Unix: sync mappa.
fn synk_mappe(_dir: &Path) {
    #[cfg(unix)]
    if let Ok(d) = File::open(_dir) { let _ = d.sync_all(); }
}

// ── Volumet: et mål på samme disk som kortet er ingen kopi når kortet formateres ─────────────

/// En nøkkel for volumet stien ligger på (`None` når det ikke lot seg finne).
fn volum(p: &Path) -> Option<String> {
    #[cfg(any(test, feature = "testportal"))]
    if let Some(v) = uhell::falskt_volum(p) { return Some(v); }
    volum_os(p)
}

#[cfg(unix)]
fn volum_os(p: &Path) -> Option<String> {
    use std::os::unix::fs::MetadataExt;
    fs::metadata(p).ok().map(|m| format!("dev:{}", m.dev()))
}

/// Windows: stasjonsbokstaven eller nettverksdelingen etter at stien er løst opp (subst og tilordnede
/// nettverksstasjoner gir den ekte). En disk montert i en mappe ser ut som stasjonen den står på.
#[cfg(windows)]
fn volum_os(p: &Path) -> Option<String> {
    use std::path::Prefix;
    let ekte = fs::canonicalize(p).ok()?;
    let forste = ekte.components().next()?;
    let Component::Prefix(pr) = forste else { return None };
    Some(match pr.kind() {
        Prefix::VerbatimDisk(b) | Prefix::Disk(b) => format!("disk:{}", (b as char).to_ascii_uppercase()),
        Prefix::VerbatimUNC(s, d) | Prefix::UNC(s, d) => format!("unc:{}\\{}", s.to_string_lossy().to_lowercase(), d.to_string_lossy().to_lowercase()),
        _ => pr.as_os_str().to_string_lossy().to_lowercase(),
    })
}

#[cfg(not(any(unix, windows)))]
fn volum_os(_p: &Path) -> Option<String> { None }

// ── Kvitteringen ─────────────────────────────────────────────────────────────────────────────

fn utfall_tekst(u: &Utfall) -> &'static str {
    match u { Utfall::Kopiert => "kopiert", Utfall::Fantes => "fantes", Utfall::Avbrutt => "avbrutt", Utfall::Feil(_) => "feil" }
}

fn manifest_json(filer: &[Innlest], valg: &Valg, maal_nr: usize, start: SystemTime, slutt: SystemTime, trygt: bool) -> serde_json::Value {
    let rot = &valg.maal[maal_nr];
    let rel = |p: &Option<PathBuf>, r: &Path| p.as_ref().map(|p| p.strip_prefix(r).map(|x| x.to_string_lossy().replace('\\', "/")).unwrap_or_else(|_| p.to_string_lossy().to_string()));
    serde_json::json!({
        "versjon": 1,
        "app": valg.app,
        "motor": concat!("skapflyt/", env!("CARGO_PKG_VERSION")),
        "start": utc(start),
        "slutt": utc(slutt),
        "dette_maalet": rot.to_string_lossy(),
        "maal": valg.maal.iter().map(|m| m.to_string_lossy().to_string()).collect::<Vec<_>>(),
        "trygt_aa_formatere": trygt,
        "filer": filer.iter().map(|f| serde_json::json!({
            "kilde": f.kilde.to_string_lossy(),
            "relativ": f.relativ,
            "bytes": f.bytes,
            "xxh64": f.xxh64,
            "kopier": f.kopier.iter().map(|k| serde_json::json!({
                "maal": k.maal.to_string_lossy(),
                "sti": rel(&k.sti, &k.maal),
                "utfall": utfall_tekst(&k.utfall),
                "feil": match &k.utfall { Utfall::Feil(e) => Some(e.clone()), _ => None },
                "kontroll": k.kontroll,
            })).collect::<Vec<_>>(),
        })).collect::<Vec<_>>(),
    })
}

/// Skriv kvitteringen som en ny fil i `<rot>/.rawskap-innlesing/`, via `.part` og til disk.
fn skriv_manifest(rot: &Path, json: &serde_json::Value, start: SystemTime) -> Result<PathBuf, String> {
    let dir = rot.join(".rawskap-innlesing");
    let feil = |e: io::Error| format!("Fikk ikke skrevet kvitteringen i {}: {e}", dir.display());
    fs::create_dir_all(&dir).map_err(feil)?;
    let navn = format!("{}.json", utc(start).replace(':', ""));
    let nr = PART_NR.fetch_add(1, Ordering::Relaxed);
    let part = dir.join(format!("{navn}.rawskap-{:x}-{nr}.part", std::process::id()));
    let res = (|| -> io::Result<PathBuf> {
        let mut f = OpenOptions::new().write(true).create_new(true).open(&part)?;
        f.write_all(serde_json::to_string_pretty(json).unwrap_or_default().as_bytes())?;
        f.sync_all()?;
        drop(f);
        for k in 0..MAKS_NAVN {
            let c = dir.join(navn_nr(&navn, k));
            match med_omforsok(|| flytt_uten_aa_erstatte(&part, &c)) {
                Ok(()) => { synk_mappe(&dir); return Ok(c); }
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(e) => return Err(e),
            }
        }
        Err(io::Error::new(io::ErrorKind::AlreadyExists, "ingen ledige navn"))
    })();
    if res.is_err() { let _ = fs::remove_file(&part); }
    res.map_err(feil)
}

/// «2026-10-24T12:13:14Z» uten chrono (sivil dato fra dagnummer, Howard Hinnant).
fn utc(t: SystemTime) -> String {
    let s = t.duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
    let (dager, rest) = (s / 86400, s % 86400);
    let z = dager as i64 + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let mnd = if mp < 10 { mp + 3 } else { mp - 9 };
    let aar = yoe + era * 400 + i64::from(mnd <= 2);
    format!("{aar:04}-{mnd:02}-{d:02}T{:02}:{:02}:{:02}Z", rest / 3600, (rest % 3600) / 60, rest % 60)
}

// ── Feilinnsetting (tester og `testportal`) ──────────────────────────────────────────────────

#[cfg(not(any(test, feature = "testportal")))]
fn uhell_skriv(_rot: &Path, _skrevet: u64, _n: usize) -> io::Result<()> { Ok(()) }
#[cfg(not(any(test, feature = "testportal")))]
fn uhell_underveis(_rot: &Path, _skrevet: u64) {}
#[cfg(not(any(test, feature = "testportal")))]
fn uhell_etter_skriving(_rot: &Path, _part: &Path) {}

#[cfg(any(test, feature = "testportal"))]
fn uhell_skriv(rot: &Path, skrevet: u64, n: usize) -> io::Result<()> { uhell::skriv(rot, skrevet, n) }
#[cfg(any(test, feature = "testportal"))]
fn uhell_underveis(rot: &Path, skrevet: u64) { uhell::underveis(rot, skrevet) }
#[cfg(any(test, feature = "testportal"))]
fn uhell_etter_skriving(rot: &Path, part: &Path) { uhell::etter_skriving(rot, part) }

/// Feilinnsetting for tester, også for appene som bruker craten (feature `testportal`): en disk som
/// blir full, en kopi som endres etter skriving, og en krok midt i en fil (for avbrudd). Uhellene
/// gjelder ett mål (roten i `Valg::maal`, akkurat som den ble gitt), så tester kan kjøre side om side.
#[cfg(any(test, feature = "testportal"))]
pub mod uhell {
    use std::io;
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex};

    #[derive(Clone)]
    pub enum Uhell {
        /// Skrivingen til målet feiler med «disken er full» når så mange bytes er skrevet i én fil.
        FullDisk { etter: u64 },
        /// Én byte i kopien snus etter `sync_all` og før kontrollen, som en disk som ikke tok vare på den.
        EndretEtterSkriving,
        /// Kalles etter hver blokk som er skrevet til målet, med bytes skrevet så langt i fila.
        Underveis(Arc<dyn Fn(u64) + Send + Sync>),
    }

    static UHELL: Mutex<Vec<(PathBuf, Uhell)>> = Mutex::new(Vec::new());
    static VOLUM: Mutex<Vec<PathBuf>> = Mutex::new(Vec::new());

    /// Legg et uhell på målet `rot`.
    pub fn sett(rot: &Path, u: Uhell) {
        UHELL.lock().unwrap_or_else(|e| e.into_inner()).push((rot.to_path_buf(), u));
    }
    /// Fjern uhellene på målet `rot`.
    pub fn fjern(rot: &Path) {
        UHELL.lock().unwrap_or_else(|e| e.into_inner()).retain(|(r, _)| r != rot);
    }
    /// La alt under `rot` regnes som et eget volum, slik et falskt kort i en midlertidig mappe må.
    /// Uten dette ligger kortet og målene på samme disk, og da er det aldri trygt å formatere.
    pub fn eget_volum(rot: &Path) {
        VOLUM.lock().unwrap_or_else(|e| e.into_inner()).push(rot.to_path_buf());
    }

    fn finn(rot: &Path) -> Vec<Uhell> {
        UHELL.lock().unwrap_or_else(|e| e.into_inner()).iter().filter(|(r, _)| r == rot).map(|(_, u)| u.clone()).collect()
    }
    pub(super) fn falskt_volum(p: &Path) -> Option<String> {
        VOLUM.lock().unwrap_or_else(|e| e.into_inner()).iter().find(|r| p.starts_with(r)).map(|r| format!("test:{}", r.display()))
    }
    pub(super) fn skriv(rot: &Path, skrevet: u64, n: usize) -> io::Result<()> {
        for u in finn(rot) {
            if let Uhell::FullDisk { etter } = u {
                if skrevet + n as u64 > etter { return Err(disk_full()); }
            }
        }
        Ok(())
    }
    pub(super) fn underveis(rot: &Path, skrevet: u64) {
        for u in finn(rot) { if let Uhell::Underveis(f) = u { f(skrevet); } }
    }
    pub(super) fn etter_skriving(rot: &Path, part: &Path) {
        if !finn(rot).iter().any(|u| matches!(u, Uhell::EndretEtterSkriving)) { return; }
        if let Ok(mut b) = std::fs::read(part) {
            if let Some(x) = b.get_mut(0) { *x ^= 0xff; }
            let _ = std::fs::write(part, b);
        }
    }
    fn disk_full() -> io::Error {
        // Den samme feilen OS-et gir: ERROR_DISK_FULL på Windows, ENOSPC ellers.
        io::Error::from_raw_os_error(if cfg!(windows) { 112 } else { 28 })
    }
}

// ── Tester: falske kort i midlertidige mapper. Ingen ekte kort, ingen filer utenfor temp. ────

#[cfg(test)]
mod tester {
    use super::uhell::{self, Uhell};
    use super::*;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;

    #[derive(Clone, Default)]
    struct Logg(Arc<Mutex<Vec<Framdrift>>>);
    impl Melder for Logg {
        fn framdrift(&self, f: Framdrift) { self.0.lock().unwrap().push(f); }
    }
    impl Logg {
        fn siste(&self, id: &Path) -> Option<String> {
            let id = id.to_string_lossy().to_string();
            self.0.lock().unwrap().iter().rev().find(|f| f.id == id).map(|f| f.status.clone())
        }
    }

    fn stopp() -> Stopp { Stopp { global: Arc::new(AtomicBool::new(false)), sett: Arc::new(Mutex::new(Default::default())), id: String::new() } }

    /// 2020-01-02 03:04:05 UTC: langt fra «nå», så en kopi som fikk dagens tid, faller igjennom.
    fn gammel() -> SystemTime { UNIX_EPOCH + Duration::from_secs(1_577_934_245) }

    fn mappe(navn: &str) -> PathBuf {
        let d = std::env::temp_dir().join("skapflyt-innlesing").join(navn); // fast navn: tømmes ved start, så temp ikke vokser for hver kjøring
        let _ = fs::remove_dir_all(&d);
        fs::create_dir_all(&d).unwrap();
        d
    }

    /// Innhold som er forskjellig for hver `frø`, og som krysser blokkgrensene.
    fn innhold(fro: u8, n: usize) -> Vec<u8> {
        (0..n).map(|i| (i as u32).wrapping_mul(2_654_435_761).rotate_left(fro as u32 % 31 + 1).to_le_bytes()[0] ^ fro).collect()
    }
    fn xxh(b: &[u8]) -> String { format!("{:016x}", xxhash_rust::xxh64::xxh64(b, 0)) }

    /// Et falskt kort: filene skrives med en gammel mtime, og kortet regnes som sitt eget volum.
    fn kort(navn: &str, filer: &[(&str, Vec<u8>)]) -> (PathBuf, Vec<Kildefil>) {
        let rot = mappe(navn);
        uhell::eget_volum(&rot);
        let mut ut = Vec::new();
        for (rel, data) in filer {
            let p = rot.join(rel);
            fs::create_dir_all(p.parent().unwrap()).unwrap();
            fs::write(&p, data).unwrap();
            let f = OpenOptions::new().write(true).open(&p).unwrap();
            f.set_modified(gammel()).unwrap();
            drop(f);
            ut.push(Kildefil::fra_sti(&p, *rel).unwrap());
        }
        (rot, ut)
    }

    /// Alle filer under `rot` (relativ sti, bytes, mtime), uten kvitteringene.
    fn innhold_i(rot: &Path) -> Vec<(String, u64, SystemTime)> {
        let mut ut = Vec::new();
        let mut stakk = vec![rot.to_path_buf()];
        while let Some(d) = stakk.pop() {
            for e in fs::read_dir(&d).unwrap().flatten() {
                let p = e.path();
                if p.file_name().is_some_and(|n| n == ".rawskap-innlesing") { continue; }
                let md = e.metadata().unwrap();
                if md.is_dir() { stakk.push(p); continue; }
                ut.push((p.strip_prefix(rot).unwrap().to_string_lossy().replace('\\', "/"), md.len(), md.modified().unwrap()));
            }
        }
        ut.sort_by(|a, b| a.0.cmp(&b.0));
        ut
    }
    fn parts(rot: &Path) -> Vec<String> {
        let mut ut = Vec::new();
        let mut stakk = vec![rot.to_path_buf()];
        while let Some(d) = stakk.pop() {
            for e in fs::read_dir(&d).unwrap().flatten() {
                let p = e.path();
                if p.is_dir() { stakk.push(p); } else if p.to_string_lossy().ends_with(".part") { ut.push(p.to_string_lossy().to_string()); }
            }
        }
        ut
    }
    fn kjor(filer: &[Kildefil], maal: &[&PathBuf]) -> Rapport {
        let valg = Valg::ny(maal.iter().map(|p| p.to_path_buf()).collect(), "skapflyt-test");
        last_inn(filer, &valg, &Logg::default(), &stopp()).unwrap()
    }
    fn utfall(r: &Rapport) -> Vec<Vec<Utfall>> {
        r.filer.iter().map(|f| f.kopier.iter().map(|k| k.utfall.clone()).collect()).collect()
    }

    #[test]
    fn kopierer_kontrollerer_og_skriver_kvittering() {
        let a = innhold(1, (5 << 20) + 123); // over én blokk
        let j = innhold(2, 70_000);
        let (rot, filer) = kort("grunn-kort", &[("DCIM/100MSDCF/DSC0001.ARW", a.clone()), ("DCIM/100MSDCF/DSC0001.JPG", j.clone()), ("DCIM/100MSDCF/TOM.XMP", Vec::new())]);
        let foer = innhold_i(&rot);
        let arkiv = mappe("grunn-arkiv");
        let logg = Logg::default();
        let r = last_inn(&filer, &Valg::ny(vec![arkiv.clone()], "skapflyt-test"), &logg, &stopp()).unwrap();

        assert_eq!(utfall(&r), vec![vec![Utfall::Kopiert]; 3]);
        assert!(r.trygt_aa_formatere, "{:?}", r.ikke_trygt);
        for (f, data) in r.filer.iter().zip([&a, &j, &Vec::new()]) {
            let k = &f.kopier[0];
            assert_eq!(f.xxh64.as_deref(), Some(xxh(data).as_str()));
            assert_eq!(&fs::read(k.sti.as_ref().unwrap()).unwrap(), data, "kopien er lik kortet");
            assert_eq!(k.sti.as_ref().unwrap(), &arkiv.join(&f.relativ), "som på kortet");
            assert_eq!(logg.siste(&f.kilde).as_deref(), Some("verifisert"));
            #[cfg(windows)]
            assert_eq!(k.kontroll, Some(Kontroll::ForbiBufferen), "NTFS i temp tar ubuffret lesing");
            #[cfg(all(target_os = "linux", target_pointer_width = "64"))]
            assert_eq!(k.kontroll, Some(Kontroll::BedtForbiBufferen));
        }
        assert!(parts(&arkiv).is_empty(), "ingen .part igjen: {:?}", parts(&arkiv));
        assert_eq!(innhold_i(&rot), foer, "kortet er urørt");

        // Kvitteringen: én fil i målet, med fil, størrelse, xxh64 og mål.
        assert_eq!(r.manifest.len(), 1);
        assert!(r.manifest[0].starts_with(arkiv.join(".rawskap-innlesing")));
        let m: serde_json::Value = serde_json::from_slice(&fs::read(&r.manifest[0]).unwrap()).unwrap();
        assert_eq!(m["trygt_aa_formatere"], true);
        assert_eq!(m["app"], "skapflyt-test");
        assert_eq!(m["filer"].as_array().unwrap().len(), 3);
        assert_eq!(m["filer"][0]["relativ"], "DCIM/100MSDCF/DSC0001.ARW");
        assert_eq!(m["filer"][0]["bytes"], a.len() as u64);
        assert_eq!(m["filer"][0]["xxh64"], xxh(&a));
        assert_eq!(m["filer"][0]["kopier"][0]["sti"], "DCIM/100MSDCF/DSC0001.ARW");
        assert_eq!(m["filer"][0]["kopier"][0]["utfall"], "kopiert");
    }

    /// F2: skapet bruker filas tid som opptaksdato for video, så kopien må ha kortets tid.
    #[test]
    fn mtime_folger_med() {
        let (_rot, filer) = kort("mtime-kort", &[("DCIM/100MSDCF/C0001.MP4", innhold(3, 300_000))]);
        let arkiv = mappe("mtime-arkiv");
        let r = kjor(&filer, &[&arkiv]);
        assert!(r.trygt_aa_formatere);
        let t = fs::metadata(r.filer[0].kopier[0].sti.as_ref().unwrap()).unwrap().modified().unwrap();
        assert!(avstand(t, gammel()) < Duration::from_secs(2), "kopien har {t:?}, kortet {:?}", gammel());
    }

    /// To kameraer med hver sin DSC0001.ARW (samme størrelse, ukomprimert ARW): den andre får -1,
    /// den første står urørt, og en ny runde med hvert kort gir Fantes.
    #[test]
    fn like_navn_fra_to_kameraer() {
        let rel = "DCIM/100MSDCF/DSC0001.ARW";
        let (a_data, b_data, c_data) = (innhold(4, 1 << 20), innhold(5, 1 << 20), innhold(6, 1 << 20));
        let (_ra, a) = kort("kamera-a", &[(rel, a_data.clone())]);
        let (_rb, b) = kort("kamera-b", &[(rel, b_data.clone())]);
        let (_rc, c) = kort("kamera-c", &[(rel, c_data.clone())]);
        let arkiv = mappe("kamera-arkiv");
        let dir = arkiv.join("DCIM/100MSDCF");

        assert_eq!(utfall(&kjor(&a, &[&arkiv])), vec![vec![Utfall::Kopiert]]);
        let rb = kjor(&b, &[&arkiv]);
        assert_eq!(utfall(&rb), vec![vec![Utfall::Kopiert]]);
        assert_eq!(rb.filer[0].kopier[0].sti.as_ref().unwrap(), &dir.join("DSC0001-1.ARW"));
        assert!(rb.trygt_aa_formatere);
        assert_eq!(kjor(&c, &[&arkiv]).filer[0].kopier[0].sti.as_ref().unwrap(), &dir.join("DSC0001-2.ARW"));
        assert_eq!(fs::read(dir.join("DSC0001.ARW")).unwrap(), a_data, "den første er ikke overskrevet");
        assert_eq!(fs::read(dir.join("DSC0001-1.ARW")).unwrap(), b_data);
        assert_eq!(fs::read(dir.join("DSC0001-2.ARW")).unwrap(), c_data);

        let foer = innhold_i(&arkiv);
        for (kortet, navn) in [(&b, "DSC0001-1.ARW"), (&a, "DSC0001.ARW"), (&c, "DSC0001-2.ARW")] {
            let r = kjor(kortet, &[&arkiv]);
            assert_eq!(utfall(&r), vec![vec![Utfall::Fantes]], "{navn}");
            assert_eq!(r.filer[0].kopier[0].sti.as_ref().unwrap(), &dir.join(navn));
            assert!(r.trygt_aa_formatere);
        }
        assert_eq!(innhold_i(&arkiv), foer, "ingenting nytt i arkivet");
        assert!(parts(&arkiv).is_empty());
    }

    /// Samme kort to ganger: Fantes, og ingenting skrives (ingen blokk når målet, ingen fil endres).
    #[test]
    fn samme_fil_to_ganger_gir_fantes_uten_aa_skrive() {
        let (_rot, filer) = kort("igjen-kort", &[("DCIM/100CANON/IMG_0001.CR3", innhold(7, 2 << 20)), ("DCIM/100CANON/IMG_0001.JPG", innhold(8, 90_000))]);
        let arkiv = mappe("igjen-arkiv");
        assert!(kjor(&filer, &[&arkiv]).trygt_aa_formatere);
        let foer = innhold_i(&arkiv);
        let skrevet = Arc::new(AtomicU64::new(0));
        let s2 = skrevet.clone();
        uhell::sett(&arkiv, Uhell::Underveis(Arc::new(move |n| { s2.fetch_add(n, Ordering::SeqCst); })));
        let r = kjor(&filer, &[&arkiv]);
        uhell::fjern(&arkiv);
        assert_eq!(utfall(&r), vec![vec![Utfall::Fantes]; 2]);
        assert!(r.trygt_aa_formatere);
        assert_eq!(skrevet.load(Ordering::SeqCst), 0, "ingenting skal skrives når fila fantes");
        assert_eq!(innhold_i(&arkiv), foer);
        assert_eq!(fs::read_dir(arkiv.join(".rawskap-innlesing")).unwrap().count(), 2, "én kvittering per runde");
    }

    #[test]
    fn reservekopi_i_to_maal() {
        let (_rot, filer) = kort("reserve-kort", &[("DCIM/100MSDCF/DSC0001.ARW", innhold(9, 3 << 20)), ("DCIM/100MSDCF/DSC0002.ARW", innhold(10, 3 << 20))]);
        let (arkiv, reserve) = (mappe("reserve-arkiv"), mappe("reserve-reserve"));
        let r = kjor(&filer, &[&arkiv, &reserve]);
        assert_eq!(utfall(&r), vec![vec![Utfall::Kopiert, Utfall::Kopiert]; 2]);
        assert!(r.trygt_aa_formatere);
        assert_eq!(r.manifest.len(), 2);
        assert!(r.manifest[1].starts_with(&reserve));
        assert_eq!(innhold_i(&arkiv).iter().map(|x| (&x.0, x.1)).collect::<Vec<_>>(), innhold_i(&reserve).iter().map(|x| (&x.0, x.1)).collect::<Vec<_>>());

        // Reservekopien mister en fil: neste runde kopierer bare den, og arkivet sier Fantes.
        fs::remove_file(reserve.join("DCIM/100MSDCF/DSC0002.ARW")).unwrap();
        let r = kjor(&filer, &[&arkiv, &reserve]);
        assert_eq!(utfall(&r), vec![vec![Utfall::Fantes, Utfall::Fantes], vec![Utfall::Fantes, Utfall::Kopiert]]);
        assert!(r.trygt_aa_formatere);

        // Samme mappe to ganger er ikke to kopier.
        let e = last_inn(&filer, &Valg::ny(vec![arkiv.clone(), arkiv.join(".")], "t"), &Logg::default(), &stopp()).err();
        assert!(e.is_some_and(|e| e.contains("samme mappe")));
    }

    /// Full disk på reservekopien: arkivet blir kopiert, reservekopien feiler og ryddes, og det er
    /// aldri trygt. Neste runde med plass fullfører uten å røre arkivet.
    #[test]
    fn full_disk_gir_feil_og_aldri_trygt() {
        let (_rot, filer) = kort("full-kort", &[("DCIM/100MSDCF/DSC0001.ARW", innhold(11, 3 << 20)), ("DCIM/100MSDCF/DSC0002.ARW", innhold(12, 3 << 20))]);
        let (arkiv, reserve) = (mappe("full-arkiv"), mappe("full-reserve"));
        uhell::sett(&reserve, Uhell::FullDisk { etter: 1 << 20 });
        let logg = Logg::default();
        let r = last_inn(&filer, &Valg::ny(vec![arkiv.clone(), reserve.clone()], "t"), &logg, &stopp()).unwrap();
        uhell::fjern(&reserve);
        assert!(!r.trygt_aa_formatere);
        for f in &r.filer {
            assert_eq!(f.kopier[0].utfall, Utfall::Kopiert);
            assert!(matches!(&f.kopier[1].utfall, Utfall::Feil(e) if e.contains("Fikk ikke skrevet")), "{:?}", f.kopier[1].utfall);
            assert!(f.kopier[1].sti.is_none());
            assert_eq!(logg.siste(&f.kilde).as_deref(), Some("feil"));
        }
        assert!(innhold_i(&reserve).is_empty(), "ingen halve filer: {:?}", innhold_i(&reserve));
        assert!(parts(&reserve).is_empty());
        let m: serde_json::Value = serde_json::from_slice(&fs::read(&r.manifest[0]).unwrap()).unwrap();
        assert_eq!(m["trygt_aa_formatere"], false);
        assert_eq!(m["filer"][0]["kopier"][1]["utfall"], "feil");

        let r = kjor(&filer, &[&arkiv, &reserve]);
        assert_eq!(utfall(&r), vec![vec![Utfall::Fantes, Utfall::Kopiert]; 2]);
        assert!(r.trygt_aa_formatere);
    }

    /// En kopi som endres etter skriving (disken tok ikke vare på den) fanges av kontrollen.
    #[test]
    fn endret_etter_skriving_gir_feil_og_aldri_trygt() {
        let (_rot, filer) = kort("endret-kort", &[("DCIM/100MSDCF/DSC0001.ARW", innhold(13, 2 << 20))]);
        let arkiv = mappe("endret-arkiv");
        uhell::sett(&arkiv, Uhell::EndretEtterSkriving);
        let r = kjor(&filer, &[&arkiv]);
        uhell::fjern(&arkiv);
        assert!(!r.trygt_aa_formatere);
        assert!(matches!(&r.filer[0].kopier[0].utfall, Utfall::Feil(e) if e.contains("ikke lik kortet")), "{:?}", r.filer[0].kopier[0].utfall);
        assert!(innhold_i(&arkiv).is_empty(), "den dårlige kopien er slettet");
        assert!(parts(&arkiv).is_empty());
        let r = kjor(&filer, &[&arkiv]);
        assert_eq!(utfall(&r), vec![vec![Utfall::Kopiert]]);
        assert!(r.trygt_aa_formatere);
    }

    /// Avbrudd midt i en fil: ingen halv fil under det endelige navnet, ingen .part, og ikke trygt.
    /// Etterpå fullfører en ny runde.
    #[test]
    fn avbrudd_midt_i_en_fil() {
        let stor = innhold(14, 10 << 20); // tre blokker
        let (_rot, filer) = kort("avbrudd-kort", &[("DCIM/100MSDCF/C0001.MP4", stor.clone()), ("DCIM/100MSDCF/C0002.MP4", innhold(15, 50_000))]);
        let arkiv = mappe("avbrudd-arkiv");
        let s = stopp();
        let flagg = s.global.clone();
        let sett_ved = Arc::new(AtomicU64::new(0));
        let sv = sett_ved.clone();
        // Dør prosessen akkurat nå, er det dette som ligger igjen: det endelige navnet skal ikke finnes.
        let endelig = arkiv.join("DCIM/100MSDCF/C0001.MP4");
        let halv = Arc::new(AtomicBool::new(false));
        let hv = halv.clone();
        uhell::sett(&arkiv, Uhell::Underveis(Arc::new(move |n| {
            if endelig.exists() { hv.store(true, Ordering::SeqCst); }
            if n >= BLOKK as u64 { sv.store(n, Ordering::SeqCst); flagg.store(true, Ordering::SeqCst); }
        })));
        let r = last_inn(&filer, &Valg::ny(vec![arkiv.clone()], "t"), &Logg::default(), &s).unwrap();
        uhell::fjern(&arkiv);
        let n = sett_ved.load(Ordering::SeqCst);
        assert!(n > 0 && n < stor.len() as u64, "stoppet midt i fila ({n} av {})", stor.len());
        assert!(!halv.load(Ordering::SeqCst), "under skrivingen fantes det endelige navnet: en krasj ville latt en halv fil ligge der");
        assert_eq!(utfall(&r), vec![vec![Utfall::Avbrutt]; 2]);
        assert_eq!(r.filer[0].feil().as_deref(), Some("Avbrutt"));
        assert!(!r.trygt_aa_formatere);
        assert!(!arkiv.join("DCIM/100MSDCF/C0001.MP4").exists(), "ingen halv fil under det endelige navnet");
        assert!(innhold_i(&arkiv).is_empty() && parts(&arkiv).is_empty(), "{:?}", innhold_i(&arkiv));

        s.global.store(false, Ordering::SeqCst);
        let r = last_inn(&filer, &Valg::ny(vec![arkiv.clone()], "t"), &Logg::default(), &s).unwrap();
        assert_eq!(utfall(&r), vec![vec![Utfall::Kopiert]; 2]);
        assert!(r.trygt_aa_formatere);
        assert_eq!(fs::read(arkiv.join("DCIM/100MSDCF/C0001.MP4")).unwrap(), stor);
    }

    /// Én fil krysset ut (Transfers ✕ på en fil): resten kopieres, men kortet er ikke trygt.
    #[test]
    fn enkeltfil_avbrutt_er_ikke_trygt() {
        let (_rot, filer) = kort("enkel-kort", &[("DCIM/A.JPG", innhold(16, 1000)), ("DCIM/B.JPG", innhold(17, 1000))]);
        let arkiv = mappe("enkel-arkiv");
        let s = stopp();
        s.sett.lock().unwrap().insert(filer[1].sti.to_string_lossy().to_string());
        let r = last_inn(&filer, &Valg::ny(vec![arkiv.clone()], "t"), &Logg::default(), &s).unwrap();
        assert_eq!(utfall(&r), vec![vec![Utfall::Kopiert], vec![Utfall::Avbrutt]]);
        assert!(!r.trygt_aa_formatere);
    }

    /// Et mål på samme disk som kilden forsvinner når kortet formateres: kopien er god, men aldri trygt.
    #[test]
    fn maal_paa_samme_disk_som_kilden_er_aldri_trygt() {
        let kilde = mappe("samme-disk-kilde"); // ikke eget volum: ligger på temp-disken, som målet
        let p = kilde.join("A.JPG");
        fs::write(&p, innhold(18, 5000)).unwrap();
        let arkiv = mappe("samme-disk-arkiv");
        let r = kjor(&[Kildefil::fra_sti(&p, "A.JPG").unwrap()], &[&arkiv]);
        assert_eq!(utfall(&r), vec![vec![Utfall::Kopiert]]);
        assert!(!r.trygt_aa_formatere);
        assert!(r.ikke_trygt.as_deref().is_some_and(|g| g.contains("samme disk")), "{:?}", r.ikke_trygt);
    }

    /// Målet er kortet selv: kilden er sin egen «kopi». Den skal ikke telle, og kortet røres ikke.
    #[test]
    fn maalet_er_kortet_selv() {
        let (rot, filer) = kort("selv-kort", &[("DCIM/100MSDCF/DSC0001.ARW", innhold(19, 400_000))]);
        let foer = innhold_i(&rot);
        let r = kjor(&filer, &[&rot]);
        assert!(matches!(&r.filer[0].kopier[0].utfall, Utfall::Feil(e) if e.contains("selve kildefila")), "{:?}", r.filer[0].kopier[0].utfall);
        assert!(!r.trygt_aa_formatere);
        assert_eq!(innhold_i(&rot), foer, "ingen -1-kopi på kortet, og originalen er urørt");
    }

    #[test]
    fn plassering_utenfor_maalet_avvises() {
        let (_rot, filer) = kort("utenfor-kort", &[("DCIM/A.JPG", innhold(20, 100))]);
        let arkiv = mappe("utenfor-arkiv").join("inni");
        for p in ["../ut.jpg", "", "a/../../ut.jpg"] {
            let mut v = Valg::ny(vec![arkiv.clone()], "t");
            let p2 = p.to_string();
            v.plassering = Box::new(move |_| PathBuf::from(&p2));
            let r = last_inn(&filer, &v, &Logg::default(), &stopp()).unwrap();
            assert!(matches!(&r.filer[0].kopier[0].utfall, Utfall::Feil(e) if e.contains("Ugyldig plassering")), "{p}: {:?}", r.filer[0].kopier[0].utfall);
        }
        let mut v = Valg::ny(vec![arkiv.clone()], "t");
        v.plassering = Box::new(|_| std::env::temp_dir().join("skapflyt-absolutt.jpg"));
        assert!(matches!(last_inn(&filer, &v, &Logg::default(), &stopp()).unwrap().filer[0].kopier[0].utfall, Utfall::Feil(_)));
        assert!(!arkiv.parent().unwrap().join("ut.jpg").exists() && !std::env::temp_dir().join("skapflyt-absolutt.jpg").exists());
    }

    /// Flere samtidig, og en plassering fra kalleren som legger alt i én mappe: like navn fra tre
    /// mapper får hvert sitt navn, og to helt like filer blir én kopi (Fantes).
    #[test]
    fn samtidig_med_like_navn_i_en_mappe() {
        let mut liste: Vec<(String, Vec<u8>)> = Vec::new();
        for (i, d) in ["100A", "100B", "100C"].iter().enumerate() {
            for k in 0..4 { liste.push((format!("DCIM/{d}/DSC000{k}.ARW"), innhold(30 + (i * 4 + k) as u8, 200_000 + k * 10))); }
        }
        liste.push(("DCIM/100D/DSC0000.ARW".into(), liste[0].1.clone())); // samme bytes som 100A/DSC0000
        let par: Vec<(&str, Vec<u8>)> = liste.iter().map(|(a, b)| (a.as_str(), b.clone())).collect();
        let (_rot, filer) = kort("samtidig-kort", &par);
        let arkiv = mappe("samtidig-arkiv");
        let mut v = Valg::ny(vec![arkiv.clone()], "t");
        v.samtidig = 4;
        v.plassering = Box::new(|f: &Kildefil| PathBuf::from("2020/01/02").join(f.sti.file_name().unwrap()));
        let r = last_inn(&filer, &v, &Logg::default(), &stopp()).unwrap();
        assert!(r.trygt_aa_formatere, "{:?}", utfall(&r));
        let fantes = r.filer.iter().filter(|f| f.kopier[0].utfall == Utfall::Fantes).count();
        assert_eq!(fantes, 1, "den doble fila");
        let mut navn: Vec<PathBuf> = r.filer.iter().map(|f| f.kopier[0].sti.clone().unwrap()).collect();
        for (f, (_, data)) in r.filer.iter().zip(&liste) {
            assert_eq!(&fs::read(f.kopier[0].sti.as_ref().unwrap()).unwrap(), data, "{}", f.relativ);
        }
        navn.sort();
        navn.dedup();
        assert_eq!(navn.len(), 12, "tolv ulike filer, tolv navn");
        assert_eq!(innhold_i(&arkiv).len(), 12);
        assert!(parts(&arkiv).is_empty());
    }

    #[test]
    fn kortet_endret_storrelse() {
        let (_rot, mut filer) = kort("storrelse-kort", &[("DCIM/A.JPG", innhold(21, 1000))]);
        filer[0].bytes = 999;
        let arkiv = mappe("storrelse-arkiv");
        let r = kjor(&filer, &[&arkiv]);
        assert!(matches!(&r.filer[0].kopier[0].utfall, Utfall::Feil(e) if e.contains("1000 byte")), "{:?}", r.filer[0].kopier[0].utfall);
        assert!(innhold_i(&arkiv).is_empty() && parts(&arkiv).is_empty());
        assert!(!r.trygt_aa_formatere);
    }

    #[test]
    fn ingen_filer_er_ikke_trygt() {
        let arkiv = mappe("tomt-arkiv");
        let r = kjor(&[], &[&arkiv]);
        assert!(!r.trygt_aa_formatere, "ingenting kopiert er ingen grunn til å formatere");
        assert!(last_inn(&[], &Valg::ny(Vec::new(), "t"), &Logg::default(), &stopp()).is_err());
    }

    #[test]
    fn navnene() {
        assert_eq!(navn_nr("DSC0001.ARW", 0), "DSC0001.ARW");
        assert_eq!(navn_nr("DSC0001.ARW", 2), "DSC0001-2.ARW");
        assert_eq!(navn_nr("arkiv.tar.gz", 1), "arkiv.tar-1.gz");
        assert_eq!(navn_nr("UTEN", 1), "UTEN-1");
        assert_eq!(navn_nr(".skjult", 1), ".skjult-1");
        assert_eq!(utc(UNIX_EPOCH + Duration::from_secs(1_577_934_245)), "2020-01-02T03:04:05Z");
        assert_eq!(utc(UNIX_EPOCH + Duration::from_secs(951_782_400)), "2000-02-29T00:00:00Z");
    }

    /// Hva kontrollen koster: les en fil på 1 GiB tilbake vanlig og utenom bufferen.
    ///   SKAPFLYT_MAALE_DIR=D:\\noe cargo test --release -- --ignored kontrollen_koster --nocapture
    #[test]
    #[ignore]
    fn kontrollen_koster() {
        let dir = std::env::var_os("SKAPFLYT_MAALE_DIR").map(PathBuf::from).unwrap_or_else(|| mappe("maaling"));
        let p = dir.join("skapflyt-maaling.bin");
        let data = innhold(42, 64 << 20);
        let mut f = OpenOptions::new().write(true).create_new(true).open(&p).unwrap();
        for _ in 0..16 { f.write_all(&data).unwrap(); }
        f.sync_all().unwrap();
        drop(f);
        let t = Instant::now();
        let mut g = File::open(&p).unwrap();
        let mut buf = vec![0u8; BLOKK];
        let mut h = xxhash_rust::xxh64::Xxh64::new(0);
        loop { let n = fyll(&mut g, &mut buf).unwrap(); if n == 0 { break; } h.update(&buf[..n]); }
        let vanlig = t.elapsed();
        let t = Instant::now();
        let (h2, n, k) = les_kontroll(&p).unwrap();
        let kontroll = t.elapsed();
        assert_eq!(format!("{:016x}", h.digest()), h2);
        let _ = fs::remove_file(&p);
        let mbs = |d: Duration| (n as f64 / 1e6) / d.as_secs_f64();
        println!("1 GiB i {}: vanlig lesing {:.2} s ({:.0} MB/s), kontrollen ({k:?}) {:.2} s ({:.0} MB/s)", dir.display(), vanlig.as_secs_f64(), mbs(vanlig), kontroll.as_secs_f64(), mbs(kontroll));
    }
}
