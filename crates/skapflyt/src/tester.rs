// ─────────────────────────────────────────────────────────────────────────────
// TESTER (17/9). Repoet hadde ingen. Fire feil ble funnet på én ettermiddag, og tre av dem satt i
// logikk som så åpenbart riktig ut ved lesing — så dekningen starter på det som faktisk ØDELEGGER
// DATA hvis det er galt, ikke på det som er lett å teste.
// (S1: nett- og opplastingstestene flyttet hit fra src-tauri/src/lib.rs sammen med koden.)
// (T2: Klient/Feil/Valg, og nye tester for parallelle deler, Kilde, versjonAv, tilstand v1 → v2, proxyKey og uferdige.)
//   cargo test --manifest-path crates/skapflyt/Cargo.toml --all-features
use crate::opplasting::{Fil as OppTil, Kilde};
use crate::testportal::*;
use crate::*;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

/// User-Agent i testene. Den falske portalen ser ikke på den.
const UA: &str = "RawskapTransfer/test";

/// Delene skal dekke fila NØYAKTIG én gang: ingen hull, ingen overlapp, siste del kortere.
/// Kjøres for flere delstørrelser fordi tallet ble endret 17/9 (64 → 16 MB) mens gamle,
/// gjenopptatte opplastinger fortsatt leses med SIN egen — begge må stemme.
#[test]
fn deler_dekker_fila_uten_hull_eller_overlapp() {
    for del in [5u64 << 20, 8 << 20, 16 << 20, 64 << 20] {
        for bytes in [1u64, 100, del - 1, del, del + 1, 3 * del, 3 * del + 7, 65_000_000] {
            let antall = ((bytes + del - 1) / del) as u32;
            let mut dekket = 0u64;
            let mut forrige_slutt = 0u64;
            for nr in 1..=antall {
                let (start, len) = del_omrade(nr, del, bytes);
                assert_eq!(start, forrige_slutt, "hull eller overlapp ved del {nr} (del={del}, bytes={bytes})");
                assert!(len > 0, "tom del {nr} (del={del}, bytes={bytes})");
                assert!(len <= del, "del {nr} er større enn delstørrelsen");
                forrige_slutt = start + len;
                dekket += len;
            }
            assert_eq!(dekket, bytes, "delene dekker ikke hele fila (del={del}, bytes={bytes})");
        }
    }
}

/// Hashen regnes nå bitvis mens bytene sendes, i stedet for i et eget gjennomløp først.
/// Den MÅ gi samme sum — ellers blir hver senere nedlasting en falsk verifiseringsfeil.
#[test]
fn bitvis_hash_er_lik_hash_av_hele() {
    let data: Vec<u8> = (0..300_000u32).map(|i| (i % 251) as u8).collect();
    let mut hel = xxhash_rust::xxh64::Xxh64::new(0);
    hel.update(&data);
    for biter in [1usize, 7, 1024, 65_536, 1 << 20] {
        let mut delvis = xxhash_rust::xxh64::Xxh64::new(0);
        for b in data.chunks(biter) { delvis.update(b); }
        assert_eq!(delvis.digest(), hel.digest(), "bitstørrelse {biter} ga en annen sum");
    }
}

/// Et filnavn fra et kort (eller fra skapet) skal aldri kunne peke ut av målmappa.
#[test]
fn trygt_navn_slipper_ikke_ut_av_mappa() {
    for stygt in ["../hemmelig.arw", "..", "/etc/passwd", "....//x"] {
        let rent = trygt_navn(stygt);
        assert!(!rent.contains('/'), "{stygt} ga {rent}");
        assert!(!rent.contains('\\'), "{stygt} ga {rent}");
        assert_ne!(rent, "..", "{stygt} ga {rent}");
        assert_ne!(rent, ".", "{stygt} ga {rent}");
    }
}

#[test]
fn pausene_vokser_og_har_tak() {
    assert_eq!((0..7).map(pause_enheter).collect::<Vec<_>>(), vec![2, 4, 8, 16, 32, 32, 32]);
}

/// R2 tar maks 10 000 deler. Med fast 16 MB-del var taket ~156 GiB — ett langt 8K-klipp.
#[test]
fn delstorrelsen_holder_seg_under_deltaket() {
    const MIB: u64 = 1 << 20;
    assert_eq!(delstorrelse(1), 16 * MIB);
    assert_eq!(delstorrelse(100 * 1024 * MIB), 16 * MIB, "vanlige filer skal ikke merke noe");
    for bytes in [25 * MIB, 140 * 1024 * MIB, 156 * 1024 * MIB, 200 * 1024 * MIB, 1024 * 1024 * MIB, 5 * 1024 * 1024 * MIB] {
        let del = delstorrelse(bytes);
        assert!(del >= 16 * MIB && del % MIB == 0, "{bytes}: {del}");
        assert!(bytes.div_ceil(del) <= MAKS_DELER, "{bytes} B gir {} deler", bytes.div_ceil(del));
    }
}

/// Bare deler med nøyaktig riktig lengde teller som ferdige; resten sendes på nytt.
#[test]
fn bare_deler_med_riktig_lengde_teller() {
    let del = 16u64 << 20;
    let bytes = 40u64 << 20;
    let fra_lager = vec![
        (3, 8 << 20, "c".to_string()),   // siste del, riktig (kortere) lengde
        (1, del, "a".to_string()),
        (2, 1000, "b".to_string()),      // halv — må sendes igjen
        (4, del, "d".to_string()),       // finnes ikke i en fil på 40 MB
        (1, del, "a".to_string()),       // dobbel
    ];
    assert_eq!(gyldige_deler(&fra_lager, del, bytes), vec![(1, "a".to_string()), (3, "c".to_string())]);
}

/// «Pause» midt i en 32 sekunders ventetid mellom to forsøk skal virke med én gang.
#[test]
fn pausen_mellom_forsok_vaakner_ved_avbryt() {
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    let s = stopp("x");
    s.global.store(true, Ordering::Relaxed);
    let start = std::time::Instant::now();
    rt.block_on(sov(std::time::Duration::from_secs(32), Some(&s)));
    assert!(start.elapsed() < std::time::Duration::from_secs(1), "sov ventet {:?}", start.elapsed());
}

/// En presignert URL er skrive-legitimasjon — den skal aldri ut i en feilmelding.
#[test]
fn nettfeil_viser_aldri_url() {
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    let e = rt.block_on(async { enkel_klient().get("http://127.0.0.1:9/r2/x?X-Amz-Signature=hemmelig").send().await }).unwrap_err();
    let tekst = nettfeil(e);
    assert!(!tekst.contains("hemmelig") && !tekst.contains("127.0.0.1"), "{tekst}");
}

/// Feilene gir de samme tekstene som før de ble typet — grensesnittet viser dem og kjenner igjen DELING_LAAST.
#[test]
fn feiltekstene_er_som_foer() {
    assert_eq!(Feil::Avbrutt.to_string(), "Avbrutt");
    assert_eq!(Feil::DelingLaast(String::new()).to_string(), "DELING_LAAST");
    assert_eq!(Feil::DelingLaast("passord".into()).to_string(), "DELING_LAAST:passord");
    assert_eq!(String::from(Feil::server(404, "Portalen svarte 404")), "Portalen svarte 404");
    // varig() = den gamle varig_nedlastingsfeil for alt nedlastingen gir.
    for (e, varig) in [
        (Feil::DelingLaast(String::new()), true),
        (Feil::IkkeTilgang("Nøkkelen er ugyldig — lag en ny".into()), true),
        (Feil::Sperret("Blokkert (samtykke trukket/utløpt)".into()), true),
        (Feil::server(404, "Portalen svarte 404"), true),
        (Feil::server(429, "Portalen svarte 429"), true),
        (Feil::server(502, "Portalen svarte 502"), false),
        (Feil::server(0, "302 uten Location"), false),
        (Feil::Nett("Lageret svarte 403".into()), false),
        (Feil::Disk("ingen plass".into()), false),
    ] { assert_eq!(e.varig(), varig, "{e}"); }
    assert!(!Feil::VersjonAv { kode: "midlertidig".into(), ryddet: false, id: None, tekst: String::new() }.varig());
    assert!(Feil::VersjonAv { kode: "slettet".into(), ryddet: true, id: None, tekst: String::new() }.varig());
}

// ── MOT DEN FALSKE PORTALEN (testportal) ──

fn testdata(n: usize, frø: u64) -> Vec<u8> {
    let mut x = frø.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
    (0..n).map(|_| { x ^= x << 13; x ^= x >> 7; x ^= x << 17; (x >> 24) as u8 }).collect()
}
fn testfil(navn: &str, data: &[u8]) -> PathBuf {
    let d = std::env::temp_dir().join("rawskap-transfer-test").join("filer");
    std::fs::create_dir_all(&d).unwrap();
    let p = d.join(navn);
    std::fs::write(&p, data).unwrap();
    p
}
fn oppfil(p: &Path) -> OppFil { OppFil { sti: p.to_string_lossy().to_string(), relativ: p.file_name().unwrap().to_string_lossy().to_string(), bytes: std::fs::metadata(p).unwrap().len() } }
fn stopp(id: &str) -> Stopp { Stopp { global: Arc::new(AtomicBool::new(false)), sett: Arc::new(std::sync::Mutex::new(Default::default())), id: id.into() } }
fn hash(d: &[u8]) -> String { format!("{:016x}", xxhash_rust::xxh64::xxh64(d, 0)) }
fn mtime_ms(sti: &Path) -> u64 { std::fs::metadata(sti).unwrap().modified().unwrap().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64 }
/// Hver test har sin egen tilstandsmappe (testene kjører samtidig, og `uferdige` ser på hele mappa).
fn valg(navn: &str) -> Valg {
    let d = std::env::temp_dir().join("rawskap-transfer-test").join("tilstand").join(navn);
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    Valg { tilstand_dir: d, ..Valg::default() }
}
fn klient(m: &Mock) -> Klient { Klient::ny(&Konto { portal: m.adr.clone(), nokkel: "test".into() }, UA).unwrap() }
/// Tilstandsfila (v2) for en fil på disk.
fn tilstand_for(v: &Valg, p: &Path, mappe: &str) -> PathBuf {
    tilstand_sti(&v.tilstand_dir, &p.to_string_lossy(), std::fs::metadata(p).unwrap().len(), mtime_ms(p), mappe)
}
fn nokler(b: &serde_json::Value) -> Vec<String> { let mut k: Vec<String> = b.as_object().unwrap().keys().cloned().collect(); k.sort(); k }

/// Stand-in for appen: framdrift går ingen steder, og etterarbeidet gjør ingenting (testfilene er
/// ikke video).
#[derive(Clone)]
struct Stille;
impl Melder for Stille {
    fn framdrift(&self, _f: Framdrift) {}
}
impl Etterarbeid for Stille {
    fn avspillingskopi(&self, _bare: &reqwest::Client, _portal: &str, _sti: &str, _navn: &str, _proxy_put: &str, _bytes: u64) -> impl std::future::Future<Output = Option<u64>> + Send { async { None } }
    fn ulesbar_video(&self, _sti: &str, _navn: &str) -> impl std::future::Future<Output = bool> + Send { async { false } }
    fn videofelt(&self, _sti: &str, _navn: &str, _bytes: u64, _body: &mut serde_json::Value) -> impl std::future::Future<Output = ()> + Send { async {} }
}

/// En app som laster en ferdig avspillingskopi til adressen serveren tilbød — som Transfer gjør etter ffmpeg.
#[derive(Clone)]
struct MedProxy(PathBuf);
impl Melder for MedProxy {
    fn framdrift(&self, _f: Framdrift) {}
}
impl Etterarbeid for MedProxy {
    fn avspillingskopi(&self, bare: &reqwest::Client, portal: &str, _sti: &str, _navn: &str, proxy_put: &str, _bytes: u64) -> impl std::future::Future<Output = Option<u64>> + Send {
        let (b, p, u, f) = (bare.clone(), portal.to_string(), proxy_put.to_string(), self.0.clone());
        async move { if u.is_empty() { return None; } put_proxy(&b, &p, &u, &f).await }
    }
    fn ulesbar_video(&self, _sti: &str, _navn: &str) -> impl std::future::Future<Output = bool> + Send { async { false } }
    fn videofelt(&self, _sti: &str, _navn: &str, _bytes: u64, _body: &mut serde_json::Value) -> impl std::future::Future<Output = ()> + Send { async {} }
}

/// Bytene fra minnet — som rawMotions tar-arkiv, bare uten filer.
struct Minne(Vec<u8>);
impl Kilde for Minne {
    fn lengde(&self) -> u64 { self.0.len() as u64 }
    fn les(&self, start: u64, buf: &mut [u8]) -> std::io::Result<()> {
        let s = start as usize;
        buf.copy_from_slice(&self.0[s..s + buf.len()]);
        Ok(())
    }
}

/// Kjører en asynkron test med korte pauser.
fn med_app<F, Fut>(f: F) where F: FnOnce(Stille) -> Fut, Fut: std::future::Future<Output = ()> {
    TIDSENHET_MS.store(20, Ordering::Relaxed);
    tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap().block_on(f(Stille));
}

/// Tre uhell i én opplasting: forbindelsen kuttes midt i del 1, del 2 får 500, og del 3 sin
/// signatur har gått ut (403). Fila skal likevel komme fram bit for bit lik, med riktig sjekksum.
#[test]
fn multipart_taaler_kutt_500_og_utlopt_signatur() {
    med_app(|app| async move {
        let m = start_mock().await;
        let data = testdata(40 << 20, 1);
        let p = testfil("mp-uhell.bin", &data);
        {
            let mut l = m.lager.lock().unwrap();
            l.uhell.insert((1, 1), Uhell::Kutt(1 << 20));
            l.uhell.insert((2, 1), Uhell::Status(500));
            l.uhell.insert((3, 1), Uhell::Status(403));
        }
        let v = valg("mp-uhell");
        let res = last_opp(&app, &klient(&m), &OppTil::fra_sti(&p, ""), &v, &stopp("x")).await.unwrap();
        assert!(res.id.starts_with("rec") && !res.allerede, "{res:?}");
        assert_eq!(res.xxh64.as_deref(), Some(hash(&data).as_str()));
        let l = m.lager.lock().unwrap();
        assert!(l.objekt.as_deref() == Some(&data[..]), "fila som ble satt sammen er ikke lik originalen");
        assert_eq!((l.forsok(1), l.forsok(2), l.forsok(3)), (2, 2, 2));
        assert!(l.signeringer >= 2, "del 3 skulle fått ny signatur etter 403");
        let fullfor = l.handlinger("fullfor");
        assert_eq!(fullfor.len(), 1);
        assert_eq!(fullfor[0]["xxh64"].as_str(), Some(hash(&data).as_str()));
        assert_eq!(l.handlinger("multipart-start")[0]["delBytes"].as_u64(), Some(16 << 20));
        assert!(!tilstand_for(&v, &p, "").exists(), "tilstanden skal ryddes når fila er i skapet");
    });
}

/// Det Transfer sender, er det samme som før T2: ingen nye nøkler når de nye feltene står tomme.
#[test]
fn transfer_sender_samme_json_som_foer() {
    med_app(|app| async move {
        let m = start_mock().await;
        let (liten, stor) = (testdata(1 << 20, 11), testdata(30 << 20, 12));
        let (pl, ps) = (testfil("json-liten.bin", &liten), testfil("json-stor.dat", &stor));
        let (k, v) = (klient(&m), valg("json"));
        last_opp(&app, &k, &OppTil::fra_sti(&pl, ""), &v, &stopp("x")).await.unwrap();
        last_opp(&app, &k, &OppTil::fra_sti(&ps, "m1"), &v, &stopp("x")).await.unwrap();
        let l = m.lager.lock().unwrap();
        assert_eq!(nokler(&l.handlinger("presign")[0]), ["action", "filnavn", "filstorrelse", "mappeId", "mimeType", "sistEndret"]);
        assert_eq!(nokler(&l.handlinger("multipart-start")[0]), ["action", "delBytes", "filnavn", "filstorrelse", "mappeId", "mimeType", "sistEndret"]);
        for f in l.handlinger("fullfor") {
            assert_eq!(nokler(&f), ["action", "filnavn", "filstorrelse", "mappeId", "mimeType", "originalKey", "sistEndret", "xxh64"]);
        }
        assert_eq!(l.handlinger("fullfor")[1]["sistEndret"].as_u64(), Some(mtime_ms(&ps)), "sistEndret er filas mtime, som før");
    });
}

/// Feltene rawMotion trenger: tags (presign/multipart-start og fullfør), opptaksdatoen som sistEndret, og ekstra felt
/// til fullfør — som aldri overstyrer det craten selv setter.
#[test]
fn tags_opptaksdato_og_felt_sendes() {
    med_app(|app| async move {
        let m = start_mock().await;
        let data = testdata(1 << 20, 13);
        let p = testfil("felt.bin", &data);
        let mut fil = OppTil::fra_sti(&p, "m1");
        fil.tags = vec!["timelapse".into(), "Tromsø".into()];
        fil.sist_endret_ms = Some(1_700_000_000_000);
        fil.felt.insert("rawmotion".into(), serde_json::json!({ "bilder": 300 }));
        fil.felt.insert("filnavn".into(), serde_json::json!("skal-ikke-vinne.bin"));
        last_opp(&app, &klient(&m), &fil, &valg("felt"), &stopp("x")).await.unwrap();
        let l = m.lager.lock().unwrap();
        let (presign, fullfor) = (&l.handlinger("presign")[0], &l.handlinger("fullfor")[0]);
        assert_eq!(presign["tags"], serde_json::json!(["timelapse", "Tromsø"]));
        assert_eq!(fullfor["tags"], serde_json::json!(["timelapse", "Tromsø"]));
        assert_eq!((presign["sistEndret"].as_u64(), fullfor["sistEndret"].as_u64()), (Some(1_700_000_000_000), Some(1_700_000_000_000)));
        assert_eq!(fullfor["rawmotion"]["bilder"], 300);
        assert_eq!(fullfor["filnavn"], "felt.bin", "ekstra felt overstyrer ikke cratens egne");
    });
}

/// Gjenopptak spør lageret, ikke bare resume-fila. Lokal bokføring er tom, men lageret har del 1
/// (hel) og del 2 (halv). Del 1 skal IKKE sendes igjen; del 2 skal det.
#[test]
fn gjenopptak_bruker_lagerets_deler() {
    med_app(|app| async move {
        let m = start_mock().await;
        let data = testdata(40 << 20, 2);
        let p = testfil("mp-gjenoppta.bin", &data);
        let del = 16u64 << 20;
        {
            let mut l = m.lager.lock().unwrap();
            l.deler.insert(1, data[..del as usize].to_vec());
            l.deler.insert(2, data[del as usize..del as usize + 1000].to_vec());
            l.del_bytes = del;
            l.paabegynt("originals/test.bin", "u9");
        }
        let v = valg("gjenoppta");
        let rsti = tilstand_for(&v, &p, "");
        resume_skriv(&rsti, &Resume { versjon: 2, sti: p.to_string_lossy().into(), upload_id: "u9".into(), original_key: "originals/test.bin".into(), bytes: data.len() as u64, sist_endret: mtime_ms(&p), del_bytes: del, ..Default::default() });
        let res = last_opp(&app, &klient(&m), &OppTil::fra_sti(&p, ""), &v, &stopp("x")).await.unwrap();
        let l = m.lager.lock().unwrap();
        assert!(l.objekt.as_deref() == Some(&data[..]), "fila som ble satt sammen er ikke lik originalen");
        assert_eq!((l.forsok(1), l.forsok(2), l.forsok(3)), (0, 1, 1), "del 1 lå der alt; del 2 var halv");
        assert!(l.handlinger("multipart-start").is_empty(), "skulle fortsatt den gamle opplastingen");
        assert_eq!(l.handlinger("multipart-status").len(), 1);
        // Gjenopptatt = vi har ikke sett hele fila i denne kjøringen, og da sendes ingen sjekksum.
        assert!(l.handlinger("fullfor")[0].get("xxh64").is_none());
        assert_eq!(res.xxh64, None);
        assert!(!rsti.exists(), "resume-fila skal ryddes når fila er oppe");
    });
}

/// ⚠ v1 (før T2) må overleve oppdateringen: en tilstandsfil under den gamle nøkkelen (uten mappe), UTEN `del_bytes`
/// (= 64 MiB-deler), skal fortsettes med den gamle delstørrelsen og flyttes til v2.
#[test]
fn v1_tilstand_gjenopptas() {
    med_app(|app| async move {
        let m = start_mock().await;
        let gammel = 64usize << 20;
        let data = testdata(gammel + (6 << 20), 21);
        let p = testfil("mp-v1.bin", &data);
        {
            let mut l = m.lager.lock().unwrap();
            l.deler.insert(1, data[..gammel].to_vec());
            l.del_bytes = 0; // pending-rader fra før delstørrelsen ble lagret
            l.paabegynt("originals/test.bin", "u7");
        }
        let v = valg("v1");
        let sti = p.to_string_lossy().to_string();
        let v1 = tilstand_sti_v1(&v.tilstand_dir, &sti, data.len() as u64, mtime_ms(&p));
        // Nøyaktig det Transfer 0.3.x skrev (før del_bytes fantes).
        std::fs::write(&v1, format!(r#"{{"upload_id":"u7","original_key":"originals/test.bin","mappe_id":"","bytes":{},"deler":[[1,"e1-{gammel}"]],"proxy_put":""}}"#, data.len())).unwrap();
        last_opp(&app, &klient(&m), &OppTil::fra_sti(&p, ""), &v, &stopp("x")).await.unwrap();
        let l = m.lager.lock().unwrap();
        assert!(l.objekt.as_deref() == Some(&data[..]), "fila som ble satt sammen er ikke lik originalen");
        assert_eq!((l.forsok(1), l.forsok(2)), (0, 1), "del 1 (64 MiB) lå der; bare resten skal sendes");
        assert!(l.handlinger("multipart-start").is_empty(), "skulle fortsatt v1-opplastingen");
        assert_eq!(l.deler.get(&2).map(|d| d.len()), Some(6 << 20), "del 2 skal starte ved 64 MiB");
        assert!(!v1.exists(), "v1-fila skal være flyttet");
        assert!(!tilstand_for(&v, &p, "").exists(), "og v2-fila ryddet når fila er oppe");
    });
}

/// En resume-fil som peker på en opplasting portalen ikke kjenner lenger (feiekosten ryddet den),
/// skal gi en ny opplasting — ikke en fil som står fast.
#[test]
fn glemt_opplasting_begynner_paa_nytt() {
    med_app(|app| async move {
        let m = start_mock().await;
        let data = testdata(30 << 20, 3);
        let p = testfil("mp-glemt.bin", &data);
        let v = valg("glemt");
        resume_skriv(&tilstand_for(&v, &p, ""), &Resume { versjon: 2, sti: p.to_string_lossy().into(), upload_id: "gammel".into(), original_key: "originals/gammel.bin".into(), bytes: data.len() as u64, deler: vec![(1, "x".into())], del_bytes: 16 << 20, ..Default::default() });
        last_opp(&app, &klient(&m), &OppTil::fra_sti(&p, ""), &v, &stopp("x")).await.unwrap();
        let l = m.lager.lock().unwrap();
        assert!(l.objekt.as_deref() == Some(&data[..]), "fila som ble satt sammen er ikke lik originalen");
        assert_eq!(l.handlinger("multipart-start").len(), 1);
        assert_eq!(l.handlinger("fullfor")[0]["xxh64"].as_str(), Some(hash(&data).as_str()), "ny opplasting leser hele fila — da skal sjekksummen med");
    });
}

/// Enkel PUT (under delgrensa): først en død linje (serveren slutter å lese), så et kutt. Stopp-
/// vakta skal ta den døde linja, og sjekksummen skal beskrive forsøket som lyktes — ikke alle tre.
#[test]
fn enkel_put_taaler_doed_linje_og_kutt() {
    med_app(|app| async move {
        let m = start_mock().await;
        let data = testdata(5 << 20, 4);
        let p = testfil("enkel.bin", &data);
        {
            let mut l = m.lager.lock().unwrap();
            l.uhell.insert((0, 1), Uhell::Stopp(64 << 10));
            l.uhell.insert((0, 2), Uhell::Kutt(1 << 20));
        }
        let start = std::time::Instant::now();
        let v = valg("enkel");
        last_opp(&app, &klient(&m), &OppTil::fra_sti(&p, ""), &v, &stopp("x")).await.unwrap();
        let l = m.lager.lock().unwrap();
        assert_eq!(l.forsok(0), 3);
        assert!(l.objekt.as_deref() == Some(&data[..]), "fila er ikke lik originalen");
        assert_eq!(l.handlinger("fullfor")[0]["xxh64"].as_str(), Some(hash(&data).as_str()));
        assert!(start.elapsed() < std::time::Duration::from_secs(20), "stopp-vakta skal kutte den døde linja raskt ({:?})", start.elapsed());
        assert!(!tilstand_for(&v, &p, "").exists());
    });
}

/// Fire deler i lufta samtidig: fila skal bli bit for bit lik, sjekksummen riktig (delene leses og hashes i
/// rekkefølge av ÉN leser), og hver del sendes én gang.
#[test]
fn parallelle_deler_gir_riktig_fil_og_xxh64() {
    med_app(|app| async move {
        let m = start_mock().await;
        let data = testdata((100 << 20) + 12_345, 31);
        let p = testfil("mp-parallell.bin", &data);
        m.lager.lock().unwrap().put_pause_ms = 60;
        let v = Valg { deler_samtidig: 4, ..valg("parallell") };
        let res = last_opp(&app, &klient(&m), &OppTil::fra_sti(&p, ""), &v, &stopp("x")).await.unwrap();
        let l = m.lager.lock().unwrap();
        assert!(l.objekt.as_deref() == Some(&data[..]), "fila som ble satt sammen er ikke lik originalen");
        assert_eq!(res.xxh64.as_deref(), Some(hash(&data).as_str()));
        assert_eq!(l.handlinger("fullfor")[0]["xxh64"].as_str(), Some(hash(&data).as_str()));
        assert!((1..=7).all(|nr| l.forsok(nr) == 1), "hver del én gang: {:?}", l.forsok);
        assert!(l.maks_samtidige >= 2 && l.maks_samtidige <= 4, "deler i lufta samtidig: {}", l.maks_samtidige);
    });
}

/// Fire deler i lufta, og kjøringen stopper (del 5 avvises). Neste kjøring fortsetter: bare det som mangler sendes, ingen
/// ny multipart — og den kjente sjekksummen sendes selv om bytene ikke er lest i denne kjøringen (F3).
#[test]
fn parallelle_deler_gjenopptas_etter_brudd() {
    med_app(|app| async move {
        let m = start_mock().await;
        let data = testdata(100 << 20, 32);
        let p = testfil("mp-parallell-brudd.bin", &data);
        {
            let mut l = m.lager.lock().unwrap();
            l.put_pause_ms = 30;
            l.uhell.insert((5, 1), Uhell::Status(400));
        }
        let (k, v) = (klient(&m), Valg { deler_samtidig: 4, ..valg("parallell-brudd") });
        let feil = last_opp(&app, &k, &OppTil::fra_sti(&p, ""), &v, &stopp("x")).await.unwrap_err();
        assert!(matches!(&feil, Feil::Nett(t) if t.starts_with("Del 5 av 7 feilet (Lageret svarte 400)")), "{feil:?}");
        let tilstand = resume_les(&tilstand_for(&v, &p, "")).expect("tilstanden skal stå etter et brudd");
        assert!(!tilstand.deler.is_empty() && tilstand.deler.iter().all(|(nr, _)| *nr != 5), "bokføringen: {:?}", tilstand.deler);
        let mut fil = OppTil::fra_sti(&p, "");
        fil.kjent_xxh64 = Some(hash(&data));
        let res = last_opp(&app, &k, &fil, &v, &stopp("x")).await.unwrap();
        let l = m.lager.lock().unwrap();
        assert!(l.objekt.as_deref() == Some(&data[..]), "fila som ble satt sammen er ikke lik originalen");
        assert_eq!(l.handlinger("multipart-start").len(), 1, "andre kjøring fortsetter den samme opplastingen");
        assert!((1..=7).filter(|nr| *nr != 5).all(|nr| l.forsok(nr) == 1), "ingen del sendes to ganger: {:?}", l.forsok);
        assert_eq!(l.forsok(5), 2);
        assert_eq!(l.handlinger("fullfor")[0]["xxh64"].as_str(), Some(hash(&data).as_str()), "den kjente sjekksummen");
        assert_eq!(res.xxh64.as_deref(), Some(hash(&data).as_str()));
    });
}

/// `Kilde`: bytene kommer ikke fra en fil (rawMotions tar-arkiv lages mens det leses). Både i deler (to i lufta) og i én
/// PUT, uten at noe finnes på stien.
#[test]
fn kilde_lastes_opp_uten_fil() {
    med_app(|app| async move {
        let m = start_mock().await;
        let k = klient(&m);
        let ingen = std::env::temp_dir().join("rawskap-transfer-test").join("finnes-ikke");
        let stor = testdata(40 << 20, 41);
        let fil = OppTil { sti: ingen.join("serie.tar"), kilde: Some(Arc::new(Minne(stor.clone()))), navn: "serie.tar".into(), mime: "application/x-tar".into(), mappe_id: "m1".into(), sist_endret_ms: Some(1_690_000_000_000), ..Default::default() };
        let v = Valg { del_grense: 0, deler_samtidig: 2, ..valg("kilde") };
        let res = last_opp(&app, &k, &fil, &v, &stopp("x")).await.unwrap();
        assert_eq!(res.xxh64.as_deref(), Some(hash(&stor).as_str()));
        {
            let l = m.lager.lock().unwrap();
            assert!(l.objekt.as_deref() == Some(&stor[..]), "arkivet som ble satt sammen er ikke likt kilden");
            assert_eq!(l.handlinger("multipart-start")[0]["sistEndret"].as_u64(), Some(1_690_000_000_000));
        }
        let liten = testdata(3 << 20, 42);
        let fil = OppTil { sti: ingen.join("liten.bin"), kilde: Some(Arc::new(Minne(liten.clone()))), navn: "liten.bin".into(), mime: "application/octet-stream".into(), ..Default::default() };
        let res = last_opp(&app, &k, &fil, &valg("kilde-enkel"), &stopp("y")).await.unwrap();
        assert_eq!(res.xxh64.as_deref(), Some(hash(&liten).as_str()));
        let l = m.lager.lock().unwrap();
        assert!(l.objekt.as_deref() == Some(&liten[..]), "kilden i én PUT er ikke lik");
    });
}

/// Ny utgave godtatt: versjonAv går med i forsjekken og fullfør (med utgavenotatet), og roten kommer tilbake.
#[test]
fn versjon_av_ok() {
    med_app(|app| async move {
        let m = start_mock().await;
        let data = testdata(30 << 20, 51);
        let p = testfil("utgave-ok.bin", &data);
        m.lager.lock().unwrap().utgaver.insert("recMaal000000001".into(), TestUtgave::Ok { rot: "recRot0000000001".into() });
        let mut fil = OppTil::fra_sti(&p, "");
        fil.versjon_av = Some("recMaal000000001".into());
        fil.utgavenotat = Some("Ny fremkalling".into());
        let v = valg("utgave-ok");
        let res = last_opp(&app, &klient(&m), &fil, &v, &stopp("x")).await.unwrap();
        assert_eq!(res.versjon_av.as_deref(), Some("recRot0000000001"));
        let l = m.lager.lock().unwrap();
        assert_eq!(l.handlinger("multipart-start")[0]["versjonAv"], "recMaal000000001");
        let f = &l.handlinger("fullfor")[0];
        assert_eq!((f["versjonAv"].as_str(), f["utgavenotat"].as_str()), (Some("recMaal000000001"), Some("Ny fremkalling")));
        assert!(!tilstand_for(&v, &p, "").exists());
    });
}

/// Forsjekken sier nei FØR bytene går: ingen PUT, ingen fullfør.
#[test]
fn versjon_av_forsjekk_stopper_foer_bytene() {
    med_app(|app| async move {
        let m = start_mock().await;
        let p = testfil("utgave-frosset.bin", &testdata(1 << 20, 52));
        m.lager.lock().unwrap().utgaver.insert("recMaal000000002".into(), TestUtgave::Nei("frosset"));
        let mut fil = OppTil::fra_sti(&p, "");
        fil.versjon_av = Some("recMaal000000002".into());
        let feil = last_opp(&app, &klient(&m), &fil, &valg("utgave-frosset"), &stopp("x")).await.unwrap_err();
        assert!(matches!(&feil, Feil::VersjonAv { kode, id: None, .. } if kode == "frosset"), "{feil:?}");
        assert!(feil.varig());
        assert_eq!(feil.to_string(), "Fila ligger i fryseren — tin den før du laster opp en ny versjon");
        let l = m.lager.lock().unwrap();
        assert_eq!(l.forsok(0), 0, "ingen bytes skal ha gått");
        assert!(l.handlinger("fullfor").is_empty());
    });
}

/// Endelig nei ved fullfør der serveren har ryddet (`ryddet: true`): tilstanden slettes, og vi sier ikke avbryt selv.
#[test]
fn versjon_av_nei_med_ryddet_sletter_tilstanden() {
    med_app(|app| async move {
        let m = start_mock().await;
        let p = testfil("utgave-slettet.bin", &testdata(30 << 20, 53));
        m.lager.lock().unwrap().utgaver_fullfor.insert("recMaal000000003".into(), TestUtgave::Nei("slettet"));
        let mut fil = OppTil::fra_sti(&p, "");
        fil.versjon_av = Some("recMaal000000003".into());
        let v = valg("utgave-slettet");
        let feil = last_opp(&app, &klient(&m), &fil, &v, &stopp("x")).await.unwrap_err();
        assert!(matches!(&feil, Feil::VersjonAv { kode, ryddet: true, id: None, .. } if kode == "slettet"), "{feil:?}");
        assert!(!tilstand_for(&v, &p, "").exists(), "serveren har ryddet — da skal tilstanden bort");
        let l = m.lager.lock().unwrap();
        assert!(l.handlinger("avbryt").is_empty(), "serveren ryddet selv");
        assert_eq!(l.ryddet, ["originals/test.bin"]);
    });
}

/// `midlertidig` (503): alt står — tilstanden også. Neste forsøk fullfører SAMME opplasting uten å sende bytene igjen.
#[test]
fn versjon_av_midlertidig_lar_alt_staa() {
    med_app(|app| async move {
        let m = start_mock().await;
        let data = testdata(30 << 20, 54);
        let p = testfil("utgave-midlertidig.bin", &data);
        m.lager.lock().unwrap().utgaver_fullfor.insert("recMaal000000004".into(), TestUtgave::Midlertidig);
        let mut fil = OppTil::fra_sti(&p, "");
        fil.versjon_av = Some("recMaal000000004".into());
        let (k, v) = (klient(&m), valg("utgave-midlertidig"));
        let feil = last_opp(&app, &k, &fil, &v, &stopp("x")).await.unwrap_err();
        assert!(matches!(&feil, Feil::VersjonAv { kode, ryddet: false, .. } if kode == "midlertidig"), "{feil:?}");
        assert!(!feil.varig());
        let t = resume_les(&tilstand_for(&v, &p, "")).expect("tilstanden skal stå");
        assert!(t.satt_sammen && t.xxh64 == hash(&data), "{t:?}");
        let puts: u32 = { let l = m.lager.lock().unwrap(); assert!(l.handlinger("avbryt").is_empty()); l.forsok.values().sum() };
        m.lager.lock().unwrap().utgaver_fullfor.clear();
        let res = last_opp(&app, &k, &fil, &v, &stopp("x")).await.unwrap();
        assert_eq!(res.versjon_av.as_deref(), Some("recMaal000000004"));
        assert_eq!(res.xxh64.as_deref(), Some(hash(&data).as_str()), "sjekksummen fra kjøringen som sendte bytene");
        let l = m.lager.lock().unwrap();
        assert_eq!(l.forsok.values().sum::<u32>(), puts, "ingen bytes på nytt");
        assert_eq!(l.handlinger("multipart-start").len(), 1);
        assert_eq!(l.handlinger("fullfor").len(), 6, "fem forsøk (503 prøves igjen) og så det som lyktes");
        assert!(!tilstand_for(&v, &p, "").exists());
    });
}

/// Endelig nei uten `ryddet` (en portal fra før P1): klienten sier `avbryt` selv og sletter tilstanden. Med
/// `ryddet: false` (ingenting ble fjernet hos serveren) er neiet like endelig, men da er ryddingen serverens sak.
#[test]
fn versjon_av_nei_uten_ryddet_kaller_avbryt() {
    med_app(|app| async move {
        // ryddet: false — ingen avbryt, men tilstanden går bort.
        let m = start_mock().await;
        let p = testfil("utgave-ryddet-false.bin", &testdata(2 << 20, 58));
        {
            let mut l = m.lager.lock().unwrap();
            l.rydding_feiler = true;
            l.utgaver_fullfor.insert("recMaal000000007".into(), TestUtgave::Nei("ukjent"));
        }
        let mut fil = OppTil::fra_sti(&p, "");
        fil.versjon_av = Some("recMaal000000007".into());
        let v = valg("utgave-ryddet-false");
        let feil = last_opp(&app, &klient(&m), &fil, &v, &stopp("x")).await.unwrap_err();
        assert!(matches!(&feil, Feil::VersjonAv { kode, ryddet: false, id: None, .. } if kode == "ukjent"), "{feil:?}");
        assert!(feil.varig());
        assert!(!tilstand_for(&v, &p, "").exists(), "endelig nei — tilstanden skal bort");
        assert!(m.lager.lock().unwrap().handlinger("avbryt").is_empty(), "feltet ryddet finnes — ikke avbryt");

        // Ingen ryddet i svaret (eldre portal): klienten sier avbryt.
        let m = start_mock().await;
        let p = testfil("utgave-eldre.bin", &testdata(2 << 20, 55));
        {
            let mut l = m.lager.lock().unwrap();
            l.eldre_server = true;
            l.utgaver_fullfor.insert("recMaal000000005".into(), TestUtgave::Nei("ukjent"));
        }
        let mut fil = OppTil::fra_sti(&p, "");
        fil.versjon_av = Some("recMaal000000005".into());
        let v = valg("utgave-eldre");
        let feil = last_opp(&app, &klient(&m), &fil, &v, &stopp("x")).await.unwrap_err();
        assert!(matches!(&feil, Feil::VersjonAv { kode, ryddet: true, id: None, .. } if kode == "ukjent"), "{feil:?}");
        assert!(!tilstand_for(&v, &p, "").exists());
        let l = m.lager.lock().unwrap();
        assert_eq!(l.handlinger("avbryt")[0]["originalKeys"], serde_json::json!(["originals/test.bin"]));
        assert_eq!(l.ryddet, ["originals/test.bin"]);
    });
}

/// Feiekosten rakk å lage raden. Står den i en stakk, er jobben gjort (som `allerede`); står den alene, er det et
/// endelig nei med `id` — bytene ER i skapet, så ingen `avbryt`, og tilstanden går bort.
#[test]
fn versjon_av_naar_feiekosten_alt_laget_raden() {
    med_app(|app| async move {
        let m = start_mock().await;
        let k = klient(&m);
        // Alene: nei med id.
        let p = testfil("utgave-feiekost.bin", &testdata(2 << 20, 56));
        m.lager.lock().unwrap().utgaver_fullfor.insert("recMaal000000006".into(), TestUtgave::Nei("slettet"));
        m.lager.lock().unwrap().rader.insert("originals/test.bin".into(), Rad { id: "recFeiekost00001".into(), versjon_av: String::new() });
        let mut fil = OppTil::fra_sti(&p, "");
        fil.versjon_av = Some("recMaal000000006".into());
        let v = valg("utgave-feiekost");
        let feil = last_opp(&app, &k, &fil, &v, &stopp("x")).await.unwrap_err();
        assert!(matches!(&feil, Feil::VersjonAv { ryddet: false, id: Some(i), .. } if i == "recFeiekost00001"), "{feil:?}");
        assert!(!tilstand_for(&v, &p, "").exists());
        assert!(m.lager.lock().unwrap().handlinger("avbryt").is_empty(), "bytene er i skapet — aldri avbryt");
        // I en stakk: vellykket, som allerede.
        let p = testfil("utgave-stakk.dat", &testdata(2 << 20, 57));
        m.lager.lock().unwrap().rader.insert("originals/test.dat".into(), Rad { id: "recFeiekost00002".into(), versjon_av: "recRot0000000002".into() });
        let mut fil = OppTil::fra_sti(&p, "");
        fil.versjon_av = Some("recMaal000000006".into());
        let res = last_opp(&app, &k, &fil, &valg("utgave-stakk"), &stopp("x")).await.unwrap();
        assert_eq!((res.id.as_str(), res.allerede, res.versjon_av.as_deref()), ("recFeiekost00002", true, Some("recRot0000000002")));
    });
}

/// F7: proxy-nøkkelen kommer fra serverens tilbud, ikke fra klientens egen oppskrift (her har serveren en annen
/// konvensjon, og fullfør godtar bare sin egen).
#[test]
fn proxy_key_fra_tilbudet() {
    med_app(|_app| async move {
        let m = start_mock().await;
        m.lager.lock().unwrap().proxy_prefiks = Some("proxy/v2/".into());
        let p = testfil("film.mp4", &testdata(2 << 20, 61));
        let proxy = testfil("film-proxy.mp4", &testdata(20_000, 62));
        last_opp(&MedProxy(proxy), &klient(&m), &OppTil::fra_sti(&p, ""), &valg("proxy"), &stopp("x")).await.unwrap();
        let l = m.lager.lock().unwrap();
        let f = &l.handlinger("fullfor")[0];
        assert_eq!(f["proxyKey"], "proxy/v2/test.mp4", "nøkkelen fra tilbudet, ikke proxy/test.mp4");
        assert_eq!(f["proxyStorrelse"], 20_000);
        assert_eq!(l.proxy_godtatt.as_deref(), Some("proxy/v2/test.mp4"));
        assert_eq!(l.proxy.as_ref().map(|p| p.len()), Some(20_000));
    });
}

// ── F8: proxy-adressen i tilbudet lever 7200 s ──

/// Som MedProxy, men den husker statusene i rekkefølge, melder «proxy» mens kopien lages (som Transfer), og legger
/// plakat og varighet i fullfør (som Transfers videofelt).
#[derive(Clone)]
struct Opptak { proxy: PathBuf, statuser: Arc<std::sync::Mutex<Vec<String>>> }
impl Opptak {
    fn ny(proxy: PathBuf) -> Opptak { Opptak { proxy, statuser: Default::default() } }
    fn statuser(&self) -> Vec<String> { self.statuser.lock().unwrap().clone() }
}
impl Melder for Opptak {
    fn framdrift(&self, f: Framdrift) { self.statuser.lock().unwrap().push(f.status); }
}
impl Etterarbeid for Opptak {
    fn avspillingskopi(&self, bare: &reqwest::Client, portal: &str, sti: &str, _navn: &str, proxy_put: &str, bytes: u64) -> impl std::future::Future<Output = Option<u64>> + Send {
        let (b, p, u, f, app, id) = (bare.clone(), portal.to_string(), proxy_put.to_string(), self.proxy.clone(), self.clone(), sti.to_string());
        async move {
            if u.is_empty() { return None; }
            meld(&app, &id, bytes, bytes, "proxy");
            put_proxy(&b, &p, &u, &f).await
        }
    }
    fn ulesbar_video(&self, _sti: &str, _navn: &str) -> impl std::future::Future<Output = bool> + Send { async { false } }
    fn videofelt(&self, _sti: &str, _navn: &str, _bytes: u64, body: &mut serde_json::Value) -> impl std::future::Future<Output = ()> + Send {
        body["posterBase64"] = serde_json::json!("data:image/jpeg;base64,/9j/");
        body["varighet"] = serde_json::json!(0.1);
        async {}
    }
}

fn naa() -> u64 { std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64 }
const TRE_TIMER_MS: u64 = 3 * 60 * 60 * 1000;
/// Hvor statusen først ble meldt.
fn pos(s: &[String], navn: &str) -> usize { s.iter().position(|x| x == navn).unwrap_or_else(|| panic!("«{navn}» mangler i {s:?}")) }
/// Rekkefølgen på fullfør, lenkingen og PUT-ene av kopien.
fn proxy_rekke(l: &Lager) -> Vec<String> {
    l.handlinger.iter().filter_map(|h| h["action"].as_str()).filter(|a| a.starts_with("proxy") || *a == "fullfor").map(String::from).collect()
}

#[test]
fn proxy_adressens_alder() {
    let n = 1_800_000_000_000u64;
    assert!(proxy_fersk(n, n));
    assert!(proxy_fersk(n - 99 * 60_000, n));
    assert!(!proxy_fersk(n - 101 * 60_000, n));
    assert!(!proxy_fersk(0, n), "0 = ukjent alder (tilstand fra før feltet fantes)");
    assert!(!proxy_fersk(n + TRE_TIMER_MS, n), "langt fram i tid er like ukjent");
    assert!(PROXY_FERSK_MS < 7_200_000, "serveren signerer adressen for 7200 s");
}

/// F8: en fersk adresse (også gjenopptatt samme dag) gir det samme som før — kopien går til tilbudets adresse FØR
/// fullfør, nøkkelen står i fullfør, og ingenting lenkes etterpå. `proxy_tid` skrives når tilbudet tas imot, både ved
/// multipart-start og presign.
#[test]
fn fersk_proxy_adresse_gaar_foer_fullfor() {
    med_app(|_app| async move {
        let m = start_mock().await;
        let data = testdata(40 << 20, 81);
        let p = testfil("fersk.mp4", &data);
        let kopi = testfil("fersk-proxy.mp4", &testdata(20_000, 82));
        m.lager.lock().unwrap().uhell.insert((2, 1), Uhell::Status(400));
        let (k, v) = (klient(&m), valg("proxy-fersk"));
        let foer = naa();
        assert!(last_opp(&Opptak::ny(kopi.clone()), &k, &OppTil::fra_sti(&p, ""), &v, &stopp("x")).await.is_err());
        let t = resume_les(&tilstand_for(&v, &p, "")).expect("tilstanden skal stå etter bruddet");
        assert!(t.proxy_put.ends_with("/r2/proxy?sig=1") && t.proxy_key == "proxy/test.mp4", "{t:?}");
        assert!(t.proxy_tid >= foer && t.proxy_tid <= naa(), "proxy_tid ved multipart-start: {}", t.proxy_tid);
        let app = Opptak::ny(kopi.clone());
        let res = last_opp(&app, &k, &OppTil::fra_sti(&p, ""), &v, &stopp("x")).await.unwrap();
        assert!(!res.allerede);
        {
            let l = m.lager.lock().unwrap();
            assert!(l.objekt.as_deref() == Some(&data[..]), "fila som ble satt sammen er ikke lik originalen");
            assert_eq!(proxy_rekke(&l), ["proxy-put", "fullfor"], "kopien går før fullfør, og ingenting lenkes");
            assert_eq!(l.handlinger("proxy-put")[0]["sti"], "/r2/proxy?sig=1");
            let f = &l.handlinger("fullfor")[0];
            assert_eq!((f["proxyKey"].as_str(), f["proxyStorrelse"].as_u64()), (Some("proxy/test.mp4"), Some(20_000)));
            assert_eq!(l.proxy_godtatt.as_deref(), Some("proxy/test.mp4"));
            assert!(l.lenket.is_empty());
        }
        let s = app.statuser();
        assert!(pos(&s, "proxy") < pos(&s, "fullfor") && s.last().map(String::as_str) == Some("ferdig"), "{s:?}");

        // presign: tidspunktet står i tilstanden når bare fullfør gjenstår («midlertidig» lar alt stå).
        let m = start_mock().await;
        let p = testfil("fersk-liten.mov", &testdata(2 << 20, 83));
        m.lager.lock().unwrap().utgaver_fullfor.insert("recMaal000000081".into(), TestUtgave::Midlertidig);
        let mut fil = OppTil::fra_sti(&p, "");
        fil.versjon_av = Some("recMaal000000081".into());
        let (k, v) = (klient(&m), valg("proxy-fersk-presign"));
        let foer = naa();
        assert!(last_opp(&Opptak::ny(kopi.clone()), &k, &fil, &v, &stopp("x")).await.is_err());
        let t = resume_les(&tilstand_for(&v, &p, "")).expect("tilstanden skal stå");
        assert!(t.satt_sammen && !t.proxy_put.is_empty() && t.proxy_tid >= foer && t.proxy_tid <= naa(), "proxy_tid ved presign: {t:?}");
        m.lager.lock().unwrap().utgaver_fullfor.clear();
        last_opp(&Opptak::ny(kopi), &k, &fil, &v, &stopp("x")).await.unwrap();
        let l = m.lager.lock().unwrap();
        assert!(l.handlinger("proxy-lenk-start").is_empty() && l.lenket.is_empty(), "fersk adresse — ingen lenking");
        assert_eq!(l.handlinger("fullfor").last().unwrap()["proxyKey"], "proxy/test.mp4");
    });
}

/// F8: gjenopptatt dagen etter — tilbudets adresse har gått ut. Ingen PUT dit, fullfør uten proxyKey (men med plakat og
/// felt som før), og så lenkes kopien med en fersk adresse: start → PUT → fullfør, med serverens nøkkel og størrelsen.
/// Sluttstatusen kommer etter lenkingen. Det samme for en tilstand fra før feltet fantes (`proxy_tid` 0), der bare
/// fullfør gjensto.
#[test]
fn gammel_proxy_adresse_lenkes_etter_fullfor() {
    med_app(|_app| async move {
        let m = start_mock().await;
        let data = testdata(40 << 20, 84);
        let p = testfil("gammel.mp4", &data);
        let kopi = testfil("gammel-proxy.mp4", &testdata(20_000, 85));
        let del = 16u64 << 20;
        {
            let mut l = m.lager.lock().unwrap();
            l.deler.insert(1, data[..del as usize].to_vec());
            l.del_bytes = del;
            l.paabegynt("originals/test.mp4", "u9");
        }
        let v = valg("proxy-gammel");
        resume_skriv(&tilstand_for(&v, &p, ""), &Resume {
            versjon: 2, sti: p.to_string_lossy().into(), upload_id: "u9".into(), original_key: "originals/test.mp4".into(),
            bytes: data.len() as u64, sist_endret: mtime_ms(&p), del_bytes: del, deler: vec![(1, format!("e1-{del}"))],
            proxy_put: format!("{}/r2/proxy?sig=utlopt", m.adr), proxy_key: "proxy/test.mp4".into(), proxy_tid: naa() - TRE_TIMER_MS,
            ..Default::default()
        });
        let app = Opptak::ny(kopi.clone());
        let res = last_opp(&app, &klient(&m), &OppTil::fra_sti(&p, ""), &v, &stopp("x")).await.unwrap();
        assert!(res.id.starts_with("rec") && !res.allerede, "{res:?}");
        {
            let l = m.lager.lock().unwrap();
            assert!(l.objekt.as_deref() == Some(&data[..]), "fila som ble satt sammen er ikke lik originalen");
            assert_eq!((l.forsok(1), l.forsok(2), l.forsok(3)), (0, 1, 1), "gjenopptatt fra del 2");
            assert_eq!(proxy_rekke(&l), ["fullfor", "proxy-lenk-start", "proxy-put", "proxy-lenk-fullfor"]);
            assert_eq!(l.handlinger("proxy-put")[0]["sti"], "/r2/proxy?sig=lenk", "aldri til den gamle adressen");
            let f = &l.handlinger("fullfor")[0];
            assert!(f.get("proxyKey").is_none() && f.get("proxyStorrelse").is_none() && f.get("proxyUlesbar").is_none(), "{f}");
            assert!(f["posterBase64"].is_string() && f["varighet"].is_number(), "plakat og felt går med i fullfør som før");
            assert_eq!(l.handlinger("proxy-lenk-start")[0], serde_json::json!({ "action": "proxy-lenk-start", "id": res.id }));
            assert_eq!(l.handlinger("proxy-lenk-fullfor")[0], serde_json::json!({ "action": "proxy-lenk-fullfor", "id": res.id, "proxyStorrelse": 20_000 }));
            assert_eq!(l.lenket, [Lenking { id: res.id.clone(), proxy_key: "proxy/test.mp4".into(), proxy_storrelse: 20_000 }]);
            assert!(l.proxy.as_deref() == Some(&std::fs::read(&kopi).unwrap()[..]), "kopien kom fram");
        }
        assert!(!tilstand_for(&v, &p, "").exists());
        let s = app.statuser();
        assert!(pos(&s, "fullfor") < pos(&s, "proxy"), "kopien lages etter fullfør: {s:?}");
        assert_eq!(s.last().map(String::as_str), Some("ferdig"), "sluttstatusen kommer etter lenkingen: {s:?}");
        assert_eq!(s.iter().filter(|x| *x == "ferdig").count(), 1, "{s:?}");

        // Fra før feltet fantes (proxy_tid 0 = ukjent alder), og bare fullfør gjensto.
        let m = start_mock().await;
        let data = testdata(2 << 20, 86);
        let p = testfil("gammel-ukjent.mov", &data);
        m.lager.lock().unwrap().paabegynt("originals/test.mov", "x");
        let v = valg("proxy-ukjent");
        resume_skriv(&tilstand_for(&v, &p, ""), &Resume {
            versjon: 2, sti: p.to_string_lossy().into(), original_key: "originals/test.mov".into(), bytes: data.len() as u64,
            sist_endret: mtime_ms(&p), proxy_put: format!("{}/r2/proxy?sig=utlopt", m.adr), proxy_key: "proxy/test.mp4".into(),
            satt_sammen: true, ..Default::default()
        });
        let res = last_opp(&Opptak::ny(kopi), &klient(&m), &OppTil::fra_sti(&p, ""), &v, &stopp("x")).await.unwrap();
        let l = m.lager.lock().unwrap();
        assert_eq!(l.forsok(0), 0, "bytene lå der alt");
        assert_eq!(proxy_rekke(&l), ["fullfor", "proxy-lenk-start", "proxy-put", "proxy-lenk-fullfor"]);
        assert_eq!(l.handlinger("proxy-put")[0]["sti"], "/r2/proxy?sig=lenk");
        assert_eq!(l.lenket, [Lenking { id: res.id, proxy_key: "proxy/test.mp4".into(), proxy_storrelse: 20_000 }]);
    });
}

/// F8: lenkingen er best effort. Feiler `proxy-lenk-start` (signeringen svarer 502), er opplastingen likevel ferdig: ingen
/// PUT, ingen `proxy-lenk-fullfor`, sluttstatusen som før, og tilstanden ryddet. Fantes fila fra før (`allerede`), lenkes
/// ingenting.
#[test]
fn lenking_som_feiler_velter_ikke_opplastingen() {
    med_app(|_app| async move {
        let m = start_mock().await;
        let kopi = testfil("lenk-feil-proxy.mp4", &testdata(20_000, 87));
        let data = testdata(2 << 20, 88);
        let p = testfil("lenk-feil.mp4", &data);
        {
            let mut l = m.lager.lock().unwrap();
            l.lenk_start_feil = Some(502);
            l.paabegynt("originals/test.mp4", "x");
        }
        let v = valg("lenk-feil");
        let gammel = |p: &Path, m: &Mock| Resume {
            versjon: 2, sti: p.to_string_lossy().into(), original_key: "originals/test.mp4".into(), bytes: std::fs::metadata(p).unwrap().len(),
            sist_endret: mtime_ms(p), proxy_put: format!("{}/r2/proxy?sig=utlopt", m.adr), proxy_key: "proxy/test.mp4".into(),
            proxy_tid: naa() - TRE_TIMER_MS, satt_sammen: true, ..Default::default()
        };
        resume_skriv(&tilstand_for(&v, &p, ""), &gammel(&p, &m));
        let app = Opptak::ny(kopi.clone());
        let res = last_opp(&app, &klient(&m), &OppTil::fra_sti(&p, ""), &v, &stopp("x")).await.expect("opplastingen er ferdig selv om kopien ikke ble lenket");
        assert!(res.id.starts_with("rec") && !res.allerede, "{res:?}");
        {
            let l = m.lager.lock().unwrap();
            assert!(!l.handlinger("proxy-lenk-start").is_empty());
            assert!(l.handlinger("proxy-put").is_empty() && l.handlinger("proxy-lenk-fullfor").is_empty() && l.lenket.is_empty());
        }
        assert!(!tilstand_for(&v, &p, "").exists(), "fila er i skapet — tilstanden ryddes");
        assert_eq!(app.statuser().last().map(String::as_str), Some("ferdig"));

        // Fila fantes fra før: ingen lenking.
        let m = start_mock().await;
        m.lager.lock().unwrap().rader.insert("originals/test.mp4".into(), Rad { id: "recFraFoer000001".into(), versjon_av: String::new() });
        let p = testfil("lenk-allerede.mp4", &data);
        let v = valg("lenk-allerede");
        resume_skriv(&tilstand_for(&v, &p, ""), &gammel(&p, &m));
        let app = Opptak::ny(kopi);
        let res = last_opp(&app, &klient(&m), &OppTil::fra_sti(&p, ""), &v, &stopp("x")).await.unwrap();
        assert!(res.allerede && res.id == "recFraFoer000001", "{res:?}");
        let l = m.lager.lock().unwrap();
        assert!(l.handlinger("proxy-lenk-start").is_empty() && l.handlinger("proxy-put").is_empty());
        assert_eq!(app.statuser().last().map(String::as_str), Some("hoppet"));
    });
}

/// F9: de påbegynte opplastingene står i tilstandsmappa med sti, navn, mappe og hvor mye som er sendt — så appen kan
/// tilby «Fortsett» etter en omstart. Synk-minnet og v1-filer (uten sti) er ikke med.
#[test]
fn uferdige_lister_paabegynte() {
    med_app(|app| async move {
        let m = start_mock().await;
        let data = testdata(40 << 20, 71);
        let p = testfil("mp-uferdig.bin", &data);
        m.lager.lock().unwrap().uhell.insert((2, 1), Uhell::Status(400));
        let v = valg("uferdige");
        std::fs::write(v.tilstand_dir.join("synk-abc.json"), "[]").unwrap();
        std::fs::write(v.tilstand_dir.join("0123456789abcdef.json"), r#"{"upload_id":"u1","original_key":"originals/x.bin","mappe_id":"","bytes":1,"deler":[]}"#).unwrap();
        assert!(last_opp(&app, &klient(&m), &OppTil::fra_sti(&p, "m1"), &v, &stopp("x")).await.is_err());
        let u = uferdige(&v);
        assert_eq!(u, vec![Uferdig { sti: p.clone(), navn: "mp-uferdig.bin".into(), mappe_id: "m1".into(), bytes: data.len() as u64, sendt: 16 << 20, sist_endret: mtime_ms(&p) }]);
    });
}

/// Nedlasting som kuttes midt i: neste forsøk skal be om resten (Range) og ende med en fil som er
/// bit for bit lik — verifisert mot sjekksummen, slik appen gjør.
#[test]
fn nedlasting_fortsetter_etter_kutt() {
    med_app(|app| async move {
        let m = start_mock().await;
        let data = testdata(3 << 20, 5);
        {
            let mut l = m.lager.lock().unwrap();
            l.nedlasting = data.clone();
            l.ned_uhell.insert(1, Uhell::Kutt(1 << 20));
        }
        let rot = std::env::temp_dir().join("rawskap-transfer-test").join("ned");
        let _ = std::fs::remove_dir_all(&rot);
        let fil = Fil { id: "a1".into(), filnavn: "ned.bin".into(), bytes: data.len() as u64, sti: String::new(), xxh64: hash(&data), url: String::new() };
        let res = last_ned_med_forsok(&app, &klient(&m), &fil, &rot, &stopp("a1"), &Struper::ny(Arc::new(AtomicU64::new(0))), "hopp").await;
        assert_eq!(res, Ok(()));
        assert!(std::fs::read(rot.join("ned.bin")).unwrap() == data, "nedlastet fil er ikke lik originalen");
        let l = m.lager.lock().unwrap();
        assert_eq!(l.ned_range, vec![None, Some(1 << 20)], "andre forsøk skal fortsette der det første slapp");
    });
}

/// «Fjern» i køen: serveren får beskjed om nøklene, og tilstandsfilene forsvinner — v2 (uansett mappe, køen vet ikke
/// hvilken undermappe fila endte i) og v1 fra før T2.
#[test]
fn forkast_rydder_hos_serveren_og_lokalt() {
    med_app(|_app| async move {
        let m = start_mock().await;
        let data = testdata(1 << 20, 6);
        let p = testfil("forkast.bin", &data);
        let v = valg("forkast");
        let (sti, len, mt) = (p.to_string_lossy().to_string(), data.len() as u64, mtime_ms(&p));
        let p2 = tilstand_sti(&v.tilstand_dir, &sti, len, mt, "m1");
        resume_skriv(&p2, &Resume { versjon: 2, sti: sti.clone(), upload_id: "u1".into(), original_key: "originals/forkast.bin".into(), mappe_id: "m1".into(), bytes: len, sist_endret: mt, del_bytes: 16 << 20, ..Default::default() });
        let p1 = tilstand_sti_v1(&v.tilstand_dir, &sti, len, mt);
        resume_skriv(&p1, &Resume { upload_id: "u0".into(), original_key: "originals/forkast-v1.bin".into(), bytes: len, ..Default::default() });
        let n = forkast_opplasting(&klient(&m), &v, &[oppfil(&p)]).await;
        assert_eq!(n, Ok(2));
        assert!(!p2.exists() && !p1.exists(), "tilstandsfilene skal være borte");
        let l = m.lager.lock().unwrap();
        let mut sendt: Vec<String> = serde_json::from_value(l.handlinger("avbryt")[0]["originalKeys"].clone()).unwrap();
        sendt.sort();
        assert_eq!(sendt, ["originals/forkast-v1.bin", "originals/forkast.bin"]);
    });
}
