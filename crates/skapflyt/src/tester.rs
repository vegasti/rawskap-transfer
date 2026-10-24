// ─────────────────────────────────────────────────────────────────────────────
// TESTER (17/9). Repoet hadde ingen. Fire feil ble funnet på én ettermiddag, og tre av dem satt i
// logikk som så åpenbart riktig ut ved lesing — så dekningen starter på det som faktisk ØDELEGGER
// DATA hvis det er galt, ikke på det som er lett å teste.
// (S1: nett- og opplastingstestene flyttet hit fra src-tauri/src/lib.rs sammen med koden.)
//   cargo test --manifest-path crates/skapflyt/Cargo.toml --all-features
use crate::testportal::*;
use crate::*;
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

// ── MOT DEN FALSKE PORTALEN (testportal) ──

fn testdata(n: usize, frø: u64) -> Vec<u8> {
    let mut x = frø.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
    (0..n).map(|_| { x ^= x << 13; x ^= x >> 7; x ^= x << 17; (x >> 24) as u8 }).collect()
}
fn testfil(navn: &str, data: &[u8]) -> OppFil {
    let d = std::env::temp_dir().join("rawskap-transfer-test").join("filer");
    std::fs::create_dir_all(&d).unwrap();
    let p = d.join(navn);
    std::fs::write(&p, data).unwrap();
    OppFil { sti: p.to_string_lossy().to_string(), relativ: navn.into(), bytes: data.len() as u64 }
}
fn stopp(id: &str) -> Stopp { Stopp { global: Arc::new(AtomicBool::new(false)), sett: Arc::new(std::sync::Mutex::new(Default::default())), id: id.into() } }
fn hash(d: &[u8]) -> String { format!("{:016x}", xxhash_rust::xxh64::xxh64(d, 0)) }
fn mtime_ms(sti: &str) -> u64 { std::fs::metadata(sti).unwrap().modified().unwrap().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64 }
fn ubegrenset() -> Arc<Struper> { Struper::ny(Arc::new(AtomicU64::new(0))) }

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
        let fil = testfil("mp-uhell.bin", &data);
        {
            let mut l = m.lager.lock().unwrap();
            l.uhell.insert((1, 1), Uhell::Kutt(1 << 20));
            l.uhell.insert((2, 1), Uhell::Status(500));
            l.uhell.insert((3, 1), Uhell::Status(403));
        }
        let k = klient("test", UA).unwrap();
        let res = last_opp_en(&app, &k, &bare_opp(), &m.adr, &fil, "", &stopp(&fil.sti), ubegrenset()).await;
        assert_eq!(res, Ok(()));
        let l = m.lager.lock().unwrap();
        assert!(l.objekt.as_deref() == Some(&data[..]), "fila som ble satt sammen er ikke lik originalen");
        assert_eq!((l.forsok(1), l.forsok(2), l.forsok(3)), (2, 2, 2));
        assert!(l.signeringer >= 2, "del 3 skulle fått ny signatur etter 403");
        let fullfor = l.handlinger("fullfor");
        assert_eq!(fullfor.len(), 1);
        assert_eq!(fullfor[0]["xxh64"].as_str(), Some(hash(&data).as_str()));
        assert_eq!(l.handlinger("multipart-start")[0]["delBytes"].as_u64(), Some(16 << 20));
    });
}

/// Gjenopptak spør lageret, ikke bare resume-fila. Lokal bokføring er tom, men lageret har del 1
/// (hel) og del 2 (halv). Del 1 skal IKKE sendes igjen; del 2 skal det.
#[test]
fn gjenopptak_bruker_lagerets_deler() {
    med_app(|app| async move {
        let m = start_mock().await;
        let data = testdata(40 << 20, 2);
        let fil = testfil("mp-gjenoppta.bin", &data);
        let del = 16u64 << 20;
        {
            let mut l = m.lager.lock().unwrap();
            l.deler.insert(1, data[..del as usize].to_vec());
            l.deler.insert(2, data[del as usize..del as usize + 1000].to_vec());
            l.del_bytes = del;
        }
        let rsti = resume_sti(&fil.sti, fil.bytes, mtime_ms(&fil.sti));
        resume_skriv(&rsti, &Resume { upload_id: "u1".into(), original_key: "originals/test.bin".into(), bytes: fil.bytes, del_bytes: del, ..Default::default() });
        let k = klient("test", UA).unwrap();
        let res = last_opp_en(&app, &k, &bare_opp(), &m.adr, &fil, "", &stopp(&fil.sti), ubegrenset()).await;
        assert_eq!(res, Ok(()));
        let l = m.lager.lock().unwrap();
        assert!(l.objekt.as_deref() == Some(&data[..]), "fila som ble satt sammen er ikke lik originalen");
        assert_eq!((l.forsok(1), l.forsok(2), l.forsok(3)), (0, 1, 1), "del 1 lå der alt; del 2 var halv");
        assert!(l.handlinger("multipart-start").is_empty(), "skulle fortsatt den gamle opplastingen");
        assert_eq!(l.handlinger("multipart-status").len(), 1);
        // Gjenopptatt = vi har ikke sett hele fila i denne kjøringen, og da sendes ingen sjekksum.
        assert!(l.handlinger("fullfor")[0].get("xxh64").is_none());
        assert!(!rsti.exists(), "resume-fila skal ryddes når fila er oppe");
    });
}

/// En resume-fil som peker på en opplasting portalen ikke kjenner lenger (feiekosten ryddet den),
/// skal gi en ny opplasting — ikke en fil som står fast.
#[test]
fn glemt_opplasting_begynner_paa_nytt() {
    med_app(|app| async move {
        let m = start_mock().await;
        let data = testdata(30 << 20, 3);
        let fil = testfil("mp-glemt.bin", &data);
        let rsti = resume_sti(&fil.sti, fil.bytes, mtime_ms(&fil.sti));
        resume_skriv(&rsti, &Resume { upload_id: "gammel".into(), original_key: "originals/gammel.bin".into(), bytes: fil.bytes, deler: vec![(1, "x".into())], del_bytes: 16 << 20, ..Default::default() });
        let k = klient("test", UA).unwrap();
        let res = last_opp_en(&app, &k, &bare_opp(), &m.adr, &fil, "", &stopp(&fil.sti), ubegrenset()).await;
        assert_eq!(res, Ok(()));
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
        let fil = testfil("enkel.bin", &data);
        {
            let mut l = m.lager.lock().unwrap();
            l.uhell.insert((0, 1), Uhell::Stopp(64 << 10));
            l.uhell.insert((0, 2), Uhell::Kutt(1 << 20));
        }
        let k = klient("test", UA).unwrap();
        let start = std::time::Instant::now();
        let res = last_opp_en(&app, &k, &bare_opp(), &m.adr, &fil, "", &stopp(&fil.sti), ubegrenset()).await;
        assert_eq!(res, Ok(()));
        let l = m.lager.lock().unwrap();
        assert_eq!(l.forsok(0), 3);
        assert!(l.objekt.as_deref() == Some(&data[..]), "fila er ikke lik originalen");
        assert_eq!(l.handlinger("fullfor")[0]["xxh64"].as_str(), Some(hash(&data).as_str()));
        assert!(start.elapsed() < std::time::Duration::from_secs(20), "stopp-vakta skal kutte den døde linja raskt ({:?})", start.elapsed());
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
        let k = klient("test", UA).unwrap();
        let res = last_ned_med_forsok(&app, &k, &bare_ned(), &m.adr, &fil, &rot, &stopp("a1"), &ubegrenset(), "hopp").await;
        assert_eq!(res, Ok(()));
        assert!(std::fs::read(rot.join("ned.bin")).unwrap() == data, "nedlastet fil er ikke lik originalen");
        let l = m.lager.lock().unwrap();
        assert_eq!(l.ned_range, vec![None, Some(1 << 20)], "andre forsøk skal fortsette der det første slapp");
    });
}

/// «Fjern» i køen: serveren får beskjed om nøkkelen, og resume-fila forsvinner.
#[test]
fn forkast_rydder_hos_serveren_og_lokalt() {
    med_app(|_app| async move {
        let m = start_mock().await;
        let data = testdata(1 << 20, 6);
        let fil = testfil("forkast.bin", &data);
        let rsti = resume_sti(&fil.sti, fil.bytes, mtime_ms(&fil.sti));
        resume_skriv(&rsti, &Resume { upload_id: "u1".into(), original_key: "originals/forkast.bin".into(), bytes: fil.bytes, del_bytes: 16 << 20, ..Default::default() });
        let n = forkast_opplasting(m.adr.clone(), "test".into(), vec![fil.clone()], UA).await;
        assert_eq!(n, Ok(1));
        assert!(!rsti.exists(), "resume-fila skal være borte");
        let l = m.lager.lock().unwrap();
        assert_eq!(l.handlinger("avbryt")[0]["originalKeys"], serde_json::json!(["originals/forkast.bin"]));
    });
}
