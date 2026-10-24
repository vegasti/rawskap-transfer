// MINNEKORT (T4, 24/10-2026): monterte volumer med en DCIM-mappe, altså DCF-oppsettet som alle
// kameraer skriver. Transfer og rawLight skal se de samme kortene, så regelen er motorens.
//
// Lånt fra LightCraft-motoren (vegasti/rawlight, crates/engine/src/devices.rs: `devices_in`,
// `dcim_in` og `mount_parents`). Den koden er Copyright (c) 2026 ArtCraft Team and the LightCraft
// contributors, lisensiert under MIT eller Apache-2.0 etter eget valg. Den er skrevet om her (norske
// navn, egen miljøvariabel) av Raw Studios AS. Se crates/skapflyt/NOTICE.
//
// Bare lesing: ingenting her skriver til et kort. Kallet er blokkerende (det leser rota på hver
// stasjon D:–Z: på Windows), så kjør det utenfor grensesnittets tråd.
use std::path::{Path, PathBuf};

/// Et minnekort eller et kamera som er koblet til maskinen.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct Kort {
    /// Volumets navn («EOS_DIGITAL», «Untitled»). På Windows er det stasjonsroten («F:\»).
    pub navn: String,
    /// Monteringspunktet, altså det som formateres.
    pub rot: PathBuf,
    /// Kortets DCIM-mappe. Fra `kort()` er den alltid satt i dag: regelen er DCIM. `None` er plass
    /// til kort uten DCIM (kinokameraer) hvis regelen en gang utvides.
    pub dcim: Option<PathBuf>,
}

/// Kortene som står i maskinen nå. Regelen er den samme som motorens `library.devices`: DCIM på
/// `/Volumes` (Mac), under `/media/$USER`, `/run/media/$USER`, `/media` og `/mnt` (Linux) og på
/// stasjonene D:–Z: (Windows).
///
/// `SKAPFLYT_KORT_ROTER` (stier skjøtt som i `PATH`) erstatter monteringsmappene, og det samme gjør
/// motorens `LIGHTCRAFT_DEVICE_ROOTS`. Tester bruker det, så begge appene ser de samme falske kortene.
pub fn kort() -> Vec<Kort> {
    let mut ekstra = Vec::new();
    if cfg!(windows) && overstyrt().is_none() {
        ekstra.extend((b'D'..=b'Z').map(|c| PathBuf::from(format!("{}:\\", c as char))));
    }
    kort_i(&monteringsmapper(), ekstra)
}

fn overstyrt() -> Option<std::ffi::OsString> {
    std::env::var_os("SKAPFLYT_KORT_ROTER").or_else(|| std::env::var_os("LIGHTCRAFT_DEVICE_ROOTS"))
}

/// Mappene der monterte volumer ligger som barn på denne plattformen.
fn monteringsmapper() -> Vec<PathBuf> {
    if let Some(v) = overstyrt() {
        return std::env::split_paths(&v).collect();
    }
    let mut ut: Vec<PathBuf> = Vec::new();
    if cfg!(target_os = "macos") {
        ut.push("/Volumes".into());
    } else if cfg!(any(target_os = "linux", target_os = "freebsd")) {
        if let Ok(bruker) = std::env::var("USER") {
            ut.push(format!("/media/{bruker}").into());
            ut.push(format!("/run/media/{bruker}").into());
        }
        ut.push("/media".into());
        ut.push("/mnt".into());
    }
    ut
}

/// Kortene blant barna av `foreldre`, pluss volumrøttene i `ekstra`. Ren funksjon av disken, så den
/// kan testes mot falske kort i en midlertidig mappe.
pub fn kort_i(foreldre: &[PathBuf], ekstra: Vec<PathBuf>) -> Vec<Kort> {
    let mut rotter: Vec<PathBuf> = Vec::new();
    for forelder in foreldre {
        if let Ok(rd) = std::fs::read_dir(forelder) {
            let mut v: Vec<PathBuf> = rd.flatten().map(|e| e.path()).collect();
            v.sort();
            // Oppstartsdisken står i /Volumes som en symlenke til /.
            rotter.extend(v.into_iter().filter(|p| !p.is_symlink()));
        }
    }
    rotter.extend(ekstra);
    let mut ut: Vec<Kort> = Vec::new();
    for rot in rotter {
        let Some(dcim) = dcim_i(&rot) else { continue };
        let navn = rot.file_name().map(|n| n.to_string_lossy().to_string()).unwrap_or_else(|| rot.to_string_lossy().to_string());
        if !ut.iter().any(|k| k.rot == rot) {
            ut.push(Kort { navn, rot, dcim: Some(dcim) });
        }
    }
    ut
}

/// Volumets DCIM-mappe. De vanlige stavemåtene sjekkes direkte. Andre stavemåter (`Dcim` på et volum
/// som skiller store og små bokstaver) finnes ved å lese rota, men ikke på Windows, der stasjonene
/// ikke skiller dem.
pub fn dcim_i(rot: &Path) -> Option<PathBuf> {
    if let Some(p) = ["DCIM", "dcim"].iter().map(|n| rot.join(n)).find(|p| p.is_dir()) {
        return Some(p);
    }
    if cfg!(windows) {
        return None;
    }
    std::fs::read_dir(rot).ok()?.flatten().map(|e| e.path()).find(|p| p.file_name().is_some_and(|n| n.eq_ignore_ascii_case("DCIM")) && p.is_dir())
}

#[cfg(test)]
mod tester {
    use super::*;

    fn rot(navn: &str) -> PathBuf {
        let d = std::env::temp_dir().join("skapflyt-kort").join(navn); // fast navn: tømmes ved start, så temp ikke vokser for hver kjøring
        let _ = std::fs::remove_dir_all(&d);
        d
    }

    /// Falske kort: tre med DCIM i tre stavemåter, én disk uten, og en rot gitt direkte.
    #[test]
    fn kort_er_volumer_med_dcim() {
        let base = rot("monterte");
        std::fs::create_dir_all(base.join("EOS_DIGITAL/DCIM/100CANON")).unwrap();
        std::fs::create_dir_all(base.join("Backup/Photos")).unwrap();
        std::fs::create_dir_all(base.join("SD/dcim")).unwrap();
        std::fs::create_dir_all(base.join("X/Dcim")).unwrap();
        std::fs::write(base.join("enfil.txt"), b"ikke et volum").unwrap();
        let direkte = rot("direkte");
        std::fs::create_dir_all(direkte.join("DCIM/100MSDCF")).unwrap();

        let k = kort_i(std::slice::from_ref(&base), vec![direkte.clone(), base.join("finnes-ikke")]);
        let navn: Vec<&str> = k.iter().map(|x| x.navn.as_str()).collect();
        assert_eq!(navn, ["EOS_DIGITAL", "SD", "X", direkte.file_name().unwrap().to_str().unwrap()], "{k:?}");
        for x in &k {
            let d = x.dcim.as_ref().expect("kort() gir alltid DCIM-mappa");
            assert!(d.is_dir() && d.file_name().is_some_and(|n| n.eq_ignore_ascii_case("DCIM")), "{x:?}");
            assert_eq!(d.parent(), Some(x.rot.as_path()), "DCIM ligger rett under rota: {x:?}");
        }
        // Samme rot to ganger blir ett kort.
        assert_eq!(kort_i(std::slice::from_ref(&base), vec![base.join("SD")]).len(), 3);
        let _ = std::fs::remove_dir_all(&base);
        let _ = std::fs::remove_dir_all(&direkte);
    }

    /// `kort()` leser miljøvariabelen, og på Windows prøves da ikke D:–Z:.
    #[test]
    fn kort_folger_miljovariabelen() {
        let base = rot("miljo");
        std::fs::create_dir_all(base.join("NIKON Z 8/DCIM/100NZ8")).unwrap();
        std::fs::create_dir_all(base.join("Tom")).unwrap();
        std::env::set_var("SKAPFLYT_KORT_ROTER", &base);
        let k = kort();
        std::env::remove_var("SKAPFLYT_KORT_ROTER");
        assert_eq!(k.len(), 1, "{k:?}");
        assert_eq!(k[0].navn, "NIKON Z 8");
        assert_eq!(k[0].rot, base.join("NIKON Z 8"));
        let _ = std::fs::remove_dir_all(&base);
    }
}
