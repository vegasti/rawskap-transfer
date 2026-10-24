// xxh64 over hele filer.
use crate::{Framdrift, Melder, Taus};
use std::path::Path;

/// xxh64 av hele fila (1 MB-blokker). Brukes ved opplasting (lagres på raden)
/// og ved nedlasting (verifisering mot serverens verdi).
pub async fn fil_xxh64(sti: &Path) -> Result<String, String> { fil_xxh64_meld::<Taus>(sti, None).await }

/// xxh64 over hele fila. `meld` gir framdrift underveis (app, id, total) —
/// STORE filer bruker flere titalls sekunder her FØR første byte er sendt, og
/// uten melding ser appen ut som den henger på 0 kB/s (Vegard 28/8).
pub async fn fil_xxh64_meld<M: Melder>(sti: &Path, meld: Option<(&M, &str, u64)>) -> Result<String, String> {
    use tokio::io::AsyncReadExt;
    let mut f = tokio::fs::File::open(sti).await.map_err(|e| format!("{e}"))?;
    let mut h = xxhash_rust::xxh64::Xxh64::new(0);
    let mut buf = vec![0u8; 1 << 20];
    let mut lest: u64 = 0;
    let mut sist_meldt = std::time::Instant::now();
    loop {
        let n = f.read(&mut buf).await.map_err(|e| format!("{e}"))?;
        if n == 0 { break; }
        h.update(&buf[..n]);
        lest += n as u64;
        if let Some((app, id, total)) = meld {
            if sist_meldt.elapsed().as_millis() > 200 {
                sist_meldt = std::time::Instant::now();
                let _ = app.framdrift(Framdrift { id: id.to_string(), hentet: lest, total, status: "hash".into(), feil: None });
            }
        }
    }
    Ok(format!("{:016x}", h.digest()))
}
