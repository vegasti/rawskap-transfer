// skapflyt (S1, 24/10-2026): overføringsmotoren fra Rawskap Transfer, trukket ut i en egen crate så
// rawMotion og rawLight kan bruke den samme løypa mot skapet. Koden er FLYTTET fra
// src-tauri/src/lib.rs uten endret oppførsel: samme forespørsler, samme JSON til serveren, samme
// statusstrenger og feiltekster til appen, og tilstandsfilene for gjenopptak ligger der de lå.
//
// Det som bor her: klientene og takene, omforsøk og venting på nettet, stoppvakten og struperen,
// opplasting (enkel PUT, multipart med gjenopptak, forkast, økt), nedlasting med Transfers regler,
// kobling, mapper og delinger. Det som IKKE bor her: ffmpeg og avspillingskopien, Tauri, køen og
// grensesnittet. Appen kobler seg på gjennom to traits — `Melder` (framdrift) og `Etterarbeid`
// (avspillingskopi og videofelt før fullfør).
//
// T2 (24/10): det rawMotion trenger — `Klient` (i stedet for ua-parameterne), typet `Feil`, `opplasting::Kilde`,
// parallelle deler, `Opplastet` med fil-id-en, proxyKey fra serveren, ny utgave (versjonAv), `uferdige()` og tilstand v2.
// Transfers oppførsel står: samme JSON (nye nøkler bare når de nye feltene er satt), samme tekster (`Feil` sin Display).
//
// ⚠ To typer heter Fil: `skapflyt::Fil` er en fil i SKAPET (nedlasting, som før), `skapflyt::opplasting::Fil` er en
// fil som skal DIT.
//
// En metode som kaller seg selv, er en krasj ved første kall — aldri bare en advarsel. (0.3.0: en
// regex-erstatning gjorde Kanal::framdrift rekursiv, og det var bare advarselen som fanget den.)
#![deny(unconditional_recursion)]
use serde::{Deserialize, Serialize};

pub mod deling;
pub mod feil;
pub mod innlesing;
pub mod navn;
pub mod nedlasting;
pub mod nett;
pub mod opplasting;
pub mod sjekksum;
pub mod skap;
#[cfg(any(test, feature = "testportal"))]
pub mod testportal;
#[cfg(test)]
mod tester;

pub use deling::*;
pub use feil::*;
pub use navn::*;
pub use nedlasting::*;
pub use nett::*;
// `opplasting::Fil` skygges her av nedlastingens `Fil` under — den nås som `skapflyt::opplasting::Fil`.
pub use opplasting::*;
pub use sjekksum::*;
pub use skap::*;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Fil {
    pub id: String,
    pub filnavn: String,
    pub bytes: u64,
    #[serde(default)]
    pub sti: String,
    #[serde(default)]
    pub xxh64: String,
    #[serde(default)]
    pub url: String, // deling: nedlastings-URL m/ token (302 → R2), uten innlogging
}

/// Hendelsen appen sender til grensesnittet for hver fil («framdrift» i Transfer).
#[derive(Clone, Serialize)]
pub struct Framdrift {
    pub id: String,
    pub hentet: u64,
    pub total: u64,
    pub status: String, // venter | laster | ferdig | feil | hoppet
    pub feil: Option<String>,
}

// ── OPPLASTING (22/8): samme løype som nettleseren — presign → PUT rett til
// R2 → fullfør (serveren lager thumb/EXIF for bilder). Undermapper gjenskapes
// i skapet (mapper-API, cache per sti). Video får ingen poster i v0.
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct OppFil { pub sti: String, pub relativ: String, pub bytes: u64 }

/// Én fil `mangler_lokalt` skal se etter i nedlastingsmappa.
#[derive(Deserialize)]
pub struct SjekkFil { pub id: String, #[serde(default)] pub sti: String, pub filnavn: String }

/// Det opp- og nedlastingsløypa trenger for å melde framdrift.
///
/// Egen trait (0.3.0) så testene kan kjøre løypa uten en ekte Tauri-app. En falsk app
/// (tauri::test::mock_app) drar inn vindus-koden, og testprogrammet mangler manifestet den koden
/// krever — det starter ikke engang på Windows (STATUS_ENTRYPOINT_NOT_FOUND). Å bygge manifestet
/// inn via linkeren ville løst det, men endret hvordan appen vi SENDER UT lenkes; dette gjør ikke det.
/// (S1: før het den `Kanal` og hadde ffmpeg med seg — den delen er nå `Etterarbeid`.)
pub trait Melder: Clone + Send + Sync + 'static {
    fn framdrift(&self, f: Framdrift);
}

/// Det opplastingen trenger av appen etter at bytene ligger i lageret og før `fullfor`:
/// avspillingskopien og videofeltene (plakat, stripe, mål, varighet). Transfer gjør det med
/// ffmpeg-sidecaren; craten vet ingenting om ffmpeg. En app uten noe slikt svarer «ingenting».
///
/// Rekkefølgen er den samme som før uttrekket: `avspillingskopi` → `ulesbar_video` (bare når det
/// ikke ble noen kopi) → framdrift «fullfor» → `videofelt` → fullfør-kallet.
pub trait Etterarbeid: Clone + Send + Sync + 'static {
    /// Lag og last opp avspillingskopien til `proxy_put` (serverens presignerte adresse; tom = ingen
    /// tilbudt). Gir størrelsen, eller None. En manglende kopi skal aldri velte selve opplastingen.
    fn avspillingskopi(&self, bare: &reqwest::Client, portal: &str, sti: &str, navn: &str, proxy_put: &str, bytes: u64) -> impl std::future::Future<Output = Option<u64>> + Send;
    /// Ble det ingen kopi: er det fordi fila er en video som ikke kan leses (ProRes RAW)? Da sendes
    /// `proxyUlesbar`, og fila meldes «utenProxy» i stedet for «ferdig».
    fn ulesbar_video(&self, sti: &str, navn: &str) -> impl std::future::Future<Output = bool> + Send;
    /// Felt til fullfør-kroppen (plakat, stripe, mål, varighet). Kalles etter framdrift «fullfor».
    fn videofelt(&self, sti: &str, navn: &str, bytes: u64, body: &mut serde_json::Value) -> impl std::future::Future<Output = ()> + Send;
}

pub fn meld<K: Melder>(app: &K, id: &str, hentet: u64, total: u64, status: &str) {
    app.framdrift(Framdrift { id: id.to_string(), hentet, total, status: status.into(), feil: None });
}

/// En melder som ikke sier noe — for kall som ikke skal melde framdrift (`fil_xxh64`).
#[derive(Clone)]
pub(crate) struct Taus;
impl Melder for Taus {
    fn framdrift(&self, _f: Framdrift) {}
}
