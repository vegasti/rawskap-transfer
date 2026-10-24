// Feilene fra craten (T2, 24/10): typet, så kalleren kan spørre HVA som gikk galt i stedet for å lese strenger.
//
// ⚠ `Display` gir NØYAKTIG tekstene fra før T2. Transfers kommandoer gjør `Feil` om til `String` (`From`/`to_string`), og
// grensesnittet viser teksten og kjenner igjen noen av dem (DELING_LAAST …) — så teksten bæres i variantene der den
// varierer, og lages på kallstedet akkurat som før. Typen er klassifiseringen; teksten er det brukeren ser.

/// Hva som gikk galt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Feil {
    /// Brukeren stoppet: pause, avbryt eller en utkrysset enkeltfil.
    Avbrutt,
    /// Nøkkelen er ugyldig, utløpt eller har ikke rett (401/403 fra portalen).
    IkkeTilgang(String),
    /// Lagringen er full (serverens `kvoteSperre`). Serverens tekst.
    KvoteFull(String),
    /// Fila er sperret, så den kan ikke lastes ned (samtykke trukket/utløpt).
    Sperret(String),
    /// Delingen er låst. Teksten er låsen serveren oppga («passord», «invitert»), tom når den ikke er kjent.
    DelingLaast(String),
    /// Opplastingen finnes ikke lenger hos portalen eller lageret. Et nytt forsøk på FILA begynner på nytt.
    Borte(String),
    /// Nettet eller lageret: tidsavbrudd, brutt forbindelse, en linje som står stille, eller lageret svarte feil.
    Nett(String),
    /// Portalen svarte noe vi ikke kunne bruke. `status` 0 = svaret manglet det vi trengte.
    Server { status: u16, tekst: String },
    /// Den lokale fila (eller kilden) kunne ikke åpnes, leses eller skrives.
    Disk(String),
    /// En ny utgave (`versjonAv`) ble avvist. `kode` er serverens `versjonAvFeil` (ugyldig, avledning, ukjent, slettet,
    /// frosset, midlertidig). Alt annet enn `midlertidig` er endelig, og tilstanden er da slettet. `ryddet` = noe ble
    /// fjernet hos serveren (av den, eller av vår `avbryt` mot en eldre portal); ved forsjekken (presign/multipart-start)
    /// er ingenting laget, og den er `true`. `id` = fila finnes alt i skapet (feiekosten rakk å lage raden): bytene er der.
    VersjonAv { kode: String, ryddet: bool, id: Option<String>, tekst: String },
}

impl Feil {
    /// Gir et nytt forsøk på samme handling samme svar? Da er det ingen vits å prøve igjen.
    ///
    /// ⚠ `Server` med 4xx er varig, også 408/429: det er regelen nedlastingen har hatt siden 0.3.0
    /// («Portalen svarte 4…» ga opp med en gang), og den skal stå. `Nett`, `Disk` og `Borte` er ikke varige: et nytt
    /// forsøk kan lykkes (et nytt forsøk på en `Borte`-opplasting begynner på nytt).
    pub fn varig(&self) -> bool {
        match self {
            Feil::Avbrutt | Feil::IkkeTilgang(_) | Feil::KvoteFull(_) | Feil::Sperret(_) | Feil::DelingLaast(_) => true,
            Feil::Borte(_) | Feil::Nett(_) | Feil::Disk(_) => false,
            Feil::Server { status, .. } => (400..500).contains(status),
            Feil::VersjonAv { kode, .. } => kode != "midlertidig",
        }
    }

    /// Kortform for `Feil::Server`.
    pub fn server(status: u16, tekst: impl Into<String>) -> Feil { Feil::Server { status, tekst: tekst.into() } }
}

impl std::fmt::Display for Feil {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Feil::Avbrutt => f.write_str("Avbrutt"),
            // Faste signaler frontenden kjenner igjen: «DELING_LAAST» fra nedlastingen, «DELING_LAAST:<lås>» fra hent_deling.
            Feil::DelingLaast(t) if t.is_empty() => f.write_str("DELING_LAAST"),
            Feil::DelingLaast(t) => write!(f, "DELING_LAAST:{t}"),
            Feil::IkkeTilgang(t) | Feil::KvoteFull(t) | Feil::Sperret(t) | Feil::Borte(t) | Feil::Nett(t) | Feil::Disk(t) => f.write_str(t),
            Feil::Server { tekst, .. } | Feil::VersjonAv { tekst, .. } => f.write_str(tekst),
        }
    }
}

impl std::error::Error for Feil {}

/// Transfers kommandoer gir `Result<_, String>`: `?` gjør feilen om til teksten, den samme som før.
impl From<Feil> for String {
    fn from(e: Feil) -> String { e.to_string() }
}

/// Feil fra den lokale fila (åpne, lese, søke). Teksten er io-feilens egen, som før.
pub(crate) fn disk(e: std::io::Error) -> Feil { Feil::Disk(format!("{e}")) }
