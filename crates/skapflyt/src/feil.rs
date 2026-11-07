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
    /// Disken filene skrives til eller leses fra, kan ikke brukes: den er full, eller den er borte (en NAS som falt ut, en
    /// disk som ble koblet fra, og som ikke kom tilbake mens vi ventet). Gjelder ALLE filene på den, ikke bare denne (6/11).
    Volum(String),
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
            Feil::Avbrutt | Feil::IkkeTilgang(_) | Feil::KvoteFull(_) | Feil::Sperret(_) | Feil::DelingLaast(_) | Feil::Volum(_) => true,
            Feil::Borte(_) | Feil::Nett(_) | Feil::Disk(_) => false,
            Feil::Server { status, .. } => (400..500).contains(status),
            Feil::VersjonAv { kode, .. } => kode != "midlertidig",
        }
    }

    /// Får de ANDRE filene i jobben samme svar? Da skal jobben ikke prøve dem én for én (6/11): før ga en utgått nøkkel,
    /// et fullt skap eller en full disk én feil per fil — 144 like feil i en nedlasting, og en opplasting på 1 778 filer
    /// som ville prøvd hver av dem to ganger mot en full kvote. Kalleren lar filene som alt går gjøre seg ferdig, og gir
    /// resten denne feilen uten å røre nettet.
    pub fn gjelder_jobben(&self) -> bool {
        matches!(self, Feil::IkkeTilgang(_) | Feil::KvoteFull(_) | Feil::DelingLaast(_) | Feil::Volum(_))
    }

    /// Mappa finnes ikke lenger, og kom ikke tilbake mens vi ventet (`vent_paa_mappe`).
    pub fn volum_borte(mappe: &std::path::Path) -> Feil {
        Feil::Volum(format!("Finner ikke {} — er disken koblet fra?", mappe.display()))
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
            Feil::IkkeTilgang(t) | Feil::KvoteFull(t) | Feil::Sperret(t) | Feil::Borte(t) | Feil::Nett(t) | Feil::Disk(t) | Feil::Volum(t) => f.write_str(t),
            Feil::Server { tekst, .. } | Feil::VersjonAv { tekst, .. } => f.write_str(tekst),
        }
    }
}

impl std::error::Error for Feil {}

/// Transfers kommandoer gir `Result<_, String>`: `?` gjør feilen om til teksten, den samme som før.
impl From<Feil> for String {
    fn from(e: Feil) -> String { e.to_string() }
}

/// Feil fra den lokale fila (åpne, lese, søke). Teksten er io-feilens egen, som før — bortsett fra en full disk, som er
/// `Volum`: den gjelder alle filene, og et nytt forsøk hjelper ikke før noen har ryddet plass (6/11).
pub(crate) fn disk(e: std::io::Error) -> Feil {
    if er_full(&e) { Feil::Volum("Ikke nok plass på disken".into()) } else { Feil::Disk(format!("{e}")) }
}

/// Full disk, eller brukerens kvote på disken. Råkodene og ikke `ErrorKind::StorageFull`, som kom i Rust 1.83 (craten
/// lover 1.77).
pub(crate) fn er_full(e: &std::io::Error) -> bool {
    let kode = e.raw_os_error();
    // ERROR_HANDLE_DISK_FULL, ERROR_DISK_FULL
    #[cfg(windows)] { matches!(kode, Some(39) | Some(112)) }
    // ENOSPC, EDQUOT
    #[cfg(target_os = "macos")] { matches!(kode, Some(28) | Some(69)) }
    #[cfg(all(unix, not(target_os = "macos")))] { matches!(kode, Some(28) | Some(122)) }
    #[cfg(not(any(windows, unix)))] { let _ = kode; false }
}
