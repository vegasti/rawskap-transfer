// Klientene, takene, omforsøkene og stoppvakten — alt nettet i craten går gjennom dette.
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

/// «Skal denne fila stoppe?» — enten fordi HELE jobben er avbrutt, eller fordi
/// nettopp denne fila er krysset ut. Ett spørsmål, så alle stedene som før
/// bare kjente til jobb-avbrudd nå også lyder enkeltfila.
#[derive(Clone)]
pub struct Stopp { pub global: Arc<AtomicBool>, pub sett: Arc<std::sync::Mutex<std::collections::HashSet<String>>>, pub id: String }
impl Stopp {
    pub fn av(&self) -> bool {
        self.global.load(Ordering::Relaxed)
            || self.sett.lock().map(|s| s.contains(&self.id)).unwrap_or(false)
    }
}

/// Struping som TOKEN-BUCKET.
///
/// ⚠ Den gamle regnet gjennomsnitt fra jobbstart, og det er en felle med
/// ubegrenset minne (31/8, Vegard satte 75 Mb/s og fikk 437 kB/s): hadde jobben
/// alt sendt 168 GB ubegrenset, ville de tatt 5 timer på 75 Mb/s — men jobben
/// hadde kjørt én. Struperen mente den lå TIMER foran skjema og sov taket sitt
/// på 2 s ved hver eneste bit, i praksis full stopp. Samme felle motsatt vei:
/// en jobb som har ligget stille en stund ville fått lov til å bruse av gårde.
///
/// En bøtte fylles med taket som rate og RENNER OVER etter ett sekund. Da har
/// den ingen hukommelse utover den burst-en, og å endre grensa midt i en jobb
/// virker umiddelbart — som brukeren forventer av et felt hen nettopp skrev i.
pub struct Struper { mbps: Arc<AtomicU64>, bøtte: tokio::sync::Mutex<(std::time::Instant, f64)> }
impl Struper {
    pub fn ny(mbps: Arc<AtomicU64>) -> Arc<Self> {
        Arc::new(Self { mbps, bøtte: tokio::sync::Mutex::new((std::time::Instant::now(), 0.0)) })
    }
    pub async fn tell(&self, n: u64) {
        let tak = self.mbps.load(Ordering::Relaxed);
        if tak == 0 { return; }
        let bps = tak as f64 * 1_000_000.0 / 8.0; // Mb/s → bytes/s
        let vent = {
            let mut b = self.bøtte.lock().await;
            let na = std::time::Instant::now();
            // Fyll på for tiden som gikk, men aldri mer enn ett sekunds burst.
            b.1 = (b.1 + na.duration_since(b.0).as_secs_f64() * bps).min(bps);
            b.0 = na;
            b.1 -= n as f64;
            // Negativ saldo = vi lånte; hver låner sover for sin egen andel, så
            // parallelle overføringer deler ventetiden i stedet for å doble den.
            if b.1 < 0.0 { -b.1 / bps } else { 0.0 }
        };
        if vent > 0.0 { tokio::time::sleep(std::time::Duration::from_secs_f64(vent.min(5.0))).await; }
    }
}

/// Klienten for portal-kall med nøkkelen.
///
/// ⚠ `ua` er appens User-Agent («RawskapTransfer/0.3.7») og MÅ komme fra appen: et
/// `env!("CARGO_PKG_VERSION")` her ville gitt cratens versjon, og portalloggene hadde vist feil app.
pub fn klient(nokkel: &str, ua: &'static str) -> Result<reqwest::Client, String> {
    let mut h = reqwest::header::HeaderMap::new();
    // API-nøkkel (rsk_live_…) = Bearer; app-nøkkel (signert sesjon) = cookie.
    if nokkel.starts_with("rsk_live_") {
        h.insert(reqwest::header::AUTHORIZATION, format!("Bearer {}", nokkel).parse().map_err(|e| format!("{e}"))?);
    } else {
        let cookie = format!("kunde_sesjon={}", nokkel);
        h.insert(reqwest::header::COOKIE, cookie.parse().map_err(|e| format!("{e}"))?);
    }
    h.insert(reqwest::header::USER_AGENT, reqwest::header::HeaderValue::from_static(ua));
    reqwest::Client::builder()
        .default_headers(h)
        .redirect(reqwest::redirect::Policy::none()) // vi følger 302 selv (uten cookie)
        .connect_timeout(TILKOBLING_TAK)
        // Portal-kall og nedlasting via portalen. Lese-taket nullstilles for hver bit av SVARET, så
        // lange nedlastinger er trygge. Det brukes IKKE på opplastings-klienten — se bare_opp.
        .read_timeout(PORTAL_LESE_TAK)
        .build()
        .map_err(nettfeil)
}

// ── NETT SOM TÅLER VIRKELIGHETEN (0.3.0) ─────────────────────────────────────────────────────
//
// Før 0.3.0 hadde ingen forespørsel tidsavbrudd, og tålmodigheten ved feil var tre forsøk med 1,2 s
// mellom. Et wifi-hopp på et halvt minutt felte fila, og en TCP-forbindelse som bare sluttet å
// bevege seg hang for alltid — bare pause fikk den løs. Nå:
//  - tilkobling har tak (15 s), og svar fra portalen har lese-tak;
//  - opplastinger har en STOPP-VAKT som følger bytene, ikke klokka: en treg linje er ikke en død linje;
//  - feil ventes ut med økende pauser (2, 4, 8, 16, 32 s), og er nettet borte, venter vi på at det
//    kommer tilbake (inntil ti minutter) i stedet for å brenne forsøkene mens det er nede.

/// Tidsenheten i millisekunder. 1000 i appen; testene skrur den ned så pausene tar millisekunder.
pub static TIDSENHET_MS: AtomicU64 = AtomicU64::new(1000);
pub(crate) fn enheter(n: u64) -> std::time::Duration { std::time::Duration::from_millis(n.saturating_mul(TIDSENHET_MS.load(Ordering::Relaxed))) }

pub const TILKOBLING_TAK: std::time::Duration = std::time::Duration::from_secs(15);
pub const PORTAL_LESE_TAK: std::time::Duration = std::time::Duration::from_secs(90);
pub const NED_LESE_TAK: std::time::Duration = std::time::Duration::from_secs(60);
/// Så lenge kan en opplasting stå helt stille før den kuttes og prøves igjen (tidsenheter).
pub const STOPP_TAK: u64 = 60;
/// Så lenge venter vi på at nettet kommer tilbake før et forsøk gis opp (tidsenheter).
pub const NETT_VENT_TAK: u64 = 600;
pub const DEL_FORSOK: u32 = 6;
pub const ENKEL_FORSOK: u32 = 5;
pub const PORTAL_FORSOK: u32 = 5;
pub const NED_FORSOK: u32 = 6;

/// Klienten for PUT rett til lageret.
///
/// ⚠ Bare tilkoblingstak, IKKE read_timeout. I reqwest starter den klokka når forespørselen sendes og
/// nullstilles ikke mens kroppen går ut (lest i kilden, 0.12.28) — en 16 MB-del over en treg linje
/// ville blitt kuttet midt i. Døde forbindelser tas av med_stoppvakt, som ser på bytene i stedet.
pub fn bare_opp() -> reqwest::Client { reqwest::Client::builder().connect_timeout(TILKOBLING_TAK).build().unwrap_or_default() }
/// Klienten for nedlasting fra lageret. Her ER lese-taket riktig: det nullstilles for hver bit som
/// kommer, så det kutter bare en forbindelse der ingenting har kommet på et minutt — og neste
/// forsøk fortsetter fra .part.
pub fn bare_ned() -> reqwest::Client { reqwest::Client::builder().connect_timeout(TILKOBLING_TAK).read_timeout(NED_LESE_TAK).build().unwrap_or_default() }
/// For kall uten nøkkel (kobling, delinger).
pub fn enkel_klient() -> reqwest::Client { reqwest::Client::builder().connect_timeout(TILKOBLING_TAK).read_timeout(PORTAL_LESE_TAK).build().unwrap_or_default() }

pub fn bare_uten_redirect() -> reqwest::Client { reqwest::Client::builder().redirect(reqwest::redirect::Policy::none()).connect_timeout(TILKOBLING_TAK).read_timeout(NED_LESE_TAK).build().unwrap_or_default() }

/// Svar som betyr «prøv igjen om litt», ikke «nei».
pub fn forbigaaende(status: u16) -> bool { matches!(status, 408 | 425 | 429 | 500 | 502 | 503 | 504) }

/// Pause før omforsøk nr. `n` (0 = det første): 2, 4, 8, 16, 32, 32 … tidsenheter.
pub fn pause_enheter(n: u32) -> u64 { 2u64 << n.min(4) }
pub fn pause_foer_forsok(n: u32) -> std::time::Duration {
    // Litt tilfeldig spredning (inntil et halvt sekund), så parallelle filer ikke slår til i takt
    // mot en server som alt sliter.
    let spredning = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| (d.subsec_millis() % 500) as u64).unwrap_or(0);
    enheter(pause_enheter(n)) + std::time::Duration::from_millis(spredning * TIDSENHET_MS.load(Ordering::Relaxed) / 1000)
}

/// Sover, men våkner med én gang brukeren avbryter — en pause på 32 s mellom to forsøk skal ikke
/// holde igjen «Pause» eller «Avbryt».
pub async fn sov(d: std::time::Duration, avbryt: Option<&Stopp>) {
    let slutt = std::time::Instant::now() + d;
    loop {
        if avbryt.map(|a| a.av()).unwrap_or(false) { return; }
        let igjen = slutt.saturating_duration_since(std::time::Instant::now());
        if igjen.is_zero() { return; }
        tokio::time::sleep(igjen.min(std::time::Duration::from_millis(250))).await;
    }
}

/// Kjører en overføring med STOPP-VAKT. Har `fram` (bytene som er sendt) ikke flyttet seg på
/// STOPP_TAK, kuttes den og gir `None`. Avbryter brukeren, kuttes den også — innen ett sekund, i
/// stedet for først når delen er ferdig.
///
/// ⚠ Vakta ser på bytene, ikke klokka. En del over en struppet linje kan godt ta minutter; det er
/// ikke en feil. En forbindelse der ingenting har rørt seg på et minutt, er det.
pub async fn med_stoppvakt<T>(fram: &AtomicU64, avbryt: Option<&Stopp>, fut: impl std::future::Future<Output = T>) -> Option<T> {
    tokio::pin!(fut);
    let mut sist = fram.load(Ordering::Relaxed);
    let mut stille_siden = std::time::Instant::now();
    loop {
        tokio::select! {
            r = &mut fut => return Some(r),
            _ = tokio::time::sleep(enheter(1)) => {
                if avbryt.map(|a| a.av()).unwrap_or(false) { return None; }
                let n = fram.load(Ordering::Relaxed);
                if n != sist { sist = n; stille_siden = std::time::Instant::now(); }
                else if stille_siden.elapsed() >= enheter(STOPP_TAK) { return None; }
            }
        }
    }
}

/// Venter til portalen svarer igjen. Et hvilket som helst svar — også en feilkode — betyr at linja
/// er oppe. Gir `false` hvis nettet ikke kom tilbake innen taket, eller brukeren avbrøt imens.
/// `meld_venter` kalles én gang, i det vi faktisk begynner å vente, så lista kan si det.
pub async fn vent_paa_nett(portal: &str, avbryt: Option<&Stopp>, meld_venter: &(dyn Fn() + Send + Sync)) -> bool {
    let Ok(k) = reqwest::Client::builder().connect_timeout(enheter(5)).timeout(enheter(10)).build() else { return true };
    let url = format!("{}/api/transfer/versjon", portal.trim_end_matches('/'));
    let start = std::time::Instant::now();
    let mut venter = false;
    loop {
        if avbryt.map(|a| a.av()).unwrap_or(false) { return false; }
        if k.get(&url).send().await.is_ok() {
            if venter { log::info!("nettet er tilbake etter {} s", start.elapsed().as_secs()); }
            return true;
        }
        if !venter { venter = true; meld_venter(); log::warn!("får ikke kontakt med portalen — venter på nettet"); }
        if start.elapsed() >= enheter(NETT_VENT_TAK) { log::warn!("nettet kom ikke tilbake på {} s — gir opp forsøket", start.elapsed().as_secs()); return false; }
        tokio::time::sleep(enheter(5)).await;
    }
}

/// POST mot /api/rawskap/opplasting med omforsøk.
///
/// Nettfeil og forbigående serverfeil (5xx, 429) prøves igjen, og er nettet borte, ventes det ut.
/// Alle andre svar — også 4xx — er et SVAR og går rett tilbake som (status, json); det er kallerens
/// sak å tolke dem. Før 0.3.0 ga ett hikk under signeringen av neste bolk opp hele fila.
pub async fn portal_post(k: &reqwest::Client, portal: &str, body: &serde_json::Value, avbryt: Option<&Stopp>) -> Result<(u16, serde_json::Value), String> {
    let url = format!("{portal}/api/rawskap/opplasting");
    let handling = body["action"].as_str().unwrap_or("?");
    let mut n = 0u32;
    loop {
        let feil = match k.post(&url).json(body).send().await {
            Ok(r) => {
                let st = r.status().as_u16();
                // Siste forsøk: gi kalleren svaret likevel, så serverens egen feiltekst når fram.
                if !forbigaaende(st) || n + 1 >= PORTAL_FORSOK { return Ok((st, r.json().await.unwrap_or(serde_json::Value::Null))); }
                format!("Portalen svarte {st}")
            }
            Err(e) => nettfeil(e),
        };
        if avbryt.map(|a| a.av()).unwrap_or(false) { return Err("Avbrutt".into()); }
        if n + 1 >= PORTAL_FORSOK { return Err(feil); }
        log::warn!("{handling} feilet ({feil}) — prøver igjen");
        vent_paa_nett(portal, avbryt, &|| {}).await;
        sov(pause_foer_forsok(n), avbryt).await;
        n += 1;
    }
}

/// Be serveren glemme opplastinger vi gir opp: pending-raden, multiparten og et halvt objekt.
/// Best-effort — feiekosten tar resten om dette ikke når fram.
pub async fn forkast_nokler(k: &reqwest::Client, portal: &str, nokler: &[String]) {
    if nokler.is_empty() { return; }
    let _ = k.post(format!("{portal}/api/rawskap/opplasting")).json(&serde_json::json!({ "action": "avbryt", "originalKeys": nokler })).send().await;
}

/// Nettverksfeil UTEN URL-en.
///
/// ⚠ reqwest tar med hele URL-en i sin `Display`, og en presignert R2-URL ER en gyldig
/// skrive-legitimasjon i et kvarter. Den havnet dermed rett i feilmeldinga brukeren ser, som går
/// videre inn i skjermbilder, jobbkøen og loggfiler (Vegard 17/9: «fikk en vanvittig lang
/// feilmelding i transfer» — med signaturen i). `without_url()` stripper den, og det som er igjen
/// skiller fortsatt tidsavbrudd fra avvist forbindelse, som er det man faktisk trenger å vite.
/// (0.3.0: brukt på ALLE nettkall, ikke bare to. Også portal-adresser bærer hemmeligheter — en
/// delings-token eller en adgangsnøkkel i spørringen.)
pub fn nettfeil(e: reqwest::Error) -> String {
    let u = e.without_url();
    if u.is_timeout() { return "Tidsavbrudd — prøv igjen".into(); }
    if u.is_connect() { return "Fikk ikke kontakt — sjekk nettforbindelsen".into(); }
    if u.is_body() || u.is_request() { return "Mistet forbindelsen underveis — prøv igjen".into(); }
    if u.is_decode() { return "Uventet svar fra serveren".into(); }
    if let Some(s) = u.status() { return format!("Serveren svarte {}", s.as_u16()); }
    format!("Nettverksfeil: {u}")
}

pub fn urlenc(s: &str) -> String { s.bytes().map(|b| match b { b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => (b as char).to_string(), _ => format!("%{b:02X}") }).collect() }
