// Navn og typer: trygge sti-ledd, MIME fra filendelsen og mappe-id slik serveren vil ha den.

/// Gjør en streng trygg å bruke som ETT ledd i en sti.
///
/// ⚠ Det holder ikke å bytte ut skilletegnene. Et navn på nøyaktig «.» eller «..» slapp gjennom før
/// (funnet av `trygt_navn_slipper_ikke_ut_av_mappa` 17/9, første gang testen kjørte): da ble
/// `mappe.join("..")` til FORELDREMAPPA. Stien i seg selv er filtrert lenger oppe, så dette ga i
/// praksis en forvirrende feil og ikke en traversering — men en funksjon som heter «trygt navn» skal
/// ikke overlate den vurderingen til kallstedet. Navn som bare er punktum, og Windows' reserverte
/// enhetsnavn (som aldri kan opprettes som fil), får et understrek foran.
pub fn trygt_navn(s: &str) -> String {
    let rent: String = s.chars().map(|c| if "\\/:*?\"<>|".contains(c) { '_' } else { c }).collect();
    let rent = rent.trim().to_string();
    if rent.is_empty() || rent.chars().all(|c| c == '.') { return format!("_{rent}"); }
    let stamme = rent.split('.').next().unwrap_or("").to_ascii_uppercase();
    const RESERVERTE: [&str; 22] = ["CON", "PRN", "AUX", "NUL",
        "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9",
        "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9"];
    if RESERVERTE.contains(&stamme.as_str()) { return format!("_{rent}"); }
    rent
}

pub fn mime_fra(navn: &str) -> &'static str {
    let e = navn.rsplit('.').next().unwrap_or("").to_ascii_lowercase();
    match e.as_str() {
        "jpg" | "jpeg" => "image/jpeg", "png" => "image/png", "webp" => "image/webp", "gif" => "image/gif", "heic" => "image/heic", "tif" | "tiff" => "image/tiff",
        "dng" => "image/x-adobe-dng", "arw" => "image/x-sony-arw", "cr2" => "image/x-canon-cr2", "cr3" => "image/x-canon-cr3", "nef" => "image/x-nikon-nef", "raf" => "image/x-fuji-raf",
        "mp4" => "video/mp4", "mov" => "video/quicktime", "mxf" => "application/mxf", "pdf" => "application/pdf", _ => "application/octet-stream",
    }
}

pub fn mappe_json(id: &str) -> serde_json::Value { if id.is_empty() { serde_json::Value::Null } else { serde_json::Value::String(id.to_string()) } }
