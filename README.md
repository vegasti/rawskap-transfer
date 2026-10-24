# Rawskap Transfer

Skrivebordsapp (Tauri 2) for store opp- og nedlastinger mot [Rawskap](https://rawskap.no) —
Raw Studios' mediebibliotek. Kø med gjenopptak, mappesynk, semantisk søk i skapet,
`rawskap://`-deep-links for delinger, og poster/scrubbe-sprites via ffmpeg.

## Bygg

```bash
npm ci
npm run tauri dev    # utvikling
npm run tauri build  # produksjonsbygg
```

ffmpeg-sidecaren ligger ikke i repoet: legg et statisk bygg i `src-tauri/binaries/`
som `ffmpeg-x86_64-pc-windows-msvc.exe` (Windows) eller `ffmpeg-aarch64-apple-darwin`
(macOS). CI (`.github/workflows/bygg.yml`) laster den ned selv, kjører testene, bygger
Windows og Mac (arm64 + Intel), signerer Mac-byggene med Developer ID og sender dem til notarisering.

## Tester

```bash
npm test             # Rust (craten + appen) + grensesnitt-riggen (headless Chrome)
```

Opp- og nedlastingsløypa bor i craten `skapflyt` (`crates/skapflyt/`, en frittstående pakke —
roten er ikke et workspace). Testene der kjører løypa mot en falsk portal og et falskt lager som kan
kutte forbindelsen, svare 500/403 eller slutte å lese — se `crates/skapflyt/src/tester.rs` og
`testportal.rs` (feature `testportal`). De kjøres også i CI (`.github/workflows/skapflyt.yml`)
når craten endres. Appens egne tester står i `mod tester` i `src-tauri/src/lib.rs`.
Loggen ligger i appens loggmappe (Innstillinger → Om → «Åpne loggmappa»).

Innlesingen fra minnekort (`skapflyt::innlesing`, som rawLight også skal bruke) testes mot falske kort
i midlertidige mapper. Feilinnsettingen `innlesing::uhell` gir full disk, en kopi som endres etter
skriving og avbrudd midt i en fil. Ingen test rører et ekte kort.

## Lisens

MIT (se `LICENSE`). ffmpeg distribueres som egen sidecar-binær under egen lisens
(GPL/LGPL avhengig av bygget) — kildekode hos [ffmpeg.org](https://ffmpeg.org) og
byggene vi bruker i CI hos [ffmpeg.martin-riedl.de](https://ffmpeg.martin-riedl.de).
Innlesingen og kortregelen i `crates/skapflyt` bygger på kode fra LightCraft (MIT eller Apache-2.0),
se `crates/skapflyt/NOTICE`.
