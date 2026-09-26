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
npm test             # Rust (cargo test) + grensesnitt-riggen (headless Chrome)
```

Rust-testene kjører opp- og nedlastingsløypa mot en falsk portal og et falskt lager som kan
kutte forbindelsen, svare 500/403 eller slutte å lese — se `mod tester` i `src-tauri/src/lib.rs`.
Loggen ligger i appens loggmappe (Innstillinger → Om → «Åpne loggmappa»).

## Lisens

MIT (se `LICENSE`). ffmpeg distribueres som egen sidecar-binær under egen lisens
(GPL/LGPL avhengig av bygget) — kildekode hos [ffmpeg.org](https://ffmpeg.org) og
byggene vi bruker i CI hos [ffmpeg.martin-riedl.de](https://ffmpeg.martin-riedl.de).
