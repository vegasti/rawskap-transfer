; Navnebyttet Rawskap Transfer -> Rawbyte Transfer (13/11-2026).
;
; Tauris NSIS-mal bruker PRODUCTNAME til alt som peker ut EN installasjon:
; mappa ($LOCALAPPDATA\<navn>), avinstallasjonsnokkelen, Software\<utgiver>\<navn>
; og snarveiene. Uten denne fila ville det nye navnet lagt seg VED SIDEN AV den
; gamle appen: to mapper, to linjer i "Apper", gamle snarveier som peker pa en
; gammel versjon, og autostart som starter feil utgave.
;
; Regelen: finnes den gamle appen, oppgraderes den DER DEN STAR. Da er exe-stien
; uendret, sa autostart (Run-verdien), festede snarveier og rawskap://-handtereren
; fortsetter a virke. Etterpa ryddes det som bar det gamle navnet.
;
; (!) Bare ASCII i denne fila: NSIS leser en inkludert fil uten BOM i systemets tegnsett.
; (!) Fila inkluderes FOR malen definerer PRODUCTNAME m.fl. Funksjoner her kan derfor
;     ikke bruke malens definisjoner - navnene star som egne definisjoner, og
;     NSIS_HOOK_PREINSTALL stopper bygget hvis de ikke stemmer med tauri.conf.json.

!define NB_GAMMELT "Rawskap Transfer"
!define NB_NYTT "Rawbyte Transfer"
!define NB_UTGIVER "rawstudios"
!define NB_EXE "rawskap-transfer.exe"
!define NB_GAMMEL_NOKKEL "Software\${NB_UTGIVER}\${NB_GAMMELT}"
!define NB_NY_NOKKEL "Software\${NB_UTGIVER}\${NB_NYTT}"
!define NB_GAMMEL_AVINST "Software\Microsoft\Windows\CurrentVersion\Uninstall\${NB_GAMMELT}"

Var NbGammelMappe

; Setter $NbGammelMappe til mappa den gamle appen star i - eller "" nar det ikke er
; noe a flytte: ingen gammel app, exe-en er borte, eller det nye navnet er alt installert.
!macro NB_FINN_GAMMEL
  StrCpy $NbGammelMappe ""
  Push $0
  ReadRegStr $0 SHCTX "${NB_NY_NOKKEL}" ""
  ${If} $0 == ""
    ReadRegStr $NbGammelMappe SHCTX "${NB_GAMMEL_NOKKEL}" ""
    ${If} $NbGammelMappe != ""
      ${IfNot} ${FileExists} "$NbGammelMappe\${NB_EXE}"
        StrCpy $NbGammelMappe ""
      ${EndIf}
    ${EndIf}
  ${EndIf}
  Pop $0
!macroend

; Snarveien med det gamle navnet far det nye - eller fjernes nar installeren alt har
; laget en ny. Bare snarveier som peker pa VAR exe rores.
!macro NB_SNARVEI MAPPE
  !insertmacro IsShortcutTarget "${MAPPE}\${NB_GAMMELT}.lnk" "$INSTDIR\${NB_EXE}"
  Pop $0
  ${If} $0 = 1
    ${If} ${FileExists} "${MAPPE}\${NB_NYTT}.lnk"
      Delete "${MAPPE}\${NB_GAMMELT}.lnk"
    ${Else}
      Rename "${MAPPE}\${NB_GAMMELT}.lnk" "${MAPPE}\${NB_NYTT}.lnk"
    ${EndIf}
  ${EndIf}
!macroend

; Med vinduer: katalogsida skal vise mappa appen faktisk havner i. Velger brukeren
; en annen mappe der, respekteres det (da blir den gamle appen staende urort).
!define MUI_CUSTOMFUNCTION_GUIINIT NbGuiInit
Function NbGuiInit
  ${If} $INSTDIR == "$LOCALAPPDATA\${NB_NYTT}"
    !insertmacro NB_FINN_GAMMEL
    ${If} $NbGammelMappe != ""
      StrCpy $INSTDIR $NbGammelMappe
    ${EndIf}
  ${EndIf}
FunctionEnd

; Rett etter "SetOutPath $INSTDIR". Stille installasjon (/S) kjorer aldri NbGuiInit,
; sa flyttingen ma kunne skje her ogsa.
!macro NSIS_HOOK_PREINSTALL
  !if "${PRODUCTNAME}" != "${NB_NYTT}"
    !error "navnebytte.nsh: NB_NYTT ma vaere lik productName i tauri.conf.json"
  !endif
  !if "${MANUFACTURER}" != "${NB_UTGIVER}"
    !error "navnebytte.nsh: NB_UTGIVER ma vaere lik utgiveren (andre ledd i identifier)"
  !endif
  !if "${MAINBINARYNAME}.exe" != "${NB_EXE}"
    !error "navnebytte.nsh: NB_EXE ma vaere lik hovedprogrammets filnavn"
  !endif

  ${If} $INSTDIR == "$LOCALAPPDATA\${NB_NYTT}"
    !insertmacro NB_FINN_GAMMEL
    ${If} $NbGammelMappe != ""
      ; Ut av den nye mappa for den fjernes (SetOutPath laget den nettopp; RMDir tar bare en tom mappe).
      SetOutPath $NbGammelMappe
      RMDir "$INSTDIR"
      StrCpy $INSTDIR $NbGammelMappe
    ${EndIf}
  ${EndIf}
!macroend

; Etter at det nye navnet er registrert og snarveiene laget: rydd det gamle navnet -
; men bare nar vi faktisk installerte OPPA den gamle appen.
!macro NSIS_HOOK_POSTINSTALL
  ReadRegStr $NbGammelMappe SHCTX "${NB_GAMMEL_NOKKEL}" ""
  ${If} $NbGammelMappe != ""
  ${AndIf} $NbGammelMappe == $INSTDIR
    DeleteRegKey SHCTX "${NB_GAMMEL_AVINST}"
    DeleteRegKey SHCTX "${NB_GAMMEL_NOKKEL}"
    Push $0
    !insertmacro NB_SNARVEI "$SMPROGRAMS"
    !insertmacro NB_SNARVEI "$DESKTOP"
    Pop $0
  ${EndIf}
!macroend

; Autostart heter fortsatt "Rawskap Transfer" i Run-nokkelen (appen later navnet sta,
; se lib.rs, sa de som har slatt det pa beholder det). Malen sletter bare verdien med
; det NYE navnet ved avinstallering.
!macro NSIS_HOOK_POSTUNINSTALL
  ${If} $UpdateMode <> 1
    DeleteRegValue HKCU "Software\Microsoft\Windows\CurrentVersion\Run" "${NB_GAMMELT}"
  ${EndIf}
!macroend
