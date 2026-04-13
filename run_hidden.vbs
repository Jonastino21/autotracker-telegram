' ═══════════════════════════════════════════════════════════
'   KAROKA Attendance Tracker — Lanceur silencieux Windows
'   Exécute start_tracker.bat sans fenêtre visible
'   À placer dans : shell:startup  (démarrage automatique)
' ═══════════════════════════════════════════════════════════

Dim WinScriptHost
Set WinScriptHost = CreateObject("WScript.Shell")

' Chemin absolu vers le dossier du projet
' Modifier si le projet est dans un autre dossier
Dim projectPath
projectPath = CreateObject("Scripting.FileSystemObject").GetParentFolderName(WScript.ScriptFullName)

' Lancer le .bat sans fenêtre (0 = fenêtre cachée)
WinScriptHost.Run Chr(34) & projectPath & "\start_tracker.bat" & Chr(34), 0, False

Set WinScriptHost = Nothing
