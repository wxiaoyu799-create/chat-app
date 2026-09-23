' Runs one backup quietly in the background (used by the Windows scheduled task).
' Double-click the .bat instead if you want to watch the progress.
Set sh = CreateObject("WScript.Shell")
Set fso = CreateObject("Scripting.FileSystemObject")
here = fso.GetParentFolderName(WScript.ScriptFullName)
sh.CurrentDirectory = here
sh.Run "cmd /c node backup.js >> auto-backup-log.txt 2>&1", 0, True
