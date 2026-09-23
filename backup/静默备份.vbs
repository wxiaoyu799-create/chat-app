' 给计划任务用的：后台悄悄跑一次备份，不弹黑窗口。
' 手动跑请双击 备份.bat（那个会显示进度）。
Set sh = CreateObject("WScript.Shell")
Set fso = CreateObject("Scripting.FileSystemObject")
here = fso.GetParentFolderName(WScript.ScriptFullName)
sh.CurrentDirectory = here
' 0 = 隐藏窗口，True = 等它跑完
sh.Run "cmd /c node """ & here & "\backup.js"" >> """ & here & "\自动备份日志.txt"" 2>&1", 0, True
