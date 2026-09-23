@echo off
chcp 65001 >nul
cd /d "%~dp0"
echo.
echo ==== CC 本地备份 ====
echo.
node backup.js
echo.
pause
