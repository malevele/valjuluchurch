@echo off
chcp 65001 > nul
echo ════════════════════════════════════════════════
echo   瓦酪露教會財務系統 — Cloudflare 公開分享
echo   不需帳號，立即取得公開網址！
echo ════════════════════════════════════════════════
echo.

REM 確認 cloudflared 存在
if not exist "..\cloudflared.exe" (
    echo [!] 找不到 cloudflared.exe
    echo     請將 cloudflared.exe 放在 church_finance 上層資料夾
    pause
    exit /b
)

echo [1] 啟動財務系統（localhost:5000）...
start /B python app.py
timeout /t 3 /nobreak > nul

echo [2] 建立 Cloudflare 公開通道中...
echo.
echo ━━━ 公開網址會在下方 trycloudflare.com 那行顯示 ━━━
echo ━━━ 複製 https://xxxx.trycloudflare.com 分享給他人 ━━━
echo.
echo     按 Ctrl+C 停止分享
echo.
..\cloudflared.exe tunnel --url http://localhost:5000
