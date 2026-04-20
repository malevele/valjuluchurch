@echo off
chcp 65001 > nul
echo ════════════════════════════════════════
echo   瓦酪露教會財務系統 — 公開分享模式
echo ════════════════════════════════════════
echo.

REM 檢查 ngrok 是否設定 auth token
ngrok config check 2>nul | findstr "authtoken" >nul
if errorlevel 1 (
    echo [!] 請先設定 ngrok 驗證碼：
    echo.
    echo     1. 前往 https://ngrok.com 免費註冊帳號
    echo     2. 登入後前往 https://dashboard.ngrok.com/get-started/your-authtoken
    echo     3. 複製 Your Authtoken
    echo     4. 在此視窗執行：ngrok config add-authtoken 貼上您的token
    echo     5. 再次雙擊 run_ngrok.bat
    echo.
    pause
    exit /b
)

echo [1] 啟動教會財務系統...
start /B python app.py
timeout /t 2 /nobreak > nul

echo [2] 建立公開網址中，請稍候...
echo.
echo ════════════════════════════════════════
echo  公開網址將顯示在下方（Forwarding 那行）
echo  格式：https://xxxx.ngrok-free.app
echo  按 Ctrl+C 停止分享
echo ════════════════════════════════════════
echo.
ngrok http 5000
