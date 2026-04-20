param(
    [string]$Folder,
    [int]$KeepSafe = 3
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($Folder)) {
    $projectRoot = Split-Path $PSScriptRoot -Parent
    $Folder = Split-Path $projectRoot -Parent
}

if (-not (Test-Path $Folder)) {
    throw "Folder not found: $Folder"
}
if ($KeepSafe -lt 1) {
    throw "KeepSafe must be >= 1"
}

# 1) Delete all old deploy/redeploy archives.
$legacy = Get-ChildItem -Path $Folder -File -Filter "church_finance_deploy*.zip" -ErrorAction SilentlyContinue
$legacy += Get-ChildItem -Path $Folder -File -Filter "church_finance_redeploy*.zip" -ErrorAction SilentlyContinue
if ($legacy) {
    $legacy | Remove-Item -Force
}

# 2) Keep only latest N safe deploy archives.
$safe = Get-ChildItem -Path $Folder -File -Filter "church_finance_safe_deploy_*.zip" -ErrorAction SilentlyContinue |
    Sort-Object LastWriteTime -Descending

if ($safe.Count -gt $KeepSafe) {
    $safe | Select-Object -Skip $KeepSafe | Remove-Item -Force
}

Write-Host "OK: archive cleanup done"
Get-ChildItem -Path $Folder -File -Filter "church_finance_safe_deploy_*.zip" |
    Sort-Object LastWriteTime -Descending |
    Select-Object Name, Length, LastWriteTime
