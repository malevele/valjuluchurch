param(
    [string]$ProjectRoot,
    [string]$OutputDir
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($ProjectRoot)) {
    $ProjectRoot = Split-Path $PSScriptRoot -Parent
}
if ([string]::IsNullOrWhiteSpace($OutputDir)) {
    $OutputDir = Split-Path $ProjectRoot -Parent
}

if (-not (Test-Path $ProjectRoot)) {
    throw "Project root not found: $ProjectRoot"
}
if (-not (Test-Path $OutputDir)) {
    throw "Output dir not found: $OutputDir"
}

$stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$zipPath = Join-Path $OutputDir "church_finance_safe_deploy_$stamp.zip"
$stage = Join-Path $env:TEMP "church_finance_stage"

if (Test-Path $stage) {
    Remove-Item $stage -Recurse -Force
}
New-Item -ItemType Directory -Path $stage | Out-Null

# Copy project to staging, excluding db/cache/venv/old zip archives.
& robocopy $ProjectRoot $stage /E /XD instance __pycache__ .git .venv venv /XF *.db *.sqlite *.sqlite3 *.zip
$rc = $LASTEXITCODE
if ($rc -ge 8) {
    throw "robocopy failed with exit code $rc"
}

if (Test-Path $zipPath) {
    Remove-Item $zipPath -Force
}
Compress-Archive -Path (Join-Path $stage "*") -DestinationPath $zipPath -Force

Write-Host "OK: created safe deploy zip"
Get-Item $zipPath | Select-Object FullName, Length, LastWriteTime
