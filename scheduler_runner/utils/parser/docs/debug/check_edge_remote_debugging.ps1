# Проверка: разрешает ли Edge DevTools remote debugging (нужен Selenium) на профиле по умолчанию и на отдельном каталоге.
# Запуск: powershell -ExecutionPolicy Bypass -File check_edge_remote_debugging.ps1
# Перед запуском закройте Edge текущего пользователя, включая фоновые процессы (иначе запуск уйдет в уже открытый Edge).
# Ожидаемо с 2026 года: default -> НЕ доступен ("requires a non-default data directory"), separate -> OK.

$edge = @("${env:ProgramFiles(x86)}\Microsoft\Edge\Application\msedge.exe", "$env:ProgramFiles\Microsoft\Edge\Application\msedge.exe") |
    Where-Object { Test-Path $_ } | Select-Object -First 1
if (-not $edge) { Write-Host "msedge.exe не найден"; exit 1 }
Write-Host "Edge: $edge  версия: $((Get-Item $edge).VersionInfo.ProductVersion)"

$running = Get-CimInstance Win32_Process -Filter "Name='msedge.exe'" |
    Where-Object { (Invoke-CimMethod -InputObject $_ -MethodName GetOwner).User -eq $env:USERNAME }
if ($running) { Write-Host "Edge текущего пользователя запущен ($(@($running).Count) процессов). Закройте его и повторите."; exit 1 }

$marker = '--edge-remote-debugging-probe'  # неизвестный Edge флаг: по нему находим и завершаем только свои процессы

function Test-RemoteDebugging([string]$label, [string]$userDataDir) {
    $errFile = Join-Path $env:TEMP "edge_probe_$label.txt"
    $started = Get-Date
    $edgeArgs = @('--headless=new', '--no-first-run', '--remote-debugging-port=0', "--user-data-dir=`"$userDataDir`"", $marker, 'about:blank')
    Start-Process -FilePath $edge -ArgumentList $edgeArgs -RedirectStandardError $errFile -WindowStyle Hidden | Out-Null
    Start-Sleep -Seconds 10
    $portFile = Join-Path $userDataDir 'DevToolsActivePort'
    if ((Test-Path $portFile) -and ((Get-Item $portFile).LastWriteTime -ge $started)) {
        Write-Host "$label : DevTools OK (порт $(Get-Content $portFile -TotalCount 1))"
    } else {
        Write-Host "$label : DevTools НЕ доступен"
    }
    Get-Content $errFile -ErrorAction SilentlyContinue | Select-String 'DevTools|non-default' | ForEach-Object { Write-Host "    $_" }
    Get-CimInstance Win32_Process -Filter "Name='msedge.exe'" | Where-Object { $_.CommandLine -like "*$marker*" } |
        ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }
    Start-Sleep -Seconds 2
    Remove-Item $errFile -ErrorAction SilentlyContinue
}

Test-RemoteDebugging 'default' (Join-Path $env:LOCALAPPDATA 'Microsoft\Edge\User Data')

$tempDir = Join-Path $env:TEMP 'edge_remote_debugging_probe'
Remove-Item $tempDir -Recurse -Force -ErrorAction SilentlyContinue
New-Item -ItemType Directory $tempDir | Out-Null
Test-RemoteDebugging 'separate' $tempDir
Remove-Item $tempDir -Recurse -Force -ErrorAction SilentlyContinue
