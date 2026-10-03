<#
.SYNOPSIS
    Открыть веб-интерфейсы сети ПВЗ (роутер, камеры) в отдельном окне Edge через SSH (SOCKS-прокси).

.DESCRIPTION
    Запускается на компьютере администратора (не на ПВЗ). Берет из ~/.ssh/config блок Host pvz<Pvz> и его
    DynamicForward (порт SOCKS-прокси, например 1144 для 144):
      1. если прокси еще не поднят — открывает отдельное окно с `ssh pvz<Pvz>` (там же — PowerShell компьютера ПВЗ;
         в нем отвечать на вопрос об отпечатке и вводить фразу-пароль ключа, если есть) и ждет порт;
      2. запускает отдельный Edge этого ПВЗ: свой профиль (закладки и пароли роутера/камер сохраняются в нем),
         весь трафик через прокси, без расширений (антивирусные расширения ломали страницы роутера), адреса — как в сети
         ПВЗ (http://192.168.0.1), поэтому роутеры и камеры с проверкой адреса открываются полностью.
    Закрыть доступ: закрыть окно Edge и окно ssh (exit).

    Примеры:
        powershell -ExecutionPolicy Bypass -File C:\tools\scheduler\docs\remote_access\open_pvz.ps1 -Pvz 144
        ... open_pvz.ps1 -Pvz 144 -Url http://192.168.0.1, http://192.168.0.109, http://192.168.0.112

    Ярлык на рабочем столе: объект
        powershell.exe -ExecutionPolicy Bypass -File C:\tools\scheduler\docs\remote_access\open_pvz.ps1 -Pvz 144
    Подробности — docs/remote_access/README.md, раздел 5.
#>
param(
    [Parameter(Mandatory = $true)][string]$Pvz,
    [string[]]$Url = @('http://192.168.0.1'),
    [int]$WaitSeconds = 90
)

$ErrorActionPreference = 'Stop'
$hostAlias = if ($Pvz -like 'pvz*') { $Pvz } else { "pvz$Pvz" }

# Порт SOCKS-прокси — из ~/.ssh/config (ssh -G показывает итоговые параметры блока Host)
$sshConfig = & ssh -G $hostAlias 2>$null
$dynamic = $sshConfig | Where-Object { $_ -match '^dynamicforward\s+' } | Select-Object -First 1
if (-not $dynamic) {
    throw "В ~/.ssh/config у Host $hostAlias нет строки DynamicForward (например: DynamicForward 1$($Pvz -replace '\D',''))"
}
$port = [int](($dynamic -split '\s+')[-1] -replace '.*:', '')

function Test-Proxy([int]$p) {
    $client = New-Object Net.Sockets.TcpClient
    try { return ($client.ConnectAsync('127.0.0.1', $p).Wait(500) -and $client.Connected) } catch { return $false } finally { $client.Close() }
}

if (-not (Test-Proxy $port)) {
    Write-Host "Подключение $hostAlias (окно ssh не закрывать, пока нужен доступ)..." -ForegroundColor Cyan
    Start-Process -FilePath 'ssh.exe' -ArgumentList $hostAlias
    $deadline = (Get-Date).AddSeconds($WaitSeconds)
    while (-not (Test-Proxy $port)) {
        if ((Get-Date) -gt $deadline) {
            throw "Прокси 127.0.0.1:$port не поднялся за $WaitSeconds с — посмотрите окно ssh (отпечаток, ключ, сеть)"
        }
        Start-Sleep -Seconds 1
    }
}
Write-Host "Прокси $hostAlias`: socks5://127.0.0.1:$port" -ForegroundColor Green

$edge = @("${env:ProgramFiles(x86)}\Microsoft\Edge\Application\msedge.exe", "$env:ProgramFiles\Microsoft\Edge\Application\msedge.exe") |
    Where-Object { Test-Path $_ } | Select-Object -First 1
if (-not $edge) { throw 'Microsoft Edge не найден' }

$profileDir = Join-Path $env:LOCALAPPDATA "pvz-browser\$hostAlias"
$arguments = @(
    "--user-data-dir=`"$profileDir`"",     # отдельный профиль: иначе при открытом Edge прокси игнорируется
    "--proxy-server=socks5://127.0.0.1:$port",
    '--disable-extensions',
    '--no-first-run',
    '--new-window'
) + $Url
Start-Process -FilePath $edge -ArgumentList $arguments
