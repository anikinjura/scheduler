<#
.SYNOPSIS
    OpenSSH Server на компьютере ПВЗ: вход администратора по ключу, без пароля, без захвата сеанса оператора.

.DESCRIPTION
    Запускать в PowerShell от имени администратора (Win+X -> «Windows PowerShell (администратор)»):

        powershell -ExecutionPolicy Bypass -File C:\tools\scheduler\docs\remote_access\setup_openssh_server.ps1 `
            -PublicKey "ssh-ed25519 AAAA... admin@workstation" -AllowUser aniki

    Скрипт можно запускать повторно: он приводит настройки к нужному виду и добавляет ключ, если его еще нет.
    Что делает:
      1. ставит компонент Windows «OpenSSH Server» (нужен доступ к Windows Update);
      2. включает автозапуск службы sshd;
      3. записывает открытый ключ в C:\ProgramData\ssh\administrators_authorized_keys с правами только для
         SYSTEM и группы администраторов (иначе sshd ключ игнорирует);
      4. в sshd_config: вход только по ключу, только пользователь -AllowUser, порт -Port, туннели разрешены;
      5. оболочка по умолчанию — PowerShell;
      6. правило брандмауэра на порт;
      7. проверяет конфигурацию (sshd -t) и перезапускает службу.
    Подробности и настройка роутера — docs/remote_access/README.md.
#>
param(
    [Parameter(Mandatory = $true)][string]$PublicKey,
    [Parameter(Mandatory = $true)][string]$AllowUser,
    [int]$Port = 22
)

$ErrorActionPreference = 'Stop'

function Step($text) { Write-Host "==> $text" -ForegroundColor Cyan }

$principal = [Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()
if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    throw 'Нужен PowerShell от имени администратора'
}
if ($PublicKey -notmatch '^(ssh-ed25519|ssh-rsa|ecdsa-sha2-\S+) \S+') {
    throw 'PublicKey должен быть строкой открытого ключа (содержимое файла .pub), а не путем к файлу и не закрытым ключом'
}
if (-not (Get-LocalUser -Name $AllowUser -ErrorAction SilentlyContinue)) {
    throw "Локальный пользователь '$AllowUser' не найден (для учетной записи Microsoft — локальное имя профиля, например aniki)"
}

Step 'Компонент OpenSSH Server'
$capability = Get-WindowsCapability -Online -Name 'OpenSSH.Server*' | Select-Object -First 1
if ($capability.State -ne 'Installed') {
    Add-WindowsCapability -Online -Name $capability.Name | Out-Null
}

Step 'Служба sshd: автозапуск'
Set-Service -Name sshd -StartupType Automatic
# Первый запуск создает C:\ProgramData\ssh\sshd_config и ключи хоста
Start-Service sshd

Step 'Ключ администратора'
$keysFile = Join-Path $env:ProgramData 'ssh\administrators_authorized_keys'
# @(...): файл из одной строки Get-Content возвращает строкой, и += склеил бы ключи
$existing = @()
if (Test-Path $keysFile) { $existing = @(Get-Content $keysFile | Where-Object { $_.Trim() }) }
if ($existing -notcontains $PublicKey.Trim()) {
    $existing += $PublicKey.Trim()
}
# ASCII без BOM: sshd не читает файл с BOM
[IO.File]::WriteAllLines($keysFile, [string[]]$existing, (New-Object Text.ASCIIEncoding))
# Только SYSTEM и Администраторы (SID — имя группы локализовано). Иначе sshd молча игнорирует файл
icacls $keysFile /inheritance:r /grant:r '*S-1-5-18:F' /grant:r '*S-1-5-32-544:F' | Out-Null
if ($LASTEXITCODE -ne 0) { throw "icacls: не удалось выставить права на $keysFile" }

Step 'sshd_config'
$configFile = Join-Path $env:ProgramData 'ssh\sshd_config'
$config = Get-Content $configFile
$settings = [ordered]@{
    'Port'                   = "$Port"
    'PubkeyAuthentication'   = 'yes'
    'PasswordAuthentication' = 'no'
    'PermitEmptyPasswords'   = 'no'
    'AllowUsers'             = $AllowUser
    'AllowTcpForwarding'     = 'yes'   # туннель к роутеру и камерам: ssh -L
    'MaxAuthTries'           = '3'
    'LoginGraceTime'         = '60'    # 30 с не хватало на сверку отпечатка сервера и ввод фразы-пароля ключа
}
# Глобальные параметры должны стоять до первого блока Match
$matchIndex = ($config | Select-String -Pattern '^\s*Match\s' | Select-Object -First 1).LineNumber
$head = if ($matchIndex -gt 1) { @($config[0..($matchIndex - 2)]) } elseif ($matchIndex) { @() } else { @($config) }
$tail = if ($matchIndex) { @($config[($matchIndex - 1)..($config.Count - 1)]) } else { @() }
# Ключи администраторов sshd берет из administrators_authorized_keys только по этому блоку (есть в стандартном файле)
if (-not ($tail | Select-String -SimpleMatch 'administrators_authorized_keys')) {
    $tail += 'Match Group administrators'
    $tail += '       AuthorizedKeysFile __PROGRAMDATA__/ssh/administrators_authorized_keys'
}
foreach ($key in $settings.Keys) {
    $head = $head | Where-Object { $_ -notmatch "^\s*#?\s*$key\s" }
}
$head += '# --- scheduler remote_access (setup_openssh_server.ps1) ---'
foreach ($key in $settings.Keys) { $head += "$key $($settings[$key])" }
$head += ''
[IO.File]::WriteAllLines($configFile, [string[]]($head + $tail), (New-Object Text.ASCIIEncoding))

Step 'Оболочка по умолчанию: PowerShell'
New-Item -Path 'HKLM:\SOFTWARE\OpenSSH' -Force | Out-Null
New-ItemProperty -Path 'HKLM:\SOFTWARE\OpenSSH' -Name DefaultShell `
    -Value "$env:SystemRoot\System32\WindowsPowerShell\v1.0\powershell.exe" -PropertyType String -Force | Out-Null

Step "Брандмауэр: TCP $Port"
$ruleName = 'scheduler-remote-access-sshd'
Get-NetFirewallRule -Name $ruleName -ErrorAction SilentlyContinue | Remove-NetFirewallRule
New-NetFirewallRule -Name $ruleName -DisplayName "OpenSSH Server (TCP $Port)" -Direction Inbound -Protocol TCP `
    -LocalPort $Port -Action Allow -Profile Any | Out-Null

Step 'Проверка конфигурации и перезапуск'
& "$env:SystemRoot\System32\OpenSSH\sshd.exe" -t
if ($LASTEXITCODE -ne 0) { throw 'sshd -t: ошибка в sshd_config, служба не перезапущена' }
Restart-Service sshd
Start-Sleep -Seconds 2
$test = Test-NetConnection -ComputerName localhost -Port $Port -WarningAction SilentlyContinue

$lanIp = (Get-NetIPConfiguration | Where-Object { $_.IPv4DefaultGateway -and $_.NetAdapter.Status -eq 'Up' } |
    ForEach-Object { $_.IPv4Address.IPAddress }) -join ', '
Write-Host ''
Write-Host "Служба sshd: $((Get-Service sshd).Status), порт $Port открыт локально: $($test.TcpTestSucceeded)" -ForegroundColor Green
Write-Host "Адрес компьютера в сети ПВЗ (для проброса на роутере и резервирования DHCP): $lanIp"
Write-Host "Подключение из сети ПВЗ: ssh -p $Port $AllowUser@<адрес>"
