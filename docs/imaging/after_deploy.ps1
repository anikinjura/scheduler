<#
.SYNOPSIS
    Настройка компьютера ПВЗ после разворачивания эталонного образа (и самого эталона после снятия образа).

.DESCRIPTION
    Обратное к prepare_image.ps1:
      1. записывает PVZ_ID в C:\tools\pvz_config.ini (UTF-8 без BOM);
      2. SSH: запускает sshd, при необходимости создает ключи хоста, печатает отпечаток — сверить при первом входе;
      3. AnyDesk: если есть C:\tools\AnyDesk_conf\<PvzId>\*.conf (снятые с этого ПВЗ раньше) — при остановленной
         службе кладет их в C:\ProgramData\AnyDesk: прежние ID и пароль неконтролируемого доступа; иначе новый ID.
         Печатает ID. Папку закрывает правами (только Администраторы и SYSTEM) и удаляет из нее папки других ПВЗ
         (-KeepAllAnyDeskConfigs — оставить все, для эталона);
      4. включает задачи планировщика scheduler (\Задачи operator, \Задачи camera, \Задачи system);
      5. показывает активацию Windows и сетевые адреса (для резервирования IP на роутере);
      6. напоминает, что сделать вручную.

    Запуск — PowerShell от имени администратора:

        powershell -ExecutionPolicy Bypass -File C:\tools\scheduler\docs\imaging\after_deploy.ps1 -PvzId ЧЕБОКСАРЫ_143

    Повторный запуск безопасен. Порядок целиком — docs/imaging/README.md.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string]$PvzId,
    [string]$PvzConfigPath = 'C:\tools\pvz_config.ini',
    [string[]]$TaskNames = @('\Задачи operator', '\Задачи camera', '\Задачи system'),
    # Не включать задачи (например, если ПВЗ еще не вошел в Турбо ПВЗ) — включить потом повторным запуском
    [switch]$SkipTasks,
    # Сохраненные конфигурации AnyDesk по ПВЗ: <корень>\<PvzId>\service.conf (ID и пароль), system.conf (настройки)
    [string]$AnyDeskConfigRoot = 'C:\tools\AnyDesk_conf',
    # Не удалять папки других ПВЗ (на эталоне, с которого снова будет сниматься образ)
    [switch]$KeepAllAnyDeskConfigs
)

$ErrorActionPreference = 'Stop'
$Placeholder = 'НЕ_НАСТРОЕН'
$summary = New-Object System.Collections.Generic.List[string]

function Step($text) { Write-Host "==> $text" -ForegroundColor Cyan }
function Done($text) { $summary.Add("[готово] $text"); Write-Host "    $text" -ForegroundColor Green }
function Warn($text) { $summary.Add("[ВНИМАНИЕ] $text"); Write-Host "    ВНИМАНИЕ: $text" -ForegroundColor Yellow }

$principal = [Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()
if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    throw 'Нужен PowerShell от имени администратора'
}
$PvzId = $PvzId.Trim()
if ($PvzId -eq $Placeholder -or $PvzId -notmatch '^[\p{Lu}0-9_]+_\d+$') {
    throw "PvzId '$PvzId' не похож на идентификатор ПВЗ (пример: ЧЕБОКСАРЫ_143, СОСНОВКА_10)"
}

# --- 1. PVZ_ID
Step "PVZ_ID -> $PvzId"
if (-not (Test-Path $PvzConfigPath)) { throw "$PvzConfigPath не найден" }
$text = [IO.File]::ReadAllText($PvzConfigPath, [Text.Encoding]::UTF8)
$match = [regex]::Match($text, '(?m)^\s*PVZ_ID\s*=\s*(.*?)\s*$')
$oldValue = if ($match.Success) { $match.Groups[1].Value } else { '' }
if ($match.Success) {
    $text = [regex]::Replace($text, '(?m)^(\s*PVZ_ID\s*=\s*).*?(\r?)$', "`${1}$PvzId`${2}")
} else {
    $text = $text.TrimEnd() + "`r`nPVZ_ID = $PvzId`r`n"
}
# UTF-8 БЕЗ BOM: с BOM configparser не найдет секцию [DEFAULT]
[IO.File]::WriteAllText($PvzConfigPath, $text, (New-Object Text.UTF8Encoding($false)))
Done "PVZ_ID: '$oldValue' -> '$PvzId'"

# --- 2. SSH
Step 'SSH: служба и ключи хоста'
$sshDir = Join-Path $env:ProgramData 'ssh'
$sshdService = Get-Service -Name 'sshd' -ErrorAction SilentlyContinue
if (-not $sshdService) {
    Warn 'OpenSSH Server не установлен — docs/remote_access/setup_openssh_server.ps1'
} else {
    if ($sshdService.StartType -ne 'Automatic') { Set-Service -Name 'sshd' -StartupType Automatic }
    Start-Service -Name 'sshd'
    $hostKey = Join-Path $sshDir 'ssh_host_ed25519_key.pub'
    if (-not (Test-Path $hostKey)) {
        # Служба обычно создает ключи сама при запуске; если нет — создать и перезапустить
        & "$env:SystemRoot\System32\OpenSSH\ssh-keygen.exe" -A | Out-Null
        Restart-Service -Name 'sshd'
    }
    if (Test-Path $hostKey) {
        $fingerprint = (& "$env:SystemRoot\System32\OpenSSH\ssh-keygen.exe" -lf $hostKey) -join ' '
        Done "sshd: $((Get-Service sshd).Status); отпечаток (сверить при первом входе): $fingerprint"
    } else {
        Warn "ключ хоста $hostKey не создан — проверить журнал OpenSSH/Operational"
    }
    if (-not (Test-Path (Join-Path $sshDir 'administrators_authorized_keys'))) {
        Warn 'administrators_authorized_keys нет — вход по ключу не сработает (setup_openssh_server.ps1)'
    }
}

# --- 3. AnyDesk
Step 'AnyDesk: ID этого ПВЗ'
if (Test-Path $AnyDeskConfigRoot) {
    # Файлы всех ПВЗ (ID и хэши паролей) — только Администраторы и SYSTEM, сотрудники под «Оператором» не читают
    icacls $AnyDeskConfigRoot /inheritance:r /grant:r '*S-1-5-18:(OI)(CI)F' /grant:r '*S-1-5-32-544:(OI)(CI)F' /T /Q | Out-Null
    if ($LASTEXITCODE -ne 0) { Warn "icacls: права на $AnyDeskConfigRoot не выставлены" }
}
$anyDeskService = Get-Service -Name 'AnyDesk' -ErrorAction SilentlyContinue
$savedConfigDir = Join-Path $AnyDeskConfigRoot $PvzId
$savedConfigs = @(Get-ChildItem -Path $savedConfigDir -Filter '*.conf' -File -ErrorAction SilentlyContinue)
if (-not $anyDeskService) {
    Done 'AnyDesk не установлен'
} else {
    if ($savedConfigs) {
        # Прежние ID и пароль неконтролируемого доступа этого ПВЗ: подменить файлы при остановленной службе
        Stop-Service -Name 'AnyDesk' -Force
        Get-Process -Name 'AnyDesk' -ErrorAction SilentlyContinue | Stop-Process -Force
        $anyDeskData = Join-Path $env:ProgramData 'AnyDesk'
        New-Item -ItemType Directory -Path $anyDeskData -Force | Out-Null
        $savedConfigs | Copy-Item -Destination $anyDeskData -Force
        Done ("конфигурация AnyDesk восстановлена из {0}: {1}" -f $savedConfigDir, ($savedConfigs.Name -join ', '))
    } else {
        Done "сохраненной конфигурации нет ($savedConfigDir) — AnyDesk создаст новый ID"
    }
    Start-Service -Name 'AnyDesk'
    $anyDeskExe = @("${env:ProgramFiles(x86)}\AnyDesk\AnyDesk.exe", "$env:ProgramFiles\AnyDesk\AnyDesk.exe") |
        Where-Object { Test-Path $_ } | Select-Object -First 1
    $anyDeskId = ''
    if ($anyDeskExe) {
        foreach ($attempt in 1..10) {
            $anyDeskId = ((& $anyDeskExe --get-id) | Out-String).Trim()
            if ($anyDeskId -match '^\d+$') { break }
            Start-Sleep -Seconds 3
        }
    }
    if ($anyDeskId -match '^\d+$') {
        Done ("ID AnyDesk этого компьютера: {0}{1}" -f $anyDeskId, $(if ($savedConfigs) { ' (должен совпасть с прежним ID этого ПВЗ)' } else { '' }))
    } else {
        Warn 'ID AnyDesk не получен — открыть AnyDesk и посмотреть ID в окне (нужен интернет)'
    }
}
if ((Test-Path $AnyDeskConfigRoot) -and -not $KeepAllAnyDeskConfigs) {
    # На ПВЗ нужны только свои файлы; чужие (ID и хэши паролей других ПВЗ) не держать
    $others = @(Get-ChildItem -Path $AnyDeskConfigRoot -Directory | Where-Object { $_.Name -ne $PvzId })
    if ($others) {
        $others | Remove-Item -Recurse -Force
        Done ("из {0} удалены папки других ПВЗ: {1}" -f $AnyDeskConfigRoot, ($others.Name -join ', '))
    }
}

# --- 4. Задачи планировщика
if ($SkipTasks) {
    Warn 'задачи планировщика не включены (-SkipTasks): включить повторным запуском без -SkipTasks'
} else {
    Step 'Задачи планировщика scheduler: включить'
    foreach ($fullName in $TaskNames) {
        $index = $fullName.LastIndexOf('\')
        $taskPath = $fullName.Substring(0, $index + 1)
        $taskName = $fullName.Substring($index + 1)
        $task = Get-ScheduledTask -TaskPath $taskPath -TaskName $taskName -ErrorAction SilentlyContinue
        if (-not $task) { Warn "задача $fullName не найдена"; continue }
        Enable-ScheduledTask -TaskPath $taskPath -TaskName $taskName | Out-Null
        Done "задача $fullName включена"
    }
}

# --- 5. Проверки
Step 'Проверки'
$license = Get-CimInstance SoftwareLicensingProduct -Filter "PartialProductKey IS NOT NULL AND Name LIKE 'Windows%'" -ErrorAction SilentlyContinue |
    Select-Object -First 1
if ($license -and $license.LicenseStatus -eq 1) {
    Done 'Windows активирована'
} else {
    Warn 'Windows не активирована: Параметры -> Обновление и безопасность -> Активация (ключ этой машины)'
}
Get-NetIPConfiguration | Where-Object { $_.IPv4DefaultGateway -and $_.NetAdapter.Status -eq 'Up' } | ForEach-Object {
    $mac = (Get-NetAdapter -InterfaceIndex $_.InterfaceIndex).MacAddress
    Done ("сеть {0}: IP {1}, шлюз {2}, MAC {3} (для резервирования IP на роутере)" -f
        $_.InterfaceAlias, ($_.IPv4Address.IPAddress -join ', '), ($_.IPv4DefaultGateway.NextHop -join ', '), $mac)
}

Write-Host ''
Write-Host 'Итог:' -ForegroundColor Cyan
$summary | ForEach-Object { Write-Host "  $_" }
Write-Host ''
Write-Host 'Вручную:' -ForegroundColor Yellow
Write-Host "  1. Под «Оператором»: Edge -> Турбо ПВЗ: выйти и войти под учетной записью $PvzId (до 21:30)."
Write-Host '  2. Роутер: постоянный IP компьютера и проброс 22XXX -> IP:22 (docs/remote_access/README.md, раздел 3).'
Write-Host '  3. Свой компьютер: блок Host в ~/.ssh/config; при первом входе сверить отпечаток выше.'
Write-Host '  4. AnyDesk: пароль неконтролируемого доступа, если нужен; сообщить сотрудницам новый ID.'
