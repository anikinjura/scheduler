<#
.SYNOPSIS
    Подготовка эталонного компьютера ПВЗ к снятию образа (Acronis, целиком оба диска, без sysprep).

.DESCRIPTION
    Убирает с эталона все, что должно быть своим на каждом ПВЗ, и делает клон «тихим» до настройки:
      1. выключает задачи планировщика scheduler (\Задачи operator, \Задачи camera, \Задачи system) — клон не начнет
         парсить и загружать данные, пока не настроен (их включает after_deploy.ps1);
      2. PVZ_ID в C:\tools\pvz_config.ini -> НЕ_НАСТРОЕН (UTF-8 без BOM, иначе configparser не прочитает файл);
      3. очищает папки .ssh во всех профилях (закрытые ключи администратора не должны попасть на ПВЗ);
      4. AnyDesk: останавливает службу и удаляет service.conf (в нем ID) — каждая машина получит свой ID;
      5. (-ClearSchedulerLogs) очищает C:\tools\scheduler\logs;
      6. предупреждает о WireGuard и BitLocker (сам не меняет);
      7. ПОСЛЕДНИМ: останавливает sshd и удаляет ключи хоста C:\ProgramData\ssh\ssh_host_* — каждая машина создаст свои.
    administrators_authorized_keys, sshd_config, права NTFS, пользователи и пароли задач не трогаются.

    Запуск — PowerShell от имени администратора, сначала посмотреть без изменений:

        powershell -ExecutionPolicy Bypass -File C:\tools\scheduler\docs\imaging\prepare_image.ps1 -WhatIf
        powershell -ExecutionPolicy Bypass -File C:\tools\scheduler\docs\imaging\prepare_image.ps1

    После скрипта НЕ загружать Windows до снятия образа: при загрузке служба sshd создаст новые ключи хоста, AnyDesk —
    новый ID, и они попадут в образ. Сразу выключить компьютер и загрузиться с флешки Acronis.
    Порядок целиком — docs/imaging/README.md.
#>
[CmdletBinding(SupportsShouldProcess = $true)]
param(
    [string]$PvzConfigPath = 'C:\tools\pvz_config.ini',
    [string[]]$TaskNames = @('\Задачи operator', '\Задачи camera', '\Задачи system'),
    [switch]$ClearSchedulerLogs,
    [string]$SchedulerLogsPath = 'C:\tools\scheduler\logs',
    # Сохраненные конфигурации AnyDesk по ПВЗ (восстанавливает after_deploy.ps1) — в образ попадают, закрываются правами
    [string]$AnyDeskConfigRoot = 'C:\tools\AnyDesk_conf'
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

function Split-TaskName([string]$fullName) {
    $index = $fullName.LastIndexOf('\')
    @{ Path = $fullName.Substring(0, $index + 1); Name = $fullName.Substring($index + 1) }
}

# --- 1. Задачи планировщика
Step 'Задачи планировщика scheduler: выключить'
foreach ($fullName in $TaskNames) {
    $parts = Split-TaskName $fullName
    $task = Get-ScheduledTask -TaskPath $parts.Path -TaskName $parts.Name -ErrorAction SilentlyContinue
    if (-not $task) { Warn "задача $fullName не найдена"; continue }
    if ($PSCmdlet.ShouldProcess($fullName, 'Stop и Disable')) {
        Stop-ScheduledTask -TaskPath $parts.Path -TaskName $parts.Name -ErrorAction SilentlyContinue
        Disable-ScheduledTask -TaskPath $parts.Path -TaskName $parts.Name | Out-Null
        Done "задача $fullName выключена"
    }
}

# --- 2. PVZ_ID
Step "PVZ_ID -> $Placeholder"
if (-not (Test-Path $PvzConfigPath)) {
    Warn "$PvzConfigPath не найден"
} else {
    $text = [IO.File]::ReadAllText($PvzConfigPath, [Text.Encoding]::UTF8)
    $match = [regex]::Match($text, '(?m)^\s*PVZ_ID\s*=\s*(.*?)\s*$')
    $oldValue = if ($match.Success) { $match.Groups[1].Value } else { '' }
    if ($oldValue -eq $Placeholder) {
        Done "PVZ_ID уже $Placeholder"
    } elseif ($PSCmdlet.ShouldProcess($PvzConfigPath, "PVZ_ID '$oldValue' -> '$Placeholder'")) {
        if ($match.Success) {
            $text = [regex]::Replace($text, '(?m)^(\s*PVZ_ID\s*=\s*).*?(\r?)$', "`${1}$Placeholder`${2}")
        } else {
            $text = $text.TrimEnd() + "`r`nPVZ_ID = $Placeholder`r`n"
        }
        # UTF-8 БЕЗ BOM: с BOM configparser не найдет секцию [DEFAULT]
        [IO.File]::WriteAllText($PvzConfigPath, $text, (New-Object Text.UTF8Encoding($false)))
        Done "PVZ_ID: '$oldValue' -> '$Placeholder' (после образа на этом компьютере: after_deploy.ps1 -PvzId $oldValue)"
    }
}

# --- 3. .ssh в профилях
Step 'Папки .ssh в профилях пользователей: очистить'
$sshDirs = Get-ChildItem -Path 'C:\Users' -Directory -Force -ErrorAction SilentlyContinue |
    ForEach-Object { Join-Path $_.FullName '.ssh' } | Where-Object { Test-Path $_ }
if (-not $sshDirs) { Done 'папок .ssh нет' }
foreach ($dir in $sshDirs) {
    $files = @(Get-ChildItem -Path $dir -File -Force -Recurse)
    if (-not $files) { continue }
    $privateKeys = @($files | Where-Object {
        (Get-Content $_.FullName -TotalCount 1 -ErrorAction SilentlyContinue) -match '^-----BEGIN .*PRIVATE KEY-----'
    })
    if ($privateKeys) { Warn ("закрытые ключи в {0}: {1} — убедитесь, что их копия есть у вас" -f $dir, ($privateKeys.Name -join ', ')) }
    if ($PSCmdlet.ShouldProcess($dir, "удалить файлы: $($files.Name -join ', ')")) {
        $files | Remove-Item -Force
        Done "$dir очищена ($($files.Count) файлов)"
    }
}

# --- 4. AnyDesk
Step 'AnyDesk: удалить ID'
$anyDeskConf = Join-Path $env:ProgramData 'AnyDesk\service.conf'
$anyDeskService = Get-Service -Name 'AnyDesk' -ErrorAction SilentlyContinue
if (-not $anyDeskService -and -not (Test-Path $anyDeskConf)) {
    Done 'AnyDesk не установлен'
} else {
    if ($anyDeskService -and $PSCmdlet.ShouldProcess('служба AnyDesk', 'Stop')) {
        Stop-Service -Name 'AnyDesk' -Force
        Get-Process -Name 'AnyDesk' -ErrorAction SilentlyContinue | Stop-Process -Force
        Done 'служба AnyDesk остановлена'
    }
    if ((Test-Path $anyDeskConf) -and $PSCmdlet.ShouldProcess($anyDeskConf, 'удалить')) {
        Remove-Item $anyDeskConf -Force
        Done "ID AnyDesk удален ($anyDeskConf); на ПВЗ с сохраненной конфигурацией after_deploy.ps1 вернет прежний ID"
    }
}
if (Test-Path $AnyDeskConfigRoot) {
    $saved = @(Get-ChildItem -Path $AnyDeskConfigRoot -Directory | Where-Object {
        Test-Path (Join-Path $_.FullName 'service.conf') })
    if ($PSCmdlet.ShouldProcess($AnyDeskConfigRoot, 'права: только Администраторы и SYSTEM')) {
        icacls $AnyDeskConfigRoot /inheritance:r /grant:r '*S-1-5-18:(OI)(CI)F' /grant:r '*S-1-5-32-544:(OI)(CI)F' /T /Q | Out-Null
        if ($LASTEXITCODE -ne 0) { Warn "icacls: права на $AnyDeskConfigRoot не выставлены" }
    }
    Done ("сохраненные конфигурации AnyDesk в образе ({0}): {1}" -f $AnyDeskConfigRoot,
        $(if ($saved) { $saved.Name -join ', ' } else { 'нет ни одной с service.conf' }))
}

# --- 5. Логи scheduler
if ($ClearSchedulerLogs) {
    Step "Логи scheduler: очистить $SchedulerLogsPath"
    if ((Test-Path $SchedulerLogsPath) -and $PSCmdlet.ShouldProcess($SchedulerLogsPath, 'удалить содержимое')) {
        Get-ChildItem -Path $SchedulerLogsPath -Force | Remove-Item -Recurse -Force
        Done "$SchedulerLogsPath очищена"
    }
}

# --- 6. Только предупреждения
Step 'Проверки (без изменений)'
$wireGuard = @(Get-Service -Name 'WireGuardTunnel$*' -ErrorAction SilentlyContinue)
if ($wireGuard) { Warn ("туннели WireGuard попадут в образ: {0} — удалить, если они только для этого компьютера" -f ($wireGuard.Name -join ', ')) }
try {
    $locked = @(Get-BitLockerVolume -ErrorAction Stop | Where-Object { $_.ProtectionStatus -eq 'On' })
    if ($locked) { Warn ("BitLocker включен на {0}: снимать образ после отключения или с ключом восстановления" -f ($locked.MountPoint -join ', ')) }
} catch { }
Warn 'профиль Edge «Оператора» содержит вход в Турбо ПВЗ этого ПВЗ: на развернутом ПВЗ войти под его учетной записью'

# --- 7. Ключи хоста SSH — последним
Step 'SSH: остановить sshd и удалить ключи хоста'
$sshDir = Join-Path $env:ProgramData 'ssh'
if (-not (Test-Path (Join-Path $sshDir 'administrators_authorized_keys'))) {
    Warn 'administrators_authorized_keys нет — на развернутых ПВЗ не войти по ключу (docs/remote_access)'
}
$sshdService = Get-Service -Name 'sshd' -ErrorAction SilentlyContinue
if ($sshdService -and $PSCmdlet.ShouldProcess('служба sshd', 'Stop')) {
    Stop-Service -Name 'sshd' -Force
    Done 'служба sshd остановлена (автозапуск сохранен: ключи создадутся при первой загрузке)'
}
$hostKeys = @(Get-ChildItem -Path $sshDir -Filter 'ssh_host_*' -File -ErrorAction SilentlyContinue)
if (-not $hostKeys) {
    Done 'ключей хоста нет'
} elseif ($PSCmdlet.ShouldProcess($sshDir, "удалить $($hostKeys.Name -join ', ')")) {
    $hostKeys | Remove-Item -Force
    Done "ключи хоста SSH удалены ($($hostKeys.Count) файлов)"
}

Write-Host ''
Write-Host 'Итог:' -ForegroundColor Cyan
$summary | ForEach-Object { Write-Host "  $_" }
Write-Host ''
if ($WhatIfPreference) {
    Write-Host 'Это был просмотр (-WhatIf): ничего не изменено.' -ForegroundColor Yellow
} else {
    Write-Host 'Дальше: НЕ перезагружать в Windows. Выключить (shutdown /s /t 0) и загрузиться с флешки Acronis.' -ForegroundColor Yellow
    Write-Host 'После снятия образа на этом же компьютере: after_deploy.ps1 -PvzId <его PVZ_ID>.' -ForegroundColor Yellow
}
