<#
.SYNOPSIS
    Сбор состояния компьютера ПВЗ для HealthMonitorScript: только чтение, результат — JSON в stdout.

.DESCRIPTION
    Диски и SMART, тома, счетчики событий журнала System за последние -Hours часов, задачи планировщика scheduler.
    Ничего не меняет и не пишет. Разделы, на которые не хватает прав, не прерывают сбор: они попадают в поле
    "limited", значения остаются пустыми. SMART и часть журналов требуют повышенных прав
    (docs/HEALTH_MONITOR_PLAN.md, раздел 3.3).

    Пример (вручную):
        powershell -NoProfile -ExecutionPolicy Bypass -File health_probe.ps1 -Hours 24
#>
param(
    [int]$Hours = 24,
    # Задачи планировщика scheduler через запятую (\Путь\Имя)
    [string]$TaskNames = '\Задачи operator,\Задачи camera,\Задачи system'
)

$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
[Console]::OutputEncoding = [Text.Encoding]::UTF8

$limited = New-Object System.Collections.Generic.List[string]
function Try-Section([string]$name, [scriptblock]$block, $fallback) {
    try { & $block } catch { $limited.Add("${name}: $($_.Exception.Message)"); $fallback }
}
function Round1($v) { if ($null -eq $v) { $null } else { [math]::Round([double]$v, 1) } }
function IsoTime($v) { if ($null -eq $v) { $null } else { ([datetime]$v).ToString('yyyy-MM-ddTHH:mm:ss') } }

$now = Get-Date
$principal = [Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()
$elevated = $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)

# --- Система
$system = Try-Section 'system' {
    $os = Get-CimInstance Win32_OperatingSystem
    $cs = Get-CimInstance Win32_ComputerSystem
    @{
        computer_name = $env:COMPUTERNAME
        os_version    = "$($os.Caption) $($os.Version)".Trim()
        boot_time     = IsoTime $os.LastBootUpTime
        uptime_hours  = Round1 (($now - $os.LastBootUpTime).TotalHours)
        cpu           = ((Get-CimInstance Win32_Processor | Select-Object -First 1).Name -replace '\s+', ' ').Trim()
        ram_gb        = Round1 ($cs.TotalPhysicalMemory / 1GB)
    }
} @{ computer_name = $env:COMPUTERNAME }

# --- Тома (размеры без обхода папок)
$volumes = Try-Section 'volumes' {
    @(Get-Volume | Where-Object { $_.DriveLetter -and $_.DriveType -eq 'Fixed' } | Sort-Object DriveLetter | ForEach-Object {
        @{
            letter   = [string]$_.DriveLetter
            label    = $_.FileSystemLabel
            size_gb  = Round1 ($_.Size / 1GB)
            free_gb  = Round1 ($_.SizeRemaining / 1GB)
            free_pct = if ($_.Size) { Round1 (100 * $_.SizeRemaining / $_.Size) } else { $null }
            health   = [string]$_.HealthStatus
        }
    })
} @()

# --- SMART (root\wmi): атрибуты по PNP-идентификатору диска
$smartById = @{}
$predictById = @{}
Try-Section 'smart' {
    foreach ($d in Get-CimInstance -Namespace root\wmi -ClassName MSStorageDriver_FailurePredictData) {
        $key = ($d.InstanceName -replace '_\d+$', '').ToUpperInvariant()
        $v = $d.VendorSpecific
        $attrs = @{}
        for ($i = 2; $i -lt 362; $i += 12) {
            $id = [int]$v[$i]
            if ($id -eq 0) { continue }
            $raw = [BitConverter]::ToUInt32([byte[]]$v[($i + 5)..($i + 8)], 0)
            $attrs["$id"] = @{ value = [int]$v[$i + 3]; worst = [int]$v[$i + 4]; raw = $raw }
        }
        $smartById[$key] = $attrs
    }
    foreach ($s in Get-CimInstance -Namespace root\wmi -ClassName MSStorageDriver_FailurePredictStatus) {
        $predictById[($s.InstanceName -replace '_\d+$', '').ToUpperInvariant()] = [bool]$s.PredictFailure
    }
} $null | Out-Null

# --- Физические диски
$disks = Try-Section 'disks' {
    $drives = @{}
    foreach ($dd in Get-CimInstance Win32_DiskDrive) { $drives[[int]$dd.Index] = $dd }
    @(Get-PhysicalDisk | Sort-Object { [int]$_.DeviceId } | ForEach-Object {
        $pd = $_
        $index = [int]$pd.DeviceId
        $dd = $drives[$index]
        $pnp = if ($dd) { ([string]$dd.PNPDeviceID).ToUpperInvariant() } else { '' }
        $attrs = if ($pnp -and $smartById.ContainsKey($pnp)) { $smartById[$pnp] } else { @{} }
        $counter = $null
        try { $counter = $pd | Get-StorageReliabilityCounter -ErrorAction Stop } catch { }
        $tempC = if ($counter -and $counter.Temperature) { [int]$counter.Temperature } elseif ($attrs['194']) { [int]($attrs['194'].raw -band 0xFF) } else { $null }
        $powerOn = if ($counter -and $counter.PowerOnHours) { [int]$counter.PowerOnHours } elseif ($attrs['9']) { [int]$attrs['9'].raw } else { $null }
        $letters = @(Get-Partition -DiskNumber $index -ErrorAction SilentlyContinue | Where-Object DriveLetter | ForEach-Object { [string]$_.DriveLetter }) -join ','
        $serial = ([string]$pd.SerialNumber).Trim()
        if (-not $serial -and $dd) { $serial = ([string]$dd.SerialNumber).Trim() }
        @{
            disk_number        = $index
            serial             = $serial
            model              = ([string]$pd.FriendlyName).Trim()
            media_type         = [string]$pd.MediaType
            bus_type           = [string]$pd.BusType
            size_gb            = Round1 ($pd.Size / 1GB)
            volumes            = $letters
            health_status      = [string]$pd.HealthStatus
            operational_status = (@($pd.OperationalStatus) -join ',')
            predict_failure    = if ($pnp -and $predictById.ContainsKey($pnp)) { $predictById[$pnp] } else { $null }
            temperature_c      = $tempC
            power_on_hours     = $powerOn
            smart              = $attrs
        }
    })
} @()

# --- События журнала System за окно
$since = $now.AddHours(-$Hours)
function Count-Events([hashtable]$filter, [scriptblock]$where) {
    $filter['LogName'] = 'System'
    $filter['StartTime'] = $since
    $events = @(Get-WinEvent -FilterHashtable $filter -ErrorAction SilentlyContinue)
    if ($where) { $events = @($events | Where-Object $where) }
    $events.Count
}
$events = Try-Section 'events' {
    @{
        window_hours         = $Hours
        unexpected_shutdowns = Count-Events @{ ProviderName = 'Microsoft-Windows-Kernel-Power'; Id = 41 } $null
        bugchecks            = Count-Events @{ Id = 1001 } { $_.ProviderName -match 'WER-SystemErrorReporting|BugCheck' }
        whea_errors          = Count-Events @{ ProviderName = 'Microsoft-Windows-WHEA-Logger'; Level = 1, 2 } $null
        whea_warnings        = Count-Events @{ ProviderName = 'Microsoft-Windows-WHEA-Logger'; Level = 3 } $null
        disk_errors          = Count-Events @{ Level = 1, 2, 3 } { $_.ProviderName -match '^(disk|storahci|iaStorA?|iaStorAC|atapi|stornvme)$' }
        ntfs_errors          = Count-Events @{ Level = 1, 2, 3 } { $_.ProviderName -match '^(Microsoft-Windows-)?Ntfs$' }
        display_resets       = Count-Events @{ Id = 4101 } $null
    }
} @{ window_hours = $Hours }

# --- Задачи планировщика scheduler
$tasks = Try-Section 'tasks' {
    @($TaskNames.Split(',') | Where-Object { $_.Trim() } | ForEach-Object {
        $full = $_.Trim()
        $i = $full.LastIndexOf('\')
        $path = $full.Substring(0, $i + 1)
        $name = $full.Substring($i + 1)
        $t = Get-ScheduledTask -TaskPath $path -TaskName $name -ErrorAction SilentlyContinue
        if (-not $t) { @{ name = $full; found = $false } }
        else {
            $info = $t | Get-ScheduledTaskInfo
            @{ name = $full; found = $true; state = [string]$t.State; last_run = IsoTime $info.LastRunTime; last_result = [int64]$info.LastTaskResult; run_level = [string]$t.Principal.RunLevel }
        }
    })
} @()

@{
    probe_version = 1
    collected_at  = IsoTime $now
    elevated      = $elevated
    limited       = @($limited)
    system        = $system
    volumes       = $volumes
    disks         = $disks
    events        = $events
    tasks         = $tasks
} | ConvertTo-Json -Depth 6 -Compress
