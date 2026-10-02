# Удаленный доступ администратора к компьютерам ПВЗ (SSH)

Зачем: администрировать компьютер ПВЗ из дома, **не занимая экран сотрудника** (AnyDesk и RDP Windows 10 Pro
забирают единственный интерактивный сеанс). SSH дает:

- PowerShell администратора на компьютере ПВЗ — службы, общие папки и права, журналы, задачи планировщика, файлы;
- туннель в сеть ПВЗ — веб-интерфейс роутера, камер и регистратора в своем браузере.

Сеанс «Оператора» при этом не трогается: сотрудник ничего не замечает, задачи планировщика работают.

Схема: `ваш компьютер → белый IP ПВЗ:внешний порт → роутер (проброс) → компьютер ПВЗ:22 (sshd) → сеть ПВЗ`.

## 1. Ключ на компьютере администратора (один раз)

На **своем** компьютере (Windows 10/11 — клиент `ssh` встроен):

```powershell
New-Item -ItemType Directory -Force $HOME\.ssh | Out-Null   # без папки ssh-keygen: "No such file or directory"
ssh-keygen -t ed25519 -f $HOME\.ssh\pvz_admin -C "admin@my-pc"
# фраза-пароль — по желанию (защищает ключ, если компьютер украдут)
Get-Content $HOME\.ssh\pvz_admin.pub     # эту строку передаем на ПВЗ
```

- `pvz_admin` — **закрытый** ключ: никуда не копировать, не пересылать, не класть в репозиторий;
- `pvz_admin.pub` — открытый: его можно передавать, он нужен на каждом ПВЗ.

Один ключ на все ПВЗ — нормально. Второй компьютер администратора — свой ключ, добавляется повторным запуском скрипта.

## 2. Компьютер ПВЗ: установка SSH

Нужен `git pull` (UpdaterScript делает его сам) и доступ к Windows Update (скрипт ставит компонент Windows).
PowerShell **от имени администратора** (Win+X → «Windows PowerShell (администратор)»):

```powershell
powershell -ExecutionPolicy Bypass -File C:\tools\scheduler\docs\remote_access\setup_openssh_server.ps1 `
    -PublicKey "ssh-ed25519 AAAA... admin@my-pc" -AllowUser aniki
```

- `-PublicKey` — строка из `pvz_admin.pub` целиком, в кавычках;
- `-AllowUser` — локальное имя вашей учетной записи администратора на этом компьютере (для учетной записи Microsoft —
  имя папки профиля, например `aniki`; список: `Get-LocalUser`). Под «Оператором» войти по SSH будет нельзя;
- `-Port` — порт внутри сети ПВЗ, по умолчанию 22 (менять не нужно: снаружи порт задается на роутере).

Скрипт:
1. ставит «OpenSSH Server», включает автозапуск службы `sshd`;
2. кладет ключ в `C:\ProgramData\ssh\administrators_authorized_keys` (права — только SYSTEM и Администраторы);
3. в `C:\ProgramData\ssh\sshd_config`: вход **только по ключу** (пароль выключен), только `-AllowUser`, туннели
   разрешены, 3 попытки, 60 с на вход;
4. оболочка по умолчанию — PowerShell; правило брандмауэра `scheduler-remote-access-sshd`;
5. проверяет конфигурацию (`sshd -t`), перезапускает службу и печатает адрес компьютера в сети ПВЗ.

Запускать повторно можно: настройки приводятся к нужному виду, новый ключ добавляется к прежним.

Проверка **с этого же компьютера** до настройки роутера:

```powershell
ssh -i <путь к закрытому ключу> aniki@localhost      # или с другого компьютера сети ПВЗ: aniki@192.168.1.97
```

## 3. Роутер ПВЗ

Пункты меню у моделей разные (Keenetic, TP-Link, MikroTik, роутер провайдера) — названия ниже типовые.

### 3.1 Постоянный адрес компьютера

Без этого после перезагрузки роутер может выдать компьютеру другой адрес, и проброс перестанет работать.

- Раздел «Домашняя сеть» / «DHCP» / «Список клиентов» → найти компьютер ПВЗ (по имени или MAC) →
  «Закрепить IP» / «Резервирование адреса».
- Адрес и MAC на компьютере: `Get-NetIPConfiguration`, `Get-NetAdapter | Format-Table Name,MacAddress,Status`.
- Компьютер подключен по Wi-Fi и по кабелю — у адаптеров разные MAC; резервировать для того, через который он работает
  (на 144 сейчас Wi-Fi, 192.168.1.97). Надежнее подключить компьютер кабелем.

### 3.2 Проброс порта

Раздел «Переадресация портов» / «Port forwarding» / «Виртуальные серверы» / «NAT»:

| Параметр | Значение |
|---|---|
| Протокол | TCP |
| Внешний порт | нестандартный, свой на каждом ПВЗ, например `22144` для 144, `22143` для 143, `22182` для 182 |
| Внутренний адрес | закрепленный адрес компьютера (3.1) |
| Внутренний порт | `22` |
| Интерфейс | WAN / подключение к интернету |

Нестандартный внешний порт не защищает сам по себе (защищает вход только по ключу), но убирает почти весь
автоматический перебор паролей по порту 22 — журналы остаются чистыми.

Если у вас самого постоянный белый IP — на роутере ПВЗ (или в правиле брандмауэра на компьютере) можно разрешить
вход только с него: это самая сильная мера. Пример для брандмауэра компьютера ПВЗ:

```powershell
Set-NetFirewallRule -Name scheduler-remote-access-sshd -RemoteAddress <ваш_белый_IP>
```

### 3.3 Что НЕ включать

- **Управление роутером из интернета** (Remote management / «Доступ из WAN» к веб-интерфейсу) — не нужно: веб-интерфейс
  открывается через SSH-туннель (раздел 5), а открытая наружу админка роутера — частая цель взлома.
- Проброс RDP (3389) и портов камер/регистратора наружу — по той же причине; все это доступно через туннель.
- UPnP можно оставить, если им пользуются камеры, но он не нужен для SSH.

### 3.4 Проверка снаружи

С компьютера **вне** сети ПВЗ (например, дома):

```powershell
Test-NetConnection <белый_IP_ПВЗ> -Port 22144      # TcpTestSucceeded : True
ssh -i $HOME\.ssh\pvz_admin -p 22144 aniki@<белый_IP_ПВЗ>
```

Проверять изнутри сети ПВЗ по белому IP бесполезно: многие роутеры не умеют «петлю» (NAT loopback).

## 4. Удобное подключение: `~/.ssh/config`

На компьютере администратора файл `$HOME\.ssh\config` (без расширения):

```text
Host pvz144
    HostName <белый_IP_144>
    Port 22144
    User aniki
    IdentityFile ~/.ssh/pvz_admin
    ServerAliveInterval 30
    # туннели в сеть ПВЗ (раздел 5)
    LocalForward 8144 192.168.1.1:80        # роутер
    LocalForward 9144 192.168.1.64:80       # камера / регистратор (свой адрес)

Host pvz143
    HostName <белый_IP_143>
    Port 22143
    User aniki
    IdentityFile ~/.ssh/pvz_admin
```

Подключение: `ssh pvz144`. Отпечаток ключа сервера при первом входе сверить с тем, что показывает компьютер ПВЗ:
`ssh-keygen -lf C:\ProgramData\ssh\ssh_host_ed25519_key.pub`.

## 5. Веб-интерфейс роутера и камер (туннель)

```powershell
ssh -i $HOME\.ssh\pvz_admin -p 22144 -L 8144:192.168.1.1:80 aniki@<белый_IP_ПВЗ>
```

Пока окно `ssh` открыто — в своем браузере `http://localhost:8144` = веб-интерфейс роутера ПВЗ (адрес роутера в сети
ПВЗ — шлюз из `Get-NetIPConfiguration`). Так же камеры, регистратор, принтер: `-L <свой_порт>:<адрес_в_сети_ПВЗ>:<порт>`.
HTTPS-интерфейс: `-L 8443:192.168.1.1:443` → `https://localhost:8443` (предупреждение о сертификате — ожидаемо).

Туннель без командной строки (только проброс): добавить `-N`.

## 6. Типовая диагностика

```powershell
Get-Service | Where-Object { $_.StartType -eq 'Automatic' -and $_.Status -ne 'Running' }   # упавшие службы
Get-SmbShare                                                   # общие папки
Get-SmbShareAccess -Name <шара>                                # кто имеет доступ к шаре
Grant-SmbShareAccess -Name <шара> -AccountName <учетка> -AccessRight Change -Force
icacls D:\<папка>                                               # права NTFS на папку
Get-SmbSession; Get-SmbOpenFile                                # кто сейчас подключен к шарам
Get-WinEvent -LogName System -MaxEvents 50 | Format-Table TimeCreated,LevelDisplayName,Message -Wrap
Get-ScheduledTask -TaskPath \ | Get-ScheduledTaskInfo | Format-Table TaskName,LastRunTime,LastTaskResult
Test-NetConnection 192.168.1.64 -Port 554                      # видна ли камера (RTSP)
Get-Volume                                                     # место на дисках
quser                                                          # кто вошел в Windows (сеанс «Оператора» не трогается)
```

Логи scheduler: `C:\tools\scheduler\logs\...`; файлы — `scp`:
`scp -P 22144 aniki@<IP>:C:/tools/scheduler/logs/reports_domain/Processor/2026-10-01.log .`

## 7. Если не входит

| Симптом | Причина / что проверить (на компьютере ПВЗ) |
|---|---|
| `Connection timed out` | нет проброса или неверный адрес на роутере (3.1–3.2); компьютер выключен; брандмауэр: `Get-NetFirewallRule -Name scheduler-remote-access-sshd` |
| `Connection refused` | служба не запущена: `Get-Service sshd`; порт: `Get-NetTCPConnection -LocalPort 22 -State Listen` |
| `Permission denied (publickey)` | ключ не тот / не добавлен: `Get-Content C:\ProgramData\ssh\administrators_authorized_keys`; права: `icacls C:\ProgramData\ssh\administrators_authorized_keys` (только SYSTEM и Администраторы); пользователь не в `AllowUsers` или не администратор |
| вход есть, но не под тем пользователем | `-AllowUser` должен совпадать с `whoami` (часть после `\`) |

Журнал sshd: `Get-WinEvent -LogName OpenSSH/Operational -MaxEvents 30 | Format-List TimeCreated,Message`.

## 8. Отключение

```powershell
Stop-Service sshd; Set-Service sshd -StartupType Disabled
Remove-NetFirewallRule -Name scheduler-remote-access-sshd
```

и удалить проброс порта на роутере. Отозвать ключ (компьютер администратора утерян) — удалить его строку из
`C:\ProgramData\ssh\administrators_authorized_keys` на каждом ПВЗ.

## 9. Альтернатива без проброса портов: WireGuard

На 144 уже есть туннель WireGuard (`anikin-jura2`, 10.8.1.x). Если поднять его на всех ПВЗ, SSH можно слушать только
внутри туннеля (`Set-NetFirewallRule -Name scheduler-remote-access-sshd -RemoteAddress 10.8.1.0/24`) и не открывать
наружу ничего — это безопаснее белого IP с пробросом.

## 10. Почему не «патч» RDP для нескольких сеансов

Одновременные интерактивные сеансы в Windows 10 Pro ограничены лицензией; патчи `termsrv.dll` (RDP Wrapper и т.п.)
обходят это ограничение, ломаются после обновлений Windows и подменяют системный файл. SSH-доступ решает задачи
администрирования штатными средствами и сеанс сотрудника не занимает.
