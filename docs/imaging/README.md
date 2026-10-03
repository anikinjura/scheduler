# Эталонный образ компьютера ПВЗ (Acronis)

Один компьютер ПВЗ (эталон) настраивается полностью, с него Acronis снимает образ **обоих дисков целиком** (с загрузочной
флешки), образ разворачивается на компьютеры остальных ПВЗ. `sysprep` не используется: компьютеры не в домене и в разных
сетях — одинаковые SID и имя компьютера им не мешают, а права NTFS, пользователи, их SID и пароли в задачах
планировщика переносятся как есть.

Своим на каждом ПВЗ должно быть:

| Что | На эталоне перед образом | После разворачивания |
|---|---|---|
| `PVZ_ID` в `C:\tools\pvz_config.ini` | заглушка `НЕ_НАСТРОЕН` | номер этого ПВЗ |
| Задачи планировщика `\Задачи operator`, `\Задачи camera`, `\Задачи system` | выключены — клон не парсит и не грузит данные под чужим ПВЗ | включены |
| Ключи хоста SSH `C:\ProgramData\ssh\ssh_host_*` | удалены | создаются службой `sshd` при первой загрузке |
| ID AnyDesk (`C:\ProgramData\AnyDesk\service.conf`) | удален | прежний — из `C:\tools\AnyDesk_conf\<PVZ_ID>\` (раздел 5), иначе новый при первом запуске |
| Закрытые ключи администратора (`C:\Users\*\.ssh`) | удалены (ключ хранится только у администратора) | — |
| Вход в Турбо ПВЗ (профиль Edge «Оператора») | остается сессия эталона | выйти и войти под учетной записью этого ПВЗ |

Общим для всех остается: `.env\secrets.env`, `administrators_authorized_keys` и настройки `sshd`, AnyDesk (без ID),
Python и пакеты, права NTFS, пользователи.

## 1. Эталон: перед снятием образа

1. Все настроено и проверено: scheduler, SSH (`docs/remote_access`), AnyDesk установлен, общие папки и права.
2. Ваш закрытый ключ SSH (`pvz_admin`) — у вас, отпечаток проверен (`docs/remote_access/README.md`, раздел 0).
3. Решить про WireGuard (туннели только для этого компьютера — удалить) и BitLocker (скрипт предупредит).
4. PowerShell от имени администратора — сначала просмотр, потом по-настоящему:

   ```powershell
   powershell -ExecutionPolicy Bypass -File C:\tools\scheduler\docs\imaging\prepare_image.ps1 -WhatIf
   powershell -ExecutionPolicy Bypass -File C:\tools\scheduler\docs\imaging\prepare_image.ps1
   ```

   `-ClearSchedulerLogs` — дополнительно очистить `C:\tools\scheduler\logs` (история эталона на новых ПВЗ не нужна).

5. **Сразу** выключить компьютер (`shutdown /s /t 0`) — **не загружать Windows** до снятия образа: при загрузке `sshd`
   создаст новые ключи хоста, AnyDesk — новый ID, и они попадут в образ.

## 2. Снятие образа

Загрузиться с флешки Acronis → резервная копия **дисков целиком** (оба диска, посекторно не обязательно) → сохранить на
внешний диск. Проверить образ средствами Acronis (Validate).

## 3. Эталон после снятия образа

Загрузить Windows и вернуть ему его настройки:

```powershell
powershell -ExecutionPolicy Bypass -File C:\tools\scheduler\docs\imaging\after_deploy.ps1 -PvzId ЧЕБОКСАРЫ_144 -KeepAllAnyDeskConfigs
```

(`-KeepAllAnyDeskConfigs` — не удалять сохраненные конфигурации AnyDesk других ПВЗ, раздел 5.) У эталона будут **новые** ключи хоста SSH и новый ID AnyDesk. На своем компьютере удалить старый отпечаток:
`ssh-keygen -R "[<белый_IP>]:22144"` и при входе принять новый.

## 4. Новый ПВЗ: разворачивание

1. Загрузиться с флешки Acronis → восстановить диски из образа. Если оборудование отличается от эталона (материнская
   плата, контроллер дисков) — **Acronis Universal Restore**, иначе Windows может не загрузиться.
2. Загрузить Windows, войти своей учетной записью администратора (ПИН Windows Hello привязан к TPM эталона и здесь
   не работает — вход паролем учетной записи Microsoft, ПИН задать заново).
3. PowerShell от имени администратора:

   ```powershell
   powershell -ExecutionPolicy Bypass -File C:\tools\scheduler\docs\imaging\after_deploy.ps1 -PvzId ЧЕБОКСАРЫ_143
   ```

   Скрипт запишет `PVZ_ID`, проверит ключи хоста SSH и покажет отпечаток, покажет новый ID AnyDesk, включит задачи
   планировщика, покажет активацию Windows и IP/MAC для роутера. `-SkipTasks` — не включать задачи (например,
   пока на ПВЗ не вошли в Турбо ПВЗ); включить потом повторным запуском без него.
4. Вручную (скрипт напомнит):
   - под «Оператором»: Edge → Турбо ПВЗ → **выйти** и войти под учетной записью этого ПВЗ — до 21:30 первого вечера.
     С сессией эталона нельзя: Ozon допускает одну активную сессию на учетную запись, вход с клона может разлогинить
     эталон;
   - роутер: постоянный IP и проброс `22XXX → IP:22` (`docs/remote_access/README.md`, раздел 3);
   - свой компьютер: блок `Host pvzXXX` в `~/.ssh/config`, при первом входе сверить отпечаток из вывода скрипта;
   - AnyDesk: пароль неконтролируемого доступа (если используется), сообщить сотрудницам новый ID;
   - Windows не активирована — активировать ключом этой машины (цифровая лицензия на том же оборудовании обычно
     активируется сама).
5. Первый вечер: проверить логи (`logs\reports_domain\Processor`, `Uploader`: `KPI_UPLOAD_STATS mode=batch`,
   `API_EXTRACTED`) и строку ПВЗ в листе `KPI`.

## 5. Прежние ID AnyDesk на переустановленных ПВЗ

ID AnyDesk и пароль неконтролируемого доступа хранятся в `C:\ProgramData\AnyDesk\service.conf` (настройки — в
`system.conf`). Если эти файлы, снятые с ПВЗ до переустановки, вернуть на тот же ПВЗ, он сохранит свой ID и пароль —
подключаться к нему можно, как раньше.

1. **С живых ПВЗ, до переустановки** (от администратора — папка доступна только ему):

   ```powershell
   $pvz = 'ЧЕБОКСАРЫ_143'
   New-Item -ItemType Directory -Force "C:\tools\AnyDesk_conf\$pvz" | Out-Null      # на эталоне
   Copy-Item C:\ProgramData\AnyDesk\service.conf, C:\ProgramData\AnyDesk\system.conf "<флешка>\AnyDesk_conf\$pvz\"   # на ПВЗ
   ```

   Собрать на эталоне: `C:\tools\AnyDesk_conf\ЧЕБОКСАРЫ_143\service.conf`, `...\ЧЕБОКСАРЫ_182\service.conf` и т.д.
   Имя папки — точно как `PVZ_ID` (`-PvzId` скрипта).
2. `prepare_image.ps1` закрывает папку правами (только Администраторы и SYSTEM) и показывает, какие ПВЗ попадут в образ.
3. `after_deploy.ps1 -PvzId <ПВЗ>`: при остановленной службе кладет `<ПВЗ>\*.conf` в `C:\ProgramData\AnyDesk`, печатает ID
   (сверить с прежним) и **удаляет папки других ПВЗ** — на ПВЗ остаются только свои файлы. На эталоне, с которого образ
   будет сниматься снова, — `-KeepAllAnyDeskConfigs`.

Правила:
- **Не в репозиторий.** `C:\tools` вне репозитория (`C:\tools\scheduler`), а `service.conf` — это ID и хэш пароля:
  в публичном GitHub их может взять любой. Резервная копия — там же, где закрытый ключ SSH (свое облако/флешка).
- **Каждый файл — только на свой ПВЗ.** Один `service.conf` на двух компьютерах — один ID на двоих, подключения путаются.
- Нового ПВЗ (AnyDesk там не было) в папке нет — он получит новый ID.

## Если скрипт не запустить

Ручные эквиваленты (PowerShell от имени администратора):

```powershell
# перед образом
'\Задачи operator','\Задачи camera','\Задачи system' | ForEach-Object { Disable-ScheduledTask -TaskPath '\' -TaskName $_.TrimStart('\') }
Stop-Service AnyDesk -Force; Remove-Item C:\ProgramData\AnyDesk\service.conf
Remove-Item C:\Users\*\.ssh\* -Force
Stop-Service sshd; Remove-Item C:\ProgramData\ssh\ssh_host_* -Force
# PVZ_ID — в Блокноте, сохранить как UTF-8 (без BOM): с BOM configparser не прочитает файл

# после разворачивания
'\Задачи operator','\Задачи camera','\Задачи system' | ForEach-Object { Enable-ScheduledTask -TaskPath '\' -TaskName $_.TrimStart('\') }
Start-Service sshd; ssh-keygen -lf C:\ProgramData\ssh\ssh_host_ed25519_key.pub
```
