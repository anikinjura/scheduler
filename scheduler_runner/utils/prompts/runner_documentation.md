# Документация модуля runner.py

## Обзор

Модуль `runner.py` является основной точкой входа для системы планирования и выполнения задач. Он запускается Windows Task Scheduler и обеспечивает контролируемое выполнение задач по расписанию или в принудительном режиме.

## Архитектура

```
runner.py
├── parse_arguments()     # Парсинг CLI аргументов
├── filter_tasks()        # Фильтрация задач по критериям
├── execute_task()        # Выполнение отдельной задачи
└── main()               # Основной цикл выполнения
```

## Функции

### parse_arguments()

**Назначение**: Парсит аргументы командной строки для планировщика задач.

**Параметры**: Нет

**Возвращает**: `argparse.Namespace` - объект с распарсенными аргументами

**Поддерживаемые аргументы**:
- `--user` (обязательный): Имя системного пользователя
- `--task` (опциональный): Имя конкретной задачи для принудительного запуска
- `--detailed` (флаг): Включение детального логирования

**Пример использования**:
```python
args = parse_arguments()
print(f"Пользователь: {args.user}")
print(f"Задача: {args.task}")
print(f"Детальное логирование: {args.detailed}")
```

**Ход выполнения**:
1. Создается парсер с описанием программы
2. Добавляются аргументы с валидацией
3. Парсятся аргументы командной строки
4. Возвращается namespace с параметрами

### filter_tasks()

**Назначение**: Фильтрует список задач по пользователю и опционально по имени задачи.

**Параметры**:
- `all_tasks: List[Dict[str, Any]]` - полный список задач
- `user: str` - имя пользователя для фильтрации
- `task_name: Optional[str]` - опциональное имя задачи

**Возвращает**: `List[Dict[str, Any]]` - отфильтрованный список задач

**Пример использования**:
```python
# Все задачи пользователя operator
operator_tasks = filter_tasks(SCHEDULE, 'operator')

# Конкретная задача backup для пользователя admin
backup_task = filter_tasks(SCHEDULE, 'admin', 'backup')
```

**Ход выполнения**:
1. Фильтрация по пользователю: `task.get('user') == user`
2. Дополнительная фильтрация по имени (если указано)
3. Возврат результирующего списка

**Алгоритм фильтрации**:
```
Входные данные: [task1, task2, task3, ...]
      ↓
Фильтр по пользователю: user == 'operator'
      ↓
Промежуточный результат: [task2, task3]
      ↓
Фильтр по имени: name == 'cleanup' (если указано)
      ↓
Финальный результат: [task3]
```

### execute_task()

**Назначение**: Выполняет отдельную задачу с полным контролем процесса.

**Параметры**:
- `task: Dict[str, Any]` - конфигурация задачи
- `logger` - настроенный логгер
- `force_run: bool = False` - принудительный запуск

**Возвращает**: `bool` - True при успешном выполнении

**Структура задачи**:
```python
task = {
    'name': 'backup_db',           # Имя задачи
    'module': 'backup_script',     # Модуль для запуска
    'args': ['--full', '--compress'], # Аргументы
    'timeout': 300,                # Таймаут в секундах
    'schedule': 'daily',           # Тип расписания
    'time': '02:00',              # Время выполнения
    'env': {'DB_HOST': 'localhost'} # Переменные окружения
}
```

**Ход выполнения**:
1. **Проверка расписания** (если не принудительный запуск)
   ```python
   if not force_run:
       should_run = should_run_now(task, datetime.now())
   ```

2. **Определение модуля для запуска**
   ```python
   script_module = task.get('module') or task.get('script') or task_name
   ```

3. **Подготовка параметров**
   ```python
   args_list = task.get('args', [])
   env_vars = get_task_env(task)
   timeout_seconds = task.get('timeout', 60)
   ```

4. **Запуск подпроцесса**
   ```python
   success = run_subprocess(
       script_name=script_module,
       args=args_list,
       env=env_vars,
       logger=logger,
       timeout=timeout_seconds
   )
   ```

5. **Логирование результата**

**Пример использования**:
```python
task = {
    'name': 'daily_backup',
    'module': 'backup_script',
    'schedule': 'daily',
    'time': '02:00'
}

logger = configure_logger(user='admin', task_name='daily_backup')
success = execute_task(task, logger, force_run=True)
```

### main()

**Назначение**: Основная функция приложения - точка входа планировщика.

**Параметры**: Нет

**Возвращает**: Нет (использует sys.exit)

**Коды завершения**:
- `0`: Все задачи выполнены успешно
- `1`: Одна или несколько задач завершились с ошибкой
- `2`: Выполнение прервано пользователем (Ctrl+C)
- `3`: Критическая ошибка приложения

**Ход выполнения**:

1. **Парсинг аргументов**
   ```python
   args = parse_arguments()
   ```

2. **Фильтрация задач**
   ```python
   tasks_to_run = filter_tasks(SCHEDULE, args.user, args.task)
   ```

3. **Проверка наличия задач**
   ```python
   if not tasks_to_run:
       print("Нет задач для выполнения")
       return
   ```

4. **Выполнение задач**
   ```python
   for task in tasks_to_run:
       logger = configure_logger(user=args.user, task_name=task.get('name'))
       success = execute_task(task, logger, force_run=bool(args.task))
   ```

5. **Подсчет статистики**
   ```python
   print(f"Выполнено: {successful_tasks} успешно, {failed_tasks} с ошибками")
   ```

**Пример сценария выполнения**:
```
Запуск: pythonw runner.py --user operator --detailed

1. Парсинг аргументов:
   - user = 'operator'
   - task = None
   - detailed = True

2. Фильтрация задач:
   - Найдено 3 задачи для пользователя 'operator'

3. Выполнение задач:
   - cleanup_cameras: УСПЕШНО
   - monitor_system: УСПЕШНО  
   - backup_logs: ОШИБКА

4. Итоговая статистика:
   - Выполнено: 2 успешно, 1 с ошибками
   - Код завершения: 1
```

## Примеры использования

### Запуск всех задач пользователя
```bash
pythonw runner.py --user operator
```

### Принудительный запуск конкретной задачи
```bash
pythonw runner.py --user admin --task backup_database
```

### Запуск с детальным логированием
```bash
pythonw runner.py --user operator --detailed
```

## Обработка ошибок

### Ошибки конфигурации
```python
try:
    should_run = should_run_now(task, datetime.now())
except ValueError as e:
    logger.error(f"Ошибка в конфигурации расписания: {e}")
    return False
```

### Неожиданные ошибки выполнения
```python
try:
    success = execute_task(task, logger)
except Exception as e:
    logger.exception(f"Неожиданная ошибка: {e}")
    failed_tasks += 1
```

### Прерывание пользователем
```python
except KeyboardInterrupt:
    print("Выполнение прервано пользователем")
    sys.exit(2)
```

## Логирование

Каждая задача получает индивидуальный логгер:
```python
logger = configure_logger(
    user=args.user,
    task_name=task.get('name'),
    detailed=args.detailed
)
```

Структура логов:
```
logs/
├── operator/
│   ├── cameras_cleanup/
│   │   └── 2024-01-15.log
│   └── system_monitor/
│       └── 2024-01-15.log
└── admin/
    └── database_backup/
        └── 2024-01-15.log
```

## Интеграция с Windows Task Scheduler

### Пример настройки задачи
```xml
<Task>
    <Actions>
        <Exec>
            <Command>pythonw.exe</Command>
            <Arguments>C:\path\to\runner.py --user operator</Arguments>
            <WorkingDirectory>C:\path\to\project</WorkingDirectory>
        </Exec>
    </Actions>
    <Triggers>
        <TimeTrigger>
            <StartBoundary>2024-01-01T00:00:00</StartBoundary>
            <Repetition>
                <Interval>PT1H</Interval>
            </Repetition>
        </TimeTrigger>
    </Triggers>
</Task>
```

## Безопасность

### Изоляция задач
- Каждая задача выполняется в отдельном подпроцессе
- Контроль таймаутов предотвращает зависание
- Индивидуальные переменные окружения

### Контроль доступа
- Фильтрация задач по пользователям
- Логирование всех действий
- Контроль рабочих директорий

## Мониторинг и отладка

### Уровни логирования
- `INFO`: Основные события выполнения
- `DEBUG`: Детальная информация (с флагом --detailed)
- `ERROR`: Ошибки выполнения
- `WARNING`: Предупреждения

### Метрики производительности
- Время выполнения каждой задачи
- Статистика успешных/неуспешных запусков
- Использование ресурсов подпроцессов
