# Reports Docs

Этот каталог собирает в одном месте актуальный контекст по задаче `scheduler_runner/tasks/reports`:

- зачем существует текущая failover-задача;
- какие изменения уже были сделаны;
- что показали реальные логи с объектов;
- какие проблемы уже решены, а какие еще остаются;
- как выглядит план дальнейшей декомпозиции кода по логическим модулям.

## Основные документы

- [REPORTS_TASK_CONTEXT_AND_PROGRESS.md](C:/tools/scheduler/scheduler_runner/tasks/reports/docs/REPORTS_TASK_CONTEXT_AND_PROGRESS.md)
  - основная описательная часть:
    - цель задачи;
    - текущая архитектура;
    - что уже реализовано;
    - промежуточные итоги по логам;
    - актуальные ограничения;
    - что наблюдать дальше.

- [REFACTORING_PLAN_APPENDIX.md](C:/tools/scheduler/scheduler_runner/tasks/reports/docs/REFACTORING_PLAN_APPENDIX.md)
  - приложение с подробным техническим планом декомпозиции `tasks/reports` на логические модули.

## Смежные документы в каталоге `tasks/reports`

- [README.md](C:/tools/scheduler/scheduler_runner/tasks/reports/README.md)
  - краткое описание runtime-слоя `reports`
- [FAILOVER_COORDINATION.md](C:/tools/scheduler/scheduler_runner/tasks/reports/FAILOVER_COORDINATION.md)
  - детальный operational runbook по failover coordination

## Практическое использование

Если новому разработчику нужно быстро войти в контекст, рекомендуемый порядок чтения такой:

1. [REPORTS_TASK_CONTEXT_AND_PROGRESS.md](C:/tools/scheduler/scheduler_runner/tasks/reports/docs/REPORTS_TASK_CONTEXT_AND_PROGRESS.md)
2. [FAILOVER_COORDINATION.md](C:/tools/scheduler/scheduler_runner/tasks/reports/FAILOVER_COORDINATION.md)
3. [REFACTORING_PLAN_APPENDIX.md](C:/tools/scheduler/scheduler_runner/tasks/reports/docs/REFACTORING_PLAN_APPENDIX.md)
