"""
Tests for refactored modules.
"""
import os
import tempfile

# Логгеры reports в тестах пишут во временный каталог: иначе строки тестов («Owner state sync ...», «FAILOVER_...»)
# попадают в рабочие logs/reports_domain и выглядят при разборе инцидентов как настоящие запуски
os.environ.setdefault("SCHEDULER_LOGS_DIR", os.path.join(tempfile.gettempdir(), "scheduler_test_logs"))
