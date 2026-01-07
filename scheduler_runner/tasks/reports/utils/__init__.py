"""
Пакет утилит для работы с отчетами (версия 3.0.0).

Экспортирует функции для работы с отчетами ОЗОН:
- Основные утилиты из reports_utils.py (для совместимости)
- Новые утилиты для универсальной загрузки данных
- Общие утилиты для работы с файлами
- Трансформеры данных для различных потребителей
"""

# Старые функции для обратной совместимости
from .reports_utils import (
    load_combined_report_data
)

# Новые утилиты для работы с файлами
from .file_utils import (
    normalize_pvz_for_filename,
    find_report_file,
    find_latest_report_file,
    load_json_file,
    list_report_files
)

# Универсальная утилита загрузки данных
from .load_reports_data import (
    load_reports_data,
    ReportConfig,
    MergeStrategy,
    get_default_config,
    load_single_report,
    merge_reports_data
)

# Трансформеры данных
from .data_transformers import (
    DataTransformer,
    GoogleSheetsTransformer
)

__all__ = [
    # Старые функции (для совместимости)
    'load_combined_report_data',

    # Новые утилиты для файлов
    'normalize_pvz_for_filename',
    'find_report_file',
    'find_latest_report_file',
    'load_json_file',
    'list_report_files',

    # Универсальная загрузка данных
    'load_reports_data',
    'ReportConfig',
    'MergeStrategy',
    'get_default_config',
    'load_single_report',
    'merge_reports_data',

    # Трансформеры
    'DataTransformer',
    'GoogleSheetsTransformer'
]

__version__ = '3.0.0'