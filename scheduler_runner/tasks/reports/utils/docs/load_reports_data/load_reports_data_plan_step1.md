Отлично! Вы абсолютно правы. Давайте **упростим для Итерации 1**, чтобы быстрее получить рабочий каркас.

## Упрощенные версии для Итерации 1:

### 1. **Упрощенный `base_config.py`** (MVP версия)

**Файл:** `scheduler_runner/tasks/reports/config/base_config.py`

```python
"""
base_config.py

Базовый класс конфигураций для унификации архитектуры (упрощенная версия для MVP).
В итерации 2 можно добавить JSON Schema, YAML и расширенную валидацию.

Author: anikinjura
Version: 3.0.0 (MVP)
"""

import json
from dataclasses import dataclass, asdict, fields
from typing import Dict, Any, Optional
from enum import Enum


class ConfigError(Exception):
    """Базовое исключение для ошибок конфигурации."""
    pass


@dataclass
class BaseConfig:
    """
    Базовый класс для всех конфигураций в проекте (упрощенная версия).
    
    В итерации 2 будет добавлено:
    - JSON Schema валидация
    - YAML сериализация
    - Расширенная валидация типов
    - Поддержка вложенных конфигураций
    """
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Преобразует конфигурацию в словарь (базовая реализация).
        
        Returns:
            Словарь с данными конфигурации
        """
        result = {}
        for field in fields(self):
            value = getattr(self, field.name)
            
            # Простая обработка Enum
            if isinstance(value, Enum):
                result[field.name] = value.value
            # Пока не обрабатываем вложенные BaseConfig сложным образом
            elif hasattr(value, 'to_dict'):
                result[field.name] = value.to_dict()
            else:
                result[field.name] = value
        
        return result
    
    def to_json(self, indent: int = 2) -> str:
        """
        Сериализует конфигурацию в JSON.
        
        Args:
            indent: отступ для форматирования
            
        Returns:
            JSON-строка с конфигурацией
        """
        return json.dumps(self.to_dict(), indent=indent, ensure_ascii=False, default=str)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'BaseConfig':
        """
        Создает конфигурацию из словаря (базовая реализация).
        
        Args:
            data: словарь с данными конфигурации
            
        Returns:
            Экземпляр конфигурации
        """
        # Фильтруем поля, которые есть в классе
        field_names = {f.name for f in fields(cls)}
        filtered_data = {k: v for k, v in data.items() if k in field_names}
        
        # Создаем экземпляр
        return cls(**filtered_data)
    
    @classmethod
    def from_json(cls, json_str: str) -> 'BaseConfig':
        """
        Создает конфигурацию из JSON строки.
        
        Args:
            json_str: JSON-строка с конфигурацией
            
        Returns:
            Экземпляр конфигурации
        """
        data = json.loads(json_str)
        return cls.from_dict(data)
    
    def __repr__(self) -> str:
        """
        Удобное представление конфигурации.
        """
        class_name = self.__class__.__name__
        fields_str = ", ".join(f"{f.name}={getattr(self, f.name)!r}" for f in fields(self)[:3])
        if len(fields(self)) > 3:
            fields_str += ", ..."
        
        return f"{class_name}({fields_str})"
```

### 2. **Исправленный `file_utils.py`** (с добавлением импорта)

**Файл:** `scheduler_runner/tasks/reports/utils/file_utils.py`

```python
"""
file_utils.py

Общие утилиты для работы с файлами отчетов (упрощенная версия для MVP).

Особенности MVP:
- Поиск файлов отчетов по шаблонам
- Загрузка JSON файлов с обработкой ошибок
- Транслитерация кириллических имен ПВЗ
- Основная функциональность без излишеств

Author: anikinjura
Version: 3.0.0 (MVP)
"""

import json
import logging
import re
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Tuple
from urllib.parse import unquote

from scheduler_runner.utils.system import SystemUtils

logger = logging.getLogger(__name__)


def normalize_pvz_for_filename(pvz_name: str) -> str:
    """
    Нормализует имя ПВЗ для использования в имени файла.
    
    Args:
        pvz_name: оригинальное имя ПВЗ
        
    Returns:
        Нормализованное имя для файла
    """
    if not pvz_name:
        return ""
    
    # Декодируем URL-encoded строки если есть
    try:
        pvz_name = unquote(pvz_name)
    except Exception:
        pass
    
    # Транслитерируем кириллицу
    return SystemUtils.cyrillic_to_translit(pvz_name)


def find_report_file(
    pattern_template: str,
    directory: Path,
    date: str,
    pvz_id: Optional[str] = None,
    use_transliteration: bool = True
) -> Optional[Path]:
    """
    Находит файл отчета по шаблону (базовая реализация).
    
    Args:
        pattern_template: шаблон имени файла с плейсхолдерами {pvz_id}, {date}
        directory: директория для поиска
        date: дата отчета в формате YYYY-MM-DD
        pvz_id: идентификатор ПВЗ (опционально)
        use_transliteration: использовать транслитерацию для pvz_id
        
    Returns:
        Path к найденному файлу или None
    """
    if not directory.exists():
        logger.error(f"Директория не существует: {directory}")
        return None
    
    try:
        # Форматируем дату
        date_obj = datetime.strptime(date, '%Y-%m-%d')
        formatted_date = date_obj.strftime('%Y%m%d')
    except ValueError:
        logger.warning(f"Некорректный формат даты: {date}, используем как есть")
        formatted_date = date.replace('-', '')
    
    # Подготавливаем параметры для шаблона
    template_params = {'date': formatted_date}
    
    if pvz_id:
        if use_transliteration:
            pvz_for_filename = normalize_pvz_for_filename(pvz_id)
        else:
            pvz_for_filename = pvz_id
        template_params['pvz_id'] = pvz_for_filename
    else:
        template_params['pvz_id'] = '*'
    
    # Формируем шаблон для поиска
    try:
        search_pattern = pattern_template.format(**template_params)
    except KeyError as e:
        logger.error(f"Ошибка в шаблоне {pattern_template}: отсутствует плейсхолдер {e}")
        return None
    
    # Ищем файлы
    try:
        matching_files = list(directory.glob(search_pattern))
        
        if not matching_files:
            logger.debug(f"Файлы не найдены по шаблону: {search_pattern}")
            return None
        
        # Возвращаем самый свежий файл
        matching_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
        found_file = matching_files[0]
        
        logger.debug(f"Найден файл: {found_file}")
        return found_file
        
    except Exception as e:
        logger.error(f"Ошибка при поиске файлов по шаблону {search_pattern}: {e}")
        return None


def find_latest_report_file(
    pattern_template: str,
    directory: Path,
    pvz_id: Optional[str] = None,
    max_days_back: int = 7
) -> Optional[Tuple[Path, str]]:
    """
    Находит самый свежий файл отчета (за последние max_days_back дней).
    
    Args:
        pattern_template: шаблон имени файла
        directory: директория для поиска
        pvz_id: идентификатор ПВЗ
        max_days_back: сколько дней назад искать
        
    Returns:
        Кортеж (путь к файлу, дата в формате YYYY-MM-DD) или None
    """
    # Генерируем даты для поиска
    base_date = datetime.now()
    dates_to_try = [
        (base_date - timedelta(days=i)).strftime('%Y-%m-%d')
        for i in range(max_days_back)
    ]
    
    for date in dates_to_try:
        file_path = find_report_file(pattern_template, directory, date, pvz_id)
        if file_path:
            return file_path, date
    
    logger.debug(f"Не найдено файлов за последние {max_days_back} дней")
    return None


def load_json_file(file_path: Path) -> Optional[Dict[str, Any]]:
    """
    Загружает данные из JSON файла с обработкой ошибок.
    
    Args:
        file_path: путь к JSON файлу
        
    Returns:
        Данные из файла или None в случае ошибки
    """
    if not file_path.exists():
        logger.error(f"Файл не существует: {file_path}")
        return None
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        if not isinstance(data, dict):
            logger.error(f"Файл {file_path} не содержит словарь (тип: {type(data).__name__})")
            return None
        
        logger.debug(f"Успешно загружен JSON файл: {file_path} ({len(data)} полей)")
        return data
        
    except json.JSONDecodeError as e:
        logger.error(f"Ошибка декодирования JSON в файле {file_path}: {e}")
    except UnicodeDecodeError as e:
        logger.error(f"Ошибка кодировки в файле {file_path}: {e}")
    except Exception as e:
        logger.error(f"Ошибка при чтении файла {file_path}: {e}")
    
    return None


def list_report_files(
    directory: Path,
    pattern: str = "*.json"
) -> List[Path]:
    """
    Списывает файлы отчетов в директории.
    
    Args:
        directory: директория для поиска
        pattern: шаблон поиска
        
    Returns:
        Список путей к файлам
    """
    if not directory.exists():
        logger.warning(f"Директория не существует: {directory}")
        return []
    
    try:
        return list(directory.glob(pattern))
    except Exception as e:
        logger.error(f"Ошибка при поиске файлов в {directory}: {e}")
        return []
```

---

## Теперь план для **Шага 3: `load_reports_data.py`**

### Основные принципы для MVP:
1. **Простая загрузка** - без сложных стратегий слияния
2. **Базовые отчеты** - только 3 типа: giveout, direct_flow, carriages
3. **Простой маппинг** - только переименование полей
4. **Без метаданных** (или минимальные)

### Каркас `load_reports_data.py` для MVP:

```python
"""
load_reports_data.py

Универсальная утилита для загрузки данных отчетов (MVP версия).
Только основная функциональность, расширение в итерации 2.

Особенности MVP:
- Загрузка 3 типов отчетов: giveout, direct_flow, carriages
- Простое объединение данных (последнее значение побеждает)
- Базовый маппинг полей
- Возвращает универсальный формат без привязки к потребителям

Author: anikinjura
Version: 3.0.0 (MVP)
"""

import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Optional
from enum import Enum
from dataclasses import dataclass

from scheduler_runner.tasks.reports.config.base_config import BaseConfig
from scheduler_runner.tasks.reports.utils.file_utils import (
    find_report_file,
    load_json_file,
    normalize_pvz_for_filename
)
from scheduler_runner.tasks.reports.config.reports_paths import REPORTS_PATHS

logger = logging.getLogger(__name__)


class MergeStrategy(Enum):
    """Стратегии объединения данных (базовые для MVP)."""
    FIRST = "first"  # Использовать первое значение
    LAST = "last"    # Использовать последнее значение (по умолчанию)
    SUM = "sum"      # Суммировать значения


@dataclass
class ReportConfig(BaseConfig):
    """
    Конфигурация для загрузки отчета (MVP версия).
    
    Attributes:
        report_type: тип отчета (giveout, direct_flow, carriages)
        file_pattern: шаблон имени файла с плейсхолдерами
        required: является ли отчет обязательным
        enabled: включен ли этот тип отчета
        fields_mapping: простой словарь переименования полей
    """
    report_type: str
    file_pattern: str
    required: bool = False
    enabled: bool = True
    fields_mapping: Optional[Dict[str, str]] = None
    
    def __post_init__(self):
        """Инициализация после создания."""
        if self.fields_mapping is None:
            self.fields_mapping = {}


def get_default_config() -> List[ReportConfig]:
    """
    Возвращает конфигурацию по умолчанию для отчетов ОЗОН.
    
    Returns:
        Список конфигураций отчетов
    """
    return [
        ReportConfig(
            report_type='giveout',
            file_pattern='ozon_giveout_report_{pvz_id}_{date}.json',
            required=False,
            fields_mapping={
                'issued_packages': 'issued_packages',
                'total_packages': 'total_packages',
                'pvz_info': 'pvz_info',
                'marketplace': 'marketplace'
            }
        ),
        ReportConfig(
            report_type='direct_flow',
            file_pattern='ozon_direct_flow_report_{pvz_id}_{date}.json',
            required=False,
            fields_mapping={
                'total_items_count': 'direct_flow_count',
                'pvz_info': 'pvz_info',
                'marketplace': 'marketplace'
            }
        ),
        ReportConfig(
            report_type='carriages',
            file_pattern='ozon_carriages_report_{date}.json',
            required=False,
            fields_mapping={
                'direct_flow': 'direct_flow_data',
                'return_flow': 'return_flow_data',
                'pvz_info': 'pvz_info',
                'marketplace': 'marketplace'
            }
        )
    ]


def _apply_fields_mapping(data: Dict[str, Any], config: ReportConfig) -> Dict[str, Any]:
    """
    Применяет маппинг полей к данным (простая реализация).
    
    Args:
        data: исходные данные
        config: конфигурация с маппингом
        
    Returns:
        Данные с примененным маппингом
    """
    if not config.fields_mapping or not data:
        return data
    
    result = {}
    for key, value in data.items():
        # Если поле есть в маппинге, переименовываем
        if key in config.fields_mapping:
            new_key = config.fields_mapping[key]
            result[new_key] = value
        else:
            result[key] = value
    
    return result


def load_single_report(
    config: ReportConfig,
    report_date: str,
    pvz_id: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """
    Загружает данные одного отчета.
    
    Args:
        config: конфигурация отчета
        report_date: дата отчета
        pvz_id: идентификатор ПВЗ
        
    Returns:
        Данные отчета или None
    """
    if not config.enabled:
        logger.debug(f"Отчет {config.report_type} отключен в конфигурации")
        return None
    
    report_dir = REPORTS_PATHS["REPORTS_JSON"]
    
    # Находим файл отчета
    file_path = find_report_file(
        pattern_template=config.file_pattern,
        directory=report_dir,
        date=report_date,
        pvz_id=pvz_id,
        use_transliteration=True
    )
    
    if not file_path:
        if config.required:
            logger.error(f"Обязательный отчет {config.report_type} не найден")
        else:
            logger.debug(f"Необязательный отчет {config.report_type} не найден")
        return None
    
    # Загружаем данные из файла
    raw_data = load_json_file(file_path)
    
    if not raw_data:
        logger.warning(f"Не удалось загрузить данные из файла: {file_path}")
        return None
    
    # Применяем маппинг полей
    mapped_data = _apply_fields_mapping(raw_data, config)
    
    # Добавляем базовую метаинформацию
    mapped_data['_report_type'] = config.report_type
    mapped_data['_report_date'] = report_date
    if pvz_id:
        mapped_data['_pvz_id'] = pvz_id
    
    logger.debug(f"Загружен отчет {config.report_type} из {file_path}")
    return mapped_data


def merge_reports_data(reports: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Объединяет данные из нескольких отчетов (простая стратегия LAST).
    
    Args:
        reports: список данных отчетов
        
    Returns:
        Объединенные данные
    """
    merged_data = {}
    
    for report in reports:
        if not report:
            continue
            
        # Простая стратегия: последнее значение побеждает
        for key, value in report.items():
            merged_data[key] = value
    
    return merged_data


def load_reports_data(
    report_date: Optional[str] = None,
    pvz_id: Optional[str] = None,
    config: Optional[List[ReportConfig]] = None
) -> Dict[str, Any]:
    """
    Основная функция для загрузки данных отчетов (MVP версия).
    
    Args:
        report_date: дата отчета в формате YYYY-MM-DD (по умолчанию - сегодня)
        pvz_id: идентификатор ПВЗ
        config: список конфигураций отчетов (по умолчанию используется стандартная)
        
    Returns:
        Объединенные данные отчетов в универсальном формате:
        {
            'issued_packages': 100,
            'total_packages': 150,
            'direct_flow_count': 50,
            'direct_flow_data': {...},
            'pvz_info': 'Москва, ул. Примерная, 1',
            'marketplace': 'ОЗОН',
            '_report_type': 'giveout',  # из последнего загруженного
            '_report_date': '2026-01-05',
            '_pvz_id': 'pvz_moscow_1'
        }
        
    Raises:
        ValueError: если report_date имеет некорректный формат
    """
    # Устанавливаем дату по умолчанию
    if not report_date:
        report_date = datetime.now().strftime('%Y-%m-%d')
    
    # Валидация формата даты
    try:
        datetime.strptime(report_date, '%Y-%m-%d')
    except ValueError:
        raise ValueError(f"Некорректный формат даты: {report_date}. Ожидается YYYY-MM-DD")
    
    # Используем конфигурацию по умолчанию если не указана
    if config is None:
        config = get_default_config()
    
    logger.info(f"Загрузка отчетов за {report_date} для ПВЗ: {pvz_id or 'не указан'}")
    
    # Загружаем каждый отчет согласно конфигурации
    reports_data = []
    for report_config in config:
        try:
            report_data = load_single_report(report_config, report_date, pvz_id)
            if report_data:
                reports_data.append(report_data)
                logger.debug(f"Загружен отчет: {report_config.report_type}")
        except Exception as e:
            logger.error(f"Ошибка при загрузке отчета {report_config.report_type}: {e}")
            if report_config.required:
                raise
    
    # Объединяем данные из всех отчетов
    merged_data = merge_reports_data(reports_data)
    
    # Добавляем базовую метаинформацию
    if merged_data:
        merged_data['_loaded_at'] = datetime.now().isoformat()
        merged_data['_reports_loaded'] = [
            data.get('_report_type') for data in reports_data 
            if data and '_report_type' in data
        ]
        
        logger.info(f"Загружено отчетов: {len(reports_data)}, полей: {len(merged_data)}")
    else:
        logger.warning("Не удалось загрузить данные ни из одного отчета")
    
    return merged_data
```

---

## Шаг 4: Простой трансформер `data_transformers.py` (MVP)

```python
"""
data_transformers.py

Трансформеры данных для преобразования универсального формата
в специфические форматы для потребителей (MVP версия).

Особенности MVP:
- Только один трансформер для Google Sheets
- Простое преобразование без сложной логики
- Базовое форматирование дат

Author: anikinjura
Version: 3.0.0 (MVP)
"""

from datetime import datetime
from typing import Dict, Any, Optional


class DataTransformer:
    """
    Базовый класс для преобразования данных.
    В итерации 2 будут добавлены другие трансформеры.
    """
    
    def transform(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Преобразует данные из универсального формата.
        
        Args:
            raw_data: данные в универсальном формате
            
        Returns:
            Преобразованные данные для специфического потребителя
        """
        raise NotImplementedError


class GoogleSheetsTransformer(DataTransformer):
    """
    Преобразует данные для Google Sheets.
    
    Формат результата:
    {
        'id': '',
        'Дата': '05.01.2026',
        'ПВЗ': 'Название ПВЗ',
        'Количество выдач': 100,
        'Прямой поток': 50,
        'Возвратный поток': 25
    }
    """
    
    def transform(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Преобразует универсальные данные в формат для Google Sheets.
        
        Args:
            raw_data: универсальные данные от load_reports_data
            
        Returns:
            Данные в формате для Google Sheets
        """
        # Извлекаем дату отчета
        report_date = raw_data.get('_report_date', '')
        
        # Преобразуем формат даты из YYYY-MM-DD в DD.MM.YYYY
        formatted_date = self._format_date(report_date)
        
        # Извлекаем данные о выдаче
        issued_packages = raw_data.get('issued_packages') or raw_data.get('total_packages') or 0
        
        # Извлекаем данные о прямом потоке
        direct_flow = self._extract_direct_flow(raw_data)
        
        # Извлекаем данные о возвратном потоке
        return_flow = self._extract_return_flow(raw_data)
        
        # Получаем информацию о ПВЗ
        pvz_info = raw_data.get('pvz_info') or raw_data.get('_pvz_id') or ''
        
        return {
            'id': '',  # Будет заполнен формулой в таблице
            'Дата': formatted_date,
            'ПВЗ': pvz_info,
            'Количество выдач': issued_packages,
            'Прямой поток': direct_flow,
            'Возвратный поток': return_flow
        }
    
    def _format_date(self, date_str: str) -> str:
        """
        Форматирует дату из YYYY-MM-DD в DD.MM.YYYY.
        
        Args:
            date_str: дата в формате YYYY-MM-DD
            
        Returns:
            Дата в формате DD.MM.YYYY или пустая строка
        """
        if not date_str:
            return ''
        
        try:
            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
            return date_obj.strftime('%d.%m.%Y')
        except ValueError:
            return date_str  # Возвращаем как есть если формат не распознан
    
    def _extract_direct_flow(self, raw_data: Dict[str, Any]) -> int:
        """
        Извлекает количество прямого потока.
        
        Args:
            raw_data: универсальные данные
            
        Returns:
            Количество прямого потока
        """
        # Пробуем разные варианты извлечения
        if 'direct_flow_count' in raw_data:
            return raw_data['direct_flow_count'] or 0
        
        if 'direct_flow_data' in raw_data and isinstance(raw_data['direct_flow_data'], dict):
            return raw_data['direct_flow_data'].get('total_items_count', 0)
        
        return 0
    
    def _extract_return_flow(self, raw_data: Dict[str, Any]) -> int:
        """
        Извлекает количество возвратного потока.
        
        Args:
            raw_data: универсальные данные
            
        Returns:
            Количество возвратного потока
        """
        if 'return_flow_data' in raw_data and isinstance(raw_data['return_flow_data'], dict):
            return raw_data['return_flow_data'].get('total_items_count', 0)
        
        return 0
```

---

## Шаг 5: Обновление `__init__.py`

**Файл:** `scheduler_runner/tasks/reports/utils/__init__.py`

```python
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
    load_combined_report_data,
    get_pvz_id,
    parse_common_arguments
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
    'get_pvz_id',
    'parse_common_arguments',
    
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
```

---

## Краткий итог Итерации 1:

### Что будет реализовано:
1. ✅ **`base_config.py`** - упрощенный BaseConfig для унификации
2. ✅ **`file_utils.py`** - общие утилиты для работы с файлами
3. ✅ **`load_reports_data.py`** - основная утилита загрузки (MVP)
4. ✅ **`data_transformers.py`** - трансформер для Google Sheets (MVP)
5. ✅ **`__init__.py`** - обновленный импорт

### Что будет в Итерации 2:
1. Расширенный BaseConfig с JSON Schema
2. Более сложные стратегии слияния
3. Дополнительные трансформеры (API, база данных)
4. Расширенные метаданные
5. YAML поддержка для конфигураций

### Следующие шаги после Итерации 1:
1. Написать простые тесты для проверки работоспособности
2. Протестировать интеграцию с существующими отчетами
3. Начать разработку нового скрипта `GoogleSheets_KPI_UploadScript.py`