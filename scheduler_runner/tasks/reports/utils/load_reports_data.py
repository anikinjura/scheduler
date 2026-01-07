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

from scheduler_runner.tasks.reports.config.reports_base_config import BaseConfig
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
    merged_data['_loaded_at'] = datetime.now().isoformat()
    merged_data['_reports_loaded'] = [
        data.get('_report_type') for data in reports_data
        if data and '_report_type' in data
    ]

    if merged_data:
        logger.info(f"Загружено отчетов: {len(reports_data)}, полей: {len(merged_data)}")
    else:
        logger.warning("Не удалось загрузить данные ни из одного отчета")

    return merged_data