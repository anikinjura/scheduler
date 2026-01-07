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