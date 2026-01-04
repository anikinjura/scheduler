"""
reports_utils.py

Утилиты для работы с отчетами ОЗОН для домена (задачи) reports.

Функции:
- load_combined_report_data - загрузка данных из обоих типов отчетов
- get_pvz_id - получение PVZ_ID из конфигурации
- parse_common_arguments - парсинг общих аргументов командной строки

Author: anikinjura
"""
__version__ = '1.0.0'

import json
from pathlib import Path
from datetime import datetime
from typing import Dict, Any

from scheduler_runner.tasks.reports.config.reports_paths import REPORTS_PATHS


def load_combined_report_data(report_date: str, pvz_id: str) -> Dict[str, Any]:
    """
    Загружает данные отчетов из JSON-файлов обоих типов.

    Args:
        report_date: дата отчета в формате YYYY-MM-DD
        pvz_id: идентификатор ПВЗ

    Returns:
        Dict[str, Any]: объединенные данные отчетов
    """
    # Формируем имя файла отчета
    if not report_date:
        report_date = datetime.now().strftime('%Y-%m-%d')

    # Используем транслитерацию для кириллических имен ПВЗ
    from scheduler_runner.utils.system import SystemUtils
    pvz_for_filename = SystemUtils.cyrillic_to_translit(pvz_id)

    # Ищем файлы с отчетами в директории REPORTS_JSON
    report_dir = REPORTS_PATHS["REPORTS_JSON"]

    # Загружаем данные из отчета по выдаче (ozon_giveout_report)
    giveout_report_data = {}
    giveout_report_filename = f"ozon_giveout_report_{pvz_for_filename}_{report_date}.json"
    giveout_report_path = report_dir / giveout_report_filename

    if not giveout_report_path.exists():
        # Если файл с именем ПВЗ не найден, ищем файл без имени ПВЗ в названии
        for file_path in report_dir.glob(f"ozon_giveout_report_*_{report_date.replace('-', '')}*.json"):
            if report_date.replace('-', '') in file_path.name:
                giveout_report_path = file_path
                break
        else:
            # Если не найден файл с датой, ищем самый последний файл с отчетом
            giveout_report_files = list(report_dir.glob("ozon_giveout_report_*.json"))
            if giveout_report_files:
                giveout_report_path = max(giveout_report_files, key=lambda x: x.stat().st_mtime)
            else:
                print(f"Файл отчета по выдаче не найден: {giveout_report_filename}")

    if giveout_report_path.exists():
        with open(giveout_report_path, 'r', encoding='utf-8') as f:
            giveout_report_data = json.load(f)

    # Загружаем данные из отчета по селлерским отправлениям (ozon_direct_flow_report)
    direct_flow_report_data = {}
    direct_flow_report_filename = f"ozon_direct_flow_report_{pvz_for_filename}_{report_date}.json"
    direct_flow_report_path = report_dir / direct_flow_report_filename

    if not direct_flow_report_path.exists():
        # Если файл с именем ПВЗ не найден, ищем файл без имени ПВЗ в названии
        for file_path in report_dir.glob(f"ozon_direct_flow_report_*_{report_date.replace('-', '')}*.json"):
            if report_date.replace('-', '') in file_path.name:
                direct_flow_report_path = file_path
                break
        else:
            # Если не найден файл с датой, ищем самый последний файл с отчетом
            direct_flow_report_files = list(report_dir.glob("ozon_direct_flow_report_*.json"))
            if direct_flow_report_files:
                direct_flow_report_path = max(direct_flow_report_files, key=lambda x: x.stat().st_mtime)
            else:
                print(f"Файл отчета по селлерским отправлениям не найден: {direct_flow_report_filename}")

    if direct_flow_report_path.exists():
        with open(direct_flow_report_path, 'r', encoding='utf-8') as f:
            direct_flow_report_data = json.load(f)

    # Загружаем данные из нового отчета по перевозкам (ozon_carriages_report)
    carriages_report_data = {}
    carriages_report_filename = f"ozon_carriages_report_{report_date.replace('-', '')}.json"
    carriages_report_path = report_dir / carriages_report_filename

    if not carriages_report_path.exists():
        # Если файл с датой не найден, ищем файлы с отчетами перевозок
        for file_path in report_dir.glob(f"ozon_carriages_report_*_{report_date.replace('-', '')}*.json"):
            if report_date.replace('-', '') in file_path.name:
                carriages_report_path = file_path
                break
        else:
            # Если не найден файл с датой, ищем самый последний файл с отчетом о перевозках
            carriages_report_files = list(report_dir.glob("ozon_carriages_report_*.json"))
            if carriages_report_files:
                carriages_report_path = max(carriages_report_files, key=lambda x: x.stat().st_mtime)

    if carriages_report_path.exists():
        with open(carriages_report_path, 'r', encoding='utf-8') as f:
            carriages_report_data = json.load(f)

    # Объединяем данные из всех отчетов
    combined_data = {
        'giveout_report': giveout_report_data,
        'direct_flow_report': direct_flow_report_data,
        'carriages_report': carriages_report_data,  # Добавляем данные из нового отчета
        'date': report_date,
        'pvz_info': giveout_report_data.get('pvz_info') or direct_flow_report_data.get('pvz_info') or carriages_report_data.get('pvz_info', pvz_id),
        'marketplace': giveout_report_data.get('marketplace') or direct_flow_report_data.get('marketplace') or carriages_report_data.get('marketplace', 'ОЗОН')
    }

    return combined_data