#!/usr/bin/env python3
"""
Дебаг-процессор для локализации проблемы с уникальными ключами в загрузке данных в Google Sheets

Этот процессор предназначен для тестирования логики уникальных ключей в изолированном микросервисе загрузчика.
Он использует фиксированный файл отчета и позволяет отлаживать процесс нормализации дат и поиска уникальных строк.

Author: anikinjura
"""

import sys
import os
from datetime import datetime
import logging
from logging.handlers import RotatingFileHandler
import json

# Добавляем корень проекта в путь Python
project_root = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

# Импортируем наши микросервисы и утилиту для логирования
from scheduler_runner.utils.uploader import upload_data, upload_batch_data, test_connection as test_upload_connection
from scheduler_runner.tasks.reports.config.scripts.kpi_google_sheets_config import KPI_GOOGLE_SHEETS_CONFIG
from scheduler_runner.utils.logging import configure_logger, TRACE_LEVEL


def create_debug_logger():
    """
    Создает и настраивает логгер для дебага уникальных ключей

    Returns:
        logging.Logger: Настроенный объект логгера для дебага
    """
    logger = configure_logger(
        user="reports_domain",
        task_name="DebugUniqueKeys",
        log_levels=[TRACE_LEVEL, logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR],
        single_file_for_levels=False
    )

    return logger


def load_fixed_report_data():
    """
    Загружает фиксированный файл отчета для тестирования
    
    Returns:
        dict: Данные из фиксированного файла отчета
    """
    report_file_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), 
        "reports", 
        "multi_step_ozon", 
        "multi_step_ozon_report_report_20260125.json"
    )
    
    if not os.path.exists(report_file_path):
        print(f"Файл отчета не найден: {report_file_path}")
        # Создадим тестовые данные, если файл не существует
        return {
            'location_info': 'ЧЕБОКСАРЫ_144',
            'extraction_timestamp': '2026-02-04 08:42:07',
            'source_url': 'https://turbo-pvz.ozon.ru',
            'execution_date': '2026-01-25',
            'summary': {
                'giveout': {'value': 260},
                'direct_flow_total': {'total_carriages': 43},
                'return_flow_total': {'total_carriages': 28}
            }
        }
    
    with open(report_file_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def prepare_debug_upload_data(parsing_result=None):
    """
    Подготавливает данные для загрузки в Google Sheets из результата парсинга

    Args:
        parsing_result: Результат работы парсера

    Returns:
        list: Список данных для загрузки в Google Sheets
    """
    upload_data_list = []

    # Если есть результат парсинга, преобразуем его в формат, подходящий для загрузки
    if parsing_result and isinstance(parsing_result, dict):
        # Создаем одну запись на основе всей структуры результата парсинга
        # Извлекаем нужные данные из вложенной структуры
        formatted_record = {}

        # Извлекаем дату и конвертируем в нужный формат
        if 'execution_date' in parsing_result:
            # Используем формат даты из конфигурации поддомена
            # В конфигурации поддомена формат даты определен как "%Y-%m-%d"
            # Но для Google Sheets может потребоваться формат "%d.%m.%Y"
            original_date = parsing_result['execution_date']
            # Проверяем формат входящей даты и конвертируем при необходимости
            try:
                # Если дата в формате YYYY-MM-DD, преобразуем в DD.MM.YYYY
                parsed_date = datetime.strptime(original_date, "%Y-%m-%d")
                formatted_record['Дата'] = parsed_date.strftime("%d.%m.%Y")
            except ValueError:
                # Если формат не YYYY-MM-DD, оставляем как есть
                formatted_record['Дата'] = original_date

        # Извлекаем ПВЗ
        if 'location_info' in parsing_result:
            formatted_record['ПВЗ'] = parsing_result['location_info']

        # Извлекаем данные из summary
        if 'summary' in parsing_result and isinstance(parsing_result['summary'], dict):
            summary = parsing_result['summary']

            # Извлекаем количество выдач
            if 'giveout' in summary and isinstance(summary['giveout'], dict) and 'value' in summary['giveout']:
                formatted_record['Количество выдач'] = summary['giveout']['value']

            # Извлекаем прямой поток
            if 'direct_flow_total' in summary and isinstance(summary['direct_flow_total'], dict):
                if 'total_carriages' in summary['direct_flow_total']:
                    formatted_record['Прямой поток'] = summary['direct_flow_total']['total_carriages']

            # Извлекаем возвратный поток
            if 'return_flow_total' in summary and isinstance(summary['return_flow_total'], dict):
                if 'total_carriages' in summary['return_flow_total']:
                    formatted_record['Возвратный поток'] = summary['return_flow_total']['total_carriages']

        # Добавляем timestamp с текущим временем
        formatted_record['timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Добавляем подготовленную запись в список
        upload_data_list.append(formatted_record)

    # Если не удалось получить данные из результата парсинга, создаем тестовые данные
    if not upload_data_list:
        upload_data_list.append({
            'Дата': '25.01.2026',
            'ПВЗ': 'ЧЕБОКСАРЫ_144',
            'Количество выдач': 260,
            'Прямой поток': 43,
            'Возвратный поток': 28,
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

    return upload_data_list


def prepare_connection_params():
    """
    Подготавливает параметры подключения к Google Sheets для изолированного микросервиса

    Returns:
        dict: Параметры подключения к Google Sheets
    """
    # Импортируем классы из нового изолированного микросервиса
    from scheduler_runner.utils.uploader.core.providers.google_sheets.google_sheets_data_models import TableConfig, ColumnDefinition, ColumnType

    # Получаем оригинальную конфигурацию таблицы
    original_table_config = KPI_GOOGLE_SHEETS_CONFIG["TABLE_CONFIG"]

    # Создаем новые колонки с обновленными параметрами
    new_columns = []
    for col in original_table_config.columns:
        new_columns.append(
            ColumnDefinition(
                name=col.name,
                column_type=col.column_type,
                formula_template=col.formula_template,
                required=col.required,
                unique_key=col.unique_key
            )
        )

    # Создаем новую конфигурацию таблицы с изолированными параметрами
    new_table_config = TableConfig(
        worksheet_name=original_table_config.worksheet_name,
        columns=new_columns,
        id_column=original_table_config.id_column,
        unique_key_columns=original_table_config.unique_key_columns,
        id_formula_template=original_table_config.id_formula_template,
        header_row=original_table_config.header_row
    )

    # Подготовим путь к файлу учетных данных
    from scheduler_runner.tasks.reports.config.reports_paths import REPORTS_PATHS

    connection_params = {
        "CREDENTIALS_PATH": str(REPORTS_PATHS['GOOGLE_SHEETS_CREDENTIALS']),  # Путь к файлу учетных данных из конфига
        "SPREADSHEET_ID": KPI_GOOGLE_SHEETS_CONFIG["SPREADSHEET_ID"],  # ID таблицы из KPI конфига
        "WORKSHEET_NAME": KPI_GOOGLE_SHEETS_CONFIG["WORKSHEET_NAME"],  # Имя листа из KPI конфига
        "TABLE_CONFIG": new_table_config,  # Используем новый объект TableConfig из изолированного микросервиса
        "REQUIRED_CONNECTION_PARAMS": ["CREDENTIALS_PATH", "SPREADSHEET_ID", "WORKSHEET_NAME", "TABLE_CONFIG"]
    }

    return connection_params


def run_debug_upload_microservice(parsing_result=None, logger=None):
    """
    Запускает изолированный микросервис загрузчика с дебаг-логгером

    Args:
        parsing_result: Результат работы парсера
        logger: Объект логгера

    Returns:
        dict: Результат выполнения изолированного микросервиса загрузчика
    """
    if logger is None:
        logger = create_uploader_logger()

    logger.info("Запуск изолированного микросервиса загрузчика данных в Google Sheets с дебаг-логированием...")

    try:
        # Подготовим данные для загрузки
        upload_data_list = prepare_debug_upload_data(parsing_result)

        # Подготовим параметры подключения
        connection_params = prepare_connection_params()

        # Проверим подключение к Google Sheets
        logger.info("Проверка подключения к Google Sheets...")
        connection_result = test_upload_connection(connection_params, logger=logger)
        logger.info(f"Результат проверки подключения: {connection_result}")

        if not connection_result.get("success", False):
            logger.error("Подключение к Google Sheets не удалось")
            return {"success": False, "error": "Не удалось подключиться к Google Sheets"}

        # Загрузим данные в Google Sheets
        logger.info(f"Загрузка данных в Google Sheets: {len(upload_data_list)} записей")
        logger.debug(f"Данные для загрузки: {upload_data_list}")
        logger.debug(f"Параметры подключения: {connection_params}")
        logger.debug(f"Конфигурация таблицы: {connection_params['TABLE_CONFIG']}")
        logger.debug(f"Уникальные ключи: {connection_params['TABLE_CONFIG'].unique_key_columns}")

        upload_result = upload_batch_data(
            data_list=upload_data_list,
            connection_params=connection_params,
            logger=logger
        )

        # Логгируем результат
        logger.info(f"Изолированный микросервис загрузчика завершен успешно. Результат: {upload_result}")

        return upload_result

    except Exception as e:
        # Логгируем ошибку
        logger.error(f"Ошибка при выполнении изолированного микросервиса загрузчика: {e}", exc_info=True)
        raise


def main():
    """
    Основная функция дебаг-процессора для локализации проблемы с уникальными ключами
    """
    print("=== Дебаг-процессор для локализации проблемы с уникальными ключами ===")

    try:
        # Создаем дебаг-логгер
        debug_logger = create_debug_logger()
        debug_logger.info("Запуск дебаг-процессора для локализации проблемы с уникальными ключами")

        # Загружаем фиксированные данные из файла отчета
        print("Загрузка фиксированных данных из файла отчета...")
        fixed_report_data = load_fixed_report_data()
        print(f"Данные из файла отчета: {fixed_report_data}")

        # Запускаем микросервис загрузки данных в Google Sheets с дебаг-логгером
        print("Запуск микросервиса загрузчика данных в Google Sheets с дебаг-логированием...")
        upload_result = run_debug_upload_microservice(fixed_report_data, debug_logger)

        print("Микросервис загрузчика завершен успешно!")
        print(f"Результат загрузки: {upload_result}")

        # Повторный запуск с теми же данными для проверки логики уникальных ключей
        print("\n=== Повторный запуск с теми же данными для проверки логики уникальных ключей ===")
        print("Запуск микросервиса загрузчика данных в Google Sheets с дебаг-логированием (повторно)...")
        upload_result_repeat = run_debug_upload_microservice(fixed_report_data, debug_logger)

        print("Повторный микросервис загрузчика завершен успешно!")
        print(f"Результат повторной загрузки: {upload_result_repeat}")

    except Exception as e:
        print(f"Произошла ошибка: {e}")
        raise

    print("=== Дебаг-процессор завершен ===")


if __name__ == "__main__":
    main()