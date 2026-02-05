#!/usr/bin/env python3
"""
reports_processor.py

Процессор поддомена reports, реализующий полный цикл:
1. Парсинг данных из системы Ozon
2. Загрузка данных в Google Sheets
3. Отправка уведомлений через Telegram

Архитектура:
- Использует изолированные микросервисы для каждой операции
- Использует централизованную систему логирования

Author: anikinjura
"""
__version__ = '0.0.1'


import sys
import os
from datetime import datetime
import logging
import argparse

# Добавляем корень проекта в путь Python
project_root = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

# Импортируем микросервисы и утилиту для логирования
from scheduler_runner.tasks.reports.parser.implementations.multi_step_ozon_parser import MultiStepOzonParser
from scheduler_runner.tasks.reports.parser.configs.implementations.multi_step_ozon_config import MULTI_STEP_OZON_CONFIG
from scheduler_runner.utils.uploader import upload_data, upload_batch_data, test_connection as test_upload_connection
from scheduler_runner.tasks.reports.config.scripts.kpi_google_sheets_config import KPI_GOOGLE_SHEETS_CONFIG
from scheduler_runner.utils.notifications import send_notification, test_connection as test_notification_connection
from scheduler_runner.utils.logging import configure_logger, TRACE_LEVEL


def create_parser_logger():
    """
    Создает и настраивает логгер для микросервиса парсера

    Returns:
        logging.Logger: Настроенный объект логгера для парсера
    """
    logger = configure_logger(
        user="reports_domain",
        task_name="Parser",
        log_levels=[TRACE_LEVEL, logging.DEBUG],
        single_file_for_levels=False
    )

    return logger


def create_uploader_logger():
    """
    Создает и настраивает логгер для микросервиса загрузчика

    Returns:
        logging.Logger: Настроенный объект логгера для загрузчика
    """
    logger = configure_logger(
        user="reports_domain",
        task_name="Uploader",
        log_levels=[TRACE_LEVEL, logging.DEBUG],
        single_file_for_levels=False
    )

    return logger


def run_parsing_microservice(execution_date=None):
    """
    Запускает микросервис парсера с его собственным логгером

    Args:
        execution_date: Дата выполнения в формате 'YYYY-MM-DD' (если не указана, используется текущая дата)

    Returns:
        dict: Результат выполнения микросервиса парсера
    """
    # Создаем логгер для парсера
    logger = create_parser_logger()

    # Логгируем начало процесса
    logger.info("Запуск микросервиса парсера отчетов Ozon")

    try:
        # Подготовим конфигурацию
        config = MULTI_STEP_OZON_CONFIG.copy()

        # Установим дату для отчета
        if execution_date is None:
            execution_date = datetime.now().strftime("%Y-%m-%d")
        
        config['execution_date'] = execution_date
        logger.info(f"Установлена дата выполнения: {execution_date}")

        # Создаем экземпляр парсера, передав ему его собственный логгер
        parser = MultiStepOzonParser(config, logger=logger)

        # Запускаем парсер
        logger.debug("Запуск парсера с собственным логгером")
        result = parser.run_parser(save_to_file=True, output_format='json')

        # Логгируем результат
        logger.info(f"Микросервис парсера завершен успешно. Результат: {result}")

        return result

    except Exception as e:
        # Логгируем ошибку
        logger.error(f"Ошибка при выполнении микросервиса парсера: {e}", exc_info=True)
        raise


def run_upload_microservice(parsing_result=None):
    """
    Запускает изолированный микросервис загрузчика данных в Google Sheets с его собственным логгером

    Args:
        parsing_result: Результат работы микросервиса парсера (для передачи данных)

    Returns:
        dict: Результат выполнения изолированного микросервиса загрузчика
    """
    # Создаем логгер для загрузчика
    logger = create_uploader_logger()

    # Логгируем начало процесса
    logger.info("Запуск изолированного микросервиса загрузчика данных в Google Sheets")

    try:
        # Подготовим параметры подключения для изолированного микросервиса
        connection_params = prepare_connection_params()

        # Подготовим данные для загрузки из результата парсинга
        upload_data_list = prepare_upload_data(parsing_result)

        # Проверим подключение к Google Sheets
        logger.info("Проверка подключения к Google Sheets...")
        connection_result = test_upload_connection(connection_params, logger=logger)
        logger.info(f"Результат проверки подключения: {connection_result}")

        if not connection_result.get("success", False):
            logger.error("Подключение к Google Sheets не удалось")
            return {"success": False, "error": "Не удалось подключиться к Google Sheets"}

        # Загрузим данные в Google Sheets
        logger.info(f"Загрузка данных в Google Sheets: {len(upload_data_list)} записей")
        upload_result = upload_batch_data(
            data_list=upload_data_list,
            connection_params=connection_params,
            logger=logger,
            strategy="update_or_append"  # Стратегия: обновить если существует, иначе добавить
        )

        # Логгируем результат
        logger.info(f"Изолированный микросервис загрузчика завершен успешно. Результат: {upload_result}")

        return upload_result

    except Exception as e:
        # Логгируем ошибку
        logger.error(f"Ошибка при выполнении изолированного микросервиса загрузчика: {e}", exc_info=True)
        raise


def prepare_connection_params():
    """
    Подготавливает параметры подключения к Google Sheets для изолированного микросервиса

    Returns:
        dict: Параметры подключения к Google Sheets
    """
    # Импортируем классы из нового изолированного микросервиса
    from scheduler_runner.utils.uploader.core.providers.google_sheets.google_sheets_data_models import TableConfig, ColumnDefinition, ColumnType

    # Получаем оригинальную конфигурацию
    original_table_config = KPI_GOOGLE_SHEETS_CONFIG["TABLE_CONFIG"]

    # Создаем новую конфигурацию с использованием классов из изолированного микросервиса,
    # но с параметрами из оригинальной конфигурации
    new_columns = []
    for col in original_table_config.columns:
        # Преобразуем старый тип колонки в новый
        new_column_type = ColumnType.DATA  # по умолчанию
        if col.column_type.name == 'DATA':
            new_column_type = ColumnType.DATA
        elif col.column_type.name == 'FORMULA':
            new_column_type = ColumnType.FORMULA
        elif col.column_type.name == 'CALCULATED':
            new_column_type = ColumnType.CALCULATED
        elif col.column_type.name == 'IGNORE':
            new_column_type = ColumnType.IGNORE

        new_columns.append(
            ColumnDefinition(
                name=col.name,
                column_type=new_column_type,
                required=col.required,
                formula_template=col.formula_template,
                unique_key=col.unique_key,
                data_key=col.data_key,
                column_letter=col.column_letter
            )
        )

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


def prepare_upload_data(parsing_result=None):
    """
    Подготавливает данные для загрузки в Google Sheets из результата парсинга

    Args:
        parsing_result: Результат работы микросервиса парсера

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

        # Добавляем любые другие поля, которые могут быть полезны
        for key, value in parsing_result.items():
            if key not in ['summary', 'location_info', 'execution_date', 'extraction_timestamp', 'source_url']:
                formatted_record[key.title()] = value

        # Добавляем timestamp с текущим временем
        formatted_record['timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Проверяем, что обязательные поля присутствуют
        if 'Дата' in formatted_record and 'ПВЗ' in formatted_record:
            upload_data_list.append(formatted_record)
        else:
            # Если обязательные поля отсутствуют, используем старую логику
            upload_record = transform_record_for_upload(parsing_result)
            if upload_record:
                upload_data_list.append(upload_record)

    # Если нет результатов парсинга, возвращаем пустой список
    # (в продуктивной версии не используем тестовые данные)
    return upload_data_list


def transform_record_for_upload(record):
    """
    Преобразует отдельную запись из результата парсинга в формат, подходящий для загрузки в Google Sheets

    Args:
        record: Отдельная запись из результата парсинга

    Returns:
        dict: Преобразованная запись для загрузки в Google Sheets
    """
    if not isinstance(record, dict):
        return None

    # Преобразуем поля результата парсинга в поля таблицы Google Sheets
    # Эта логика может варьироваться в зависимости от структуры результата парсинга
    upload_record = {}

    # Пример преобразования - может потребоваться адаптация под реальную структуру данных
    field_mapping = {
        'date': 'Дата',
        'pvz': 'ПВЗ',
        'issued_packages': 'Количество выдач',
        'direct_flow': 'Прямой поток',
        'return_flow': 'Возвратный поток'
    }

    for source_field, target_field in field_mapping.items():
        if source_field in record:
            upload_record[target_field] = record[source_field]

    # Если в записи есть поля с другими названиями, добавим их тоже
    for key, value in record.items():
        if key not in field_mapping and key not in ['summary', 'details', 'timestamp']:
            # Приведем название поля к формату, используемому в Google Sheets
            formatted_key = key.replace('_', ' ').title()
            upload_record[formatted_key] = value

    # Убедимся, что все обязательные поля присутствуют
    required_fields = ['Дата', 'ПВЗ']
    for field in required_fields:
        if field not in upload_record:
            if field == 'Дата':
                upload_record[field] = datetime.now().strftime("%Y-%m-%d")
            elif field == 'ПВЗ':
                upload_record[field] = "DEFAULT_PVZ"

    return upload_record


def prepare_notification_data(parsing_result=None):
    """
    Подготавливает данные для уведомления из результата парсинга

    Args:
        parsing_result: Результат работы микросервиса парсера

    Returns:
        dict: Словарь с данными для уведомления
    """
    notification_data = {}

    if parsing_result and isinstance(parsing_result, dict):
        # Извлекаем дату
        if 'execution_date' in parsing_result:
            # Преобразуем дату в формат DD.MM.YYYY для уведомления
            original_date = parsing_result['execution_date']
            try:
                parsed_date = datetime.strptime(original_date, "%Y-%m-%d")
                notification_data['date'] = parsed_date.strftime("%d.%m.%Y")
            except ValueError:
                notification_data['date'] = original_date

        # Извлекаем ПВЗ
        if 'location_info' in parsing_result:
            notification_data['pvz'] = parsing_result['location_info']

        # Извлекаем данные из summary
        if 'summary' in parsing_result and isinstance(parsing_result['summary'], dict):
            summary = parsing_result['summary']

            # Извлекаем количество выдач
            if 'giveout' in summary and isinstance(summary['giveout'], dict) and 'value' in summary['giveout']:
                notification_data['issued_packages'] = summary['giveout']['value']

            # Извлекаем прямой поток
            if 'direct_flow_total' in summary and isinstance(summary['direct_flow_total'], dict):
                if 'total_carriages' in summary['direct_flow_total']:
                    notification_data['direct_flow'] = summary['direct_flow_total']['total_carriages']

            # Извлекаем возвратный поток
            if 'return_flow_total' in summary and isinstance(summary['return_flow_total'], dict):
                if 'total_carriages' in summary['return_flow_total']:
                    notification_data['return_flow'] = summary['return_flow_total']['total_carriages']

    return notification_data


def format_notification_message(notification_data):
    """
    Форматирует сообщение для уведомления в Telegram

    Args:
        notification_data: Данные для формирования сообщения

    Returns:
        str: Отформатированное сообщение для уведомления
    """
    # Шаблон сообщения
    message_template = "📊 KPI отчет за {date}\nПВЗ: {pvz}\nВыдач: {issued_packages}\nПрямой поток: {direct_flow}\nВозвратный поток: {return_flow}"

    # Заполняем шаблон данными
    message = message_template.format(
        date=notification_data.get('date', 'Неизвестно'),
        pvz=notification_data.get('pvz', 'Неизвестно'),
        issued_packages=notification_data.get('issued_packages', 0),
        direct_flow=notification_data.get('direct_flow', 0),
        return_flow=notification_data.get('return_flow', 0)
    )

    return message


def send_notification_microservice(notification_message, logger=None):
    """
    Отправляет уведомление через изолированный микросервис уведомлений

    Args:
        notification_message: Сообщение для отправки
        logger: Объект логгера

    Returns:
        dict: Результат отправки уведомления
    """
    if logger is None:
        logger = create_uploader_logger()  # Используем тот же логгер, что и для загрузчика

    logger.info("Подготовка к отправке уведомления в Telegram...")

    try:
        # Подготовим параметры подключения из конфигурации поддомена
        from scheduler_runner.tasks.reports.config.reports_paths import REPORTS_PATHS

        # Читаем параметры из REPORTS_PATHS
        token = REPORTS_PATHS.get("TELEGRAM_TOKEN")
        chat_id = REPORTS_PATHS.get("TELEGRAM_CHAT_ID")

        if not token or not chat_id:
            logger.error("Не все параметры подключения присутствуют в REPORTS_PATHS")
            if not token:
                logger.error("Отсутствует TELEGRAM_TOKEN")
            if not chat_id:
                logger.error("Отсутствует TELEGRAM_CHAT_ID")
            return {"success": False, "error": "Отсутствуют параметры подключения для Telegram"}

        # Подготовим параметры подключения
        connection_params = {
            "TELEGRAM_BOT_TOKEN": token,
            "TELEGRAM_CHAT_ID": chat_id
        }

        # Проверим подключение к Telegram
        logger.info("Проверка подключения к Telegram...")
        connection_result = test_notification_connection(connection_params, logger=logger)
        logger.info(f"Результат проверки подключения к Telegram: {connection_result}")

        if not connection_result.get("success", False):
            logger.error("Подключение к Telegram не удалось")
            return {"success": False, "error": "Не удалось подключиться к Telegram"}

        # Отправим уведомление
        logger.info(f"Отправка уведомления в Telegram: {len(notification_message)} символов")
        notification_result = send_notification(
            message=notification_message,
            connection_params=connection_params,
            logger=logger
        )

        logger.info(f"Результат отправки уведомления: {notification_result}")
        return notification_result

    except Exception as e:
        logger.error(f"Ошибка при отправке уведомления: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


def main():
    """
    Основная функция продуктового процессора домена reports
    """
    # Парсим аргументы командной строки
    parser = argparse.ArgumentParser(description='Продуктовый процессор домена reports')
    parser.add_argument('--execution_date', '-d', 
                       help='Дата выполнения в формате YYYY-MM-DD (по умолчанию используется текущая дата)')
    parser.add_argument('--detailed_logs', action='store_true', 
                       help='Включить детализированное логирование')
    
    args = parser.parse_args()
    
    execution_date = args.execution_date
    detailed_logs = args.detailed_logs

    try:
        # Запускаем микросервис парсинга с его собственным логгером
        parsing_result = run_parsing_microservice(execution_date=execution_date)

        # Проверяем, что парсинг прошел успешно (проверяем наличие ключевых полей)
        if parsing_result and isinstance(parsing_result, dict) and ('summary' in parsing_result or 'issued_packages' in parsing_result):
            # Запускаем микросервис загрузки данных в Google Sheets с его собственным логгером
            upload_result = run_upload_microservice(parsing_result)

            # Если загрузка данных прошла успешно, отправляем уведомление
            if upload_result and upload_result.get("success", False):
                # Подготовим данные для уведомления
                notification_data = prepare_notification_data(parsing_result)

                # Форматируем сообщение для уведомления
                notification_message = format_notification_message(notification_data)

                # Отправляем уведомление
                notification_result = send_notification_microservice(notification_message)
            else:
                # Логгируем, что загрузчик завершился с ошибкой
                logger = create_uploader_logger()
                logger.warning("Микросервис загрузчика завершился с ошибкой, пропускаем отправку уведомления")
        else:
            # Логгируем, что парсер не завершился успешно
            logger = create_parser_logger()
            logger.warning("Микросервис парсера не завершился успешно, пропускаем загрузку данных и уведомление")

        # Здесь может быть дополнительная логика центрального процессора:
        # - обработка результатов
        # - запуск других микросервисов
        # - контроль последовательности выполнения
        # - отчетность на вышестоящий уровень

    except Exception as e:
        logger = configure_logger(user="reports_domain", task_name="Processor", detailed=detailed_logs)
        logger.error(f"Произошла ошибка в продуктовом процессоре: {e}", exc_info=True)
        raise

    logger = configure_logger(user="reports_domain", task_name="Processor", detailed=detailed_logs)
    logger.info("Продуктовый процессор домена reports завершен успешно")


if __name__ == "__main__":
    main()