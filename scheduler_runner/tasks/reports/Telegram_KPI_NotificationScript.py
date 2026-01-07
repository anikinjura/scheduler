"""
Telegram_KPI_NotificationScript.py

Скрипт для автоматической отправки KPI данных отчетов ОЗОН в Telegram.
Использует новую архитектуру с универсальной загрузкой данных и форматированием.

Функции:
- Загрузка KPI данных отчетов через универсальную утилиту load_reports_data
- Форматирование KPI данных для уведомления в Telegram
- Отправка уведомления в Telegram через утилиту notify.py
- Обеспечение логирования процесса

Архитектура:
- Использует конфигурацию из Telegram_KPI_NotificationScript_config.py
- Использует универсальный модуль scheduler_runner/utils/google_sheets.py для загрузки данных
- Использует утилиту scheduler_runner/utils/notify.py для отправки уведомлений
- Использует транслитерацию для кириллических имен ПВЗ при поиске файлов
- Обеспечивает уникальность записей с помощью Id столбца с формулой (для совместимости)

Author: anikinjura
Version: 3.0.0 (новая архитектура)
"""
__version__ = '1.0.0'

import argparse
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, Any

# Добавляем корень проекта в sys.path для корректного импорта
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from scheduler_runner.tasks.reports.config.scripts.Telegram_KPI_NotificationScript_config import SCRIPT_CONFIG
from scheduler_runner.tasks.reports.utils.load_reports_data import load_reports_data
from scheduler_runner.utils.notify import send_telegram_message
from scheduler_runner.utils.logging import configure_logger
from scheduler_runner.utils.system import SystemUtils


def parse_arguments() -> argparse.Namespace:
    """
    Парсит аргументы командной строки для скрипта уведомлений в Telegram.

    --report_date              - дата отчета в формате YYYY-MM-DD (по умолчанию сегодняшняя дата)
    --detailed_logs            - включить детализированные логи
    --pvz_id                   - идентификатор ПВЗ для загрузки отчета
    """
    parser = argparse.ArgumentParser(
        description="Скрипт для отправки уведомлений с KPI данными отчетов ОЗОН в Telegram",
        epilog="Пример: python Telegram_KPI_NotificationScript.py --report_date 2026-01-02 --detailed_logs"
    )
    parser.add_argument(
        "--report_date",
        type=str,
        help="Дата отчета в формате YYYY-MM-DD (по умолчанию сегодняшняя дата)"
    )
    parser.add_argument(
        "--detailed_logs",
        action="store_true",
        default=False,
        help="Включить детализированные логи"
    )
    parser.add_argument(
        "--pvz_id",
        type=str,
        help="Идентификатор ПВЗ для загрузки отчета"
    )

    return parser.parse_args()


def load_kpi_report_data(report_date: str, pvz_id: str) -> Dict[str, Any]:
    """
    Загружает KPI данные отчетов через универсальную утилиту.

    Args:
        report_date: дата отчета в формате YYYY-MM-DD
        pvz_id: идентификатор ПВЗ

    Returns:
        Dict[str, Any]: объединенные данные отчетов в универсальном формате
    """
    from scheduler_runner.tasks.reports.config.scripts.Telegram_KPI_NotificationScript_config import SCRIPT_CONFIG
    
    logger = configure_logger(
        user=SCRIPT_CONFIG["USER"],
        task_name=SCRIPT_CONFIG["TASK_NAME"],
        detailed=False  # логирование здесь не нужно, будет в основном скрипте
    )
    
    logger.info(f"Загрузка KPI данных отчетов за {report_date} для ПВЗ {pvz_id}")
    
    try:
        # Загружаем данные через универсальную утилиту
        raw_data = load_reports_data(
            report_date=report_date,
            pvz_id=pvz_id,
            config=SCRIPT_CONFIG["REPORT_CONFIGS"]
        )
        
        logger.info(f"Данные отчетов загружены: {bool(raw_data)}")
        if raw_data:
            logger.info(f"Ключи данных: {list(raw_data.keys())[:10]}...")  # первые 10 ключей
        
        return raw_data
        
    except Exception as e:
        logger.error(f"Ошибка при загрузке данных отчетов: {e}")
        raise


def format_notification_message(report_data: Dict[str, Any]) -> str:
    """
    Форматирует данные отчета для уведомления в Telegram.

    Args:
        report_data: объединенные данные отчетов из универсальной утилиты

    Returns:
        str: отформатированное сообщение для Telegram
    """
    logger = configure_logger(
        user=SCRIPT_CONFIG["USER"],
        task_name=SCRIPT_CONFIG["TASK_NAME"],
        detailed=False
    )

    logger.info("Форматирование данных для уведомления в Telegram...")

    try:
        # Проверяем, есть ли реальные данные (а не только метаинформация)
        actual_data_keys = [k for k in report_data.keys() if not k.startswith('_')]

        if not actual_data_keys or len(actual_data_keys) <= 2:  # только метаинформация
            # Формируем сообщение об ошибке
            report_date = report_data.get('_report_date', datetime.now().strftime('%Y-%m-%d'))
            pvz_id = report_data.get('_pvz_id', 'Неизвестный ПВЗ')

            # Получаем информацию о том, какие файлы искались
            from scheduler_runner.tasks.reports.config.scripts.Telegram_KPI_NotificationScript_config import REPORT_CONFIGS
            searched_files = []
            for config in REPORT_CONFIGS:
                if config.enabled:
                    # Формируем имя файла для поиска
                    try:
                        date_obj = datetime.strptime(report_date, '%Y-%m-%d')
                        formatted_date = date_obj.strftime('%Y%m%d')  # используем стандартный формат даты

                        template_params = {
                            'date': formatted_date,
                            'pvz_id': SystemUtils.cyrillic_to_translit(pvz_id) if pvz_id else '*'
                        }

                        try:
                            expected_filename = config.file_pattern.format(**template_params)
                            searched_files.append(f"- {expected_filename}")
                        except KeyError as e:
                            searched_files.append(f"- {config.file_pattern} (не удалось сформировать: {e})")
                    except Exception:
                        searched_files.append(f"- {config.file_pattern} (не удалось сформировать)")

            error_message = (
                "⚠️ ОШИБКА: Данные отчетов не найдены\n"
                f"Дата: {report_date}\n"
                f"ПВЗ: {pvz_id}\n"
                "Поиск файлов:\n" + "\n".join(searched_files) + "\n"
                "Проверьте имена файлов и шаблоны поиска."
            )

            logger.warning(f"Данные отчетов не найдены, сформировано сообщение об ошибке")
            return error_message

        # Извлекаем основные данные из отчета
        date = report_data.get('_report_date', datetime.now().strftime('%d.%m.%Y'))
        pvz = report_data.get('pvz_info', report_data.get('_pvz_id', 'Неизвестный ПВЗ'))

        # Извлекаем KPI метрики
        issued_packages = report_data.get('issued_packages', report_data.get('total_packages', 0))
        direct_flow = report_data.get('direct_flow_count',
                                    report_data.get('direct_flow_data', {}).get('total_items_count', 0))
        return_flow = report_data.get('return_flow_data', {}).get('total_items_count', 0)

        # Формируем сообщение по шаблону из конфигурации
        message_template = SCRIPT_CONFIG.get("MESSAGE_TEMPLATE",
            "📊 KPI отчет за {date}\nПВЗ: {pvz}\nВыдач: {issued_packages}\nПрямой поток: {direct_flow}\nВозвратный поток: {return_flow}")

        message = message_template.format(
            date=date,
            pvz=pvz,
            issued_packages=issued_packages,
            direct_flow=direct_flow,
            return_flow=return_flow
        )

        logger.info(f"Сообщение для Telegram сформировано: {len(message)} символов")
        return message

    except Exception as e:
        logger.error(f"Ошибка при формировании сообщения для Telegram: {e}")
        # Возвращаем базовое сообщение об ошибке
        return f"⚠️ Ошибка формирования уведомления: {str(e)}"


def _format_for_google_sheets(data: Dict[str, Any], report_date: str, pvz_id: str) -> Dict[str, Any]:
    """
    Форматирует данные для совместимости с Google Sheets структурой.

    Args:
        data: данные отчета
        report_date: дата отчета
        pvz_id: идентификатор ПВЗ

    Returns:
        Dict[str, Any]: данные в формате, совместимом с Google Sheets
    """
    # Преобразуем дату из YYYY-MM-DD в DD.MM.YYYY
    try:
        date_obj = datetime.strptime(report_date, '%Y-%m-%d')
        formatted_date = date_obj.strftime('%d.%m.%Y')
    except ValueError:
        formatted_date = report_date  # если формат не распознан, используем как есть

    # Формируем структуру данных для Google Sheets
    result = {
        'id': '',  # будет заполнен формулой в таблице
        'Дата': formatted_date,
        'ПВЗ': data.get('pvz_info', pvz_id),
        'Количество выдач': data.get('issued_packages', data.get('total_packages', 0)),
        'Прямой поток': data.get('direct_flow_count',
                               data.get('direct_flow_data', {}).get('total_items_count', 0)),
        'Возвратный поток': data.get('return_flow_data', {}).get('total_items_count', 0)
    }

    return result


def validate_report_data(data: Dict[str, Any]) -> bool:
    """
    Проверяет корректность данных для уведомлений.

    Args:
        data: данные для проверки

    Returns:
        bool: True если данные корректны
    """
    required_fields = ['Дата', 'ПВЗ']

    for field in required_fields:
        if field not in data or not data[field]:
            return False

    # Проверяем формат даты
    try:
        datetime.strptime(data['Дата'], '%d.%m.%Y')
    except ValueError:
        return False

    return True


def get_report_summary(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Возвращает сводку по отчету.

    Args:
        data: данные отчета

    Returns:
        Dict[str, Any]: сводка по отчету
    """
    summary = {
        'report_date': data.get('Дата'),
        'pvz_id': data.get('ПВЗ'),
        'total_fields': len(data),
        'has_issued_packages': 'Количество выдач' in data,
        'has_direct_flow': 'Прямой поток' in data,
        'has_return_flow': 'Возвратный поток' in data
    }

    if 'Количество выдач' in data:
        summary['issued_packages'] = data['Количество выдач']

    if 'Прямой поток' in data:
        summary['direct_flow'] = data['Прямой поток']

    if 'Возвратный поток' in data:
        summary['return_flow'] = data['Возвратный поток']

    return summary


def main() -> None:
    """Основная функция управления процессом отправки уведомлений в Telegram"""
    args = parse_arguments()

    # Настройка логгера
    logger = configure_logger(
        user=SCRIPT_CONFIG["USER"],
        task_name=SCRIPT_CONFIG["TASK_NAME"],
        detailed=args.detailed_logs or SCRIPT_CONFIG["DETAILED_LOGS"]
    )

    try:
        # Получаем PVZ_ID из конфигурации, если не указан в аргументах
        from config.base_config import PVZ_ID
        pvz_id = args.pvz_id or PVZ_ID

        # Загружаем данные отчетов через новую архитектуру
        logger.info("Загрузка KPI данных отчетов...")
        raw_data = load_reports_data(
            report_date=args.report_date,
            pvz_id=pvz_id,
            config=SCRIPT_CONFIG["REPORT_CONFIGS"]
        )
        
        if not raw_data:
            logger.warning("Нет данных для формирования уведомления в Telegram")
            return

        # Форматируем сообщение для Telegram
        logger.info("Форматирование сообщения для Telegram...")
        telegram_message = format_notification_message(raw_data)
        
        if not telegram_message:
            logger.error("Не удалось сформировать сообщение для Telegram")
            return

        # Отправляем уведомление в Telegram через утилиту notify
        logger.info("Отправка уведомления в Telegram...")
        success, result = send_telegram_message(
            token=SCRIPT_CONFIG["TELEGRAM_BOT_TOKEN"],
            chat_id=SCRIPT_CONFIG["TELEGRAM_CHAT_ID"],
            message=telegram_message
        )

        if success:
            logger.info(f"Уведомление успешно отправлено в Telegram: {result}")
        else:
            logger.error(f"Ошибка при отправке уведомления в Telegram: {result}")

    except FileNotFoundError as e:
        logger.error(f"Файл отчета не найден: {e}")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}", exc_info=True)


if __name__ == "__main__":
    main()