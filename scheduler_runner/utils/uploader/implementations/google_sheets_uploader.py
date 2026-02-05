"""
Базовый класс для загрузки отчетов в Google Sheets

Архитектура:
- BaseUploader (абстрактный класс) → BaseReportUploader (абстрактный класс) → GoogleSheetsUploader (абстрактный класс) → конкретные реализации
- Добавляет специфичную логику для работы с Google Sheets
- Включает методы для подключения к Google Sheets API
- Переопределяет метод _perform_upload для загрузки в Google Sheets
- Обеспечивает единообразную работу с Google Sheets для всех дочерних классов

=== ОПИСАНИЕ МЕТОДОВ ===

Метод _establish_connection() - устанавливает подключение к Google Sheets API с использованием учетных данных
Метод _close_connection() - закрывает подключение к Google Sheets API
Метод _perform_upload() - выполняет загрузку данных в Google Sheets с использованием конфигурации таблицы
Метод _perform_upload_process() - реализует основной процесс загрузки отчетов в Google Sheets
"""

from typing import Dict, Any, Optional
from pathlib import Path

from ..core.base_report_uploader import BaseReportUploader
from ..core.providers.google_sheets.google_sheets_core import GoogleSheetsReporter, retry_on_api_error
from ..core.providers.google_sheets.google_sheets_data_models import TableConfig


class GoogleSheetsUploader(BaseReportUploader):
    """
    Базовый класс для загрузки отчетов в Google Sheets.
    
    Этот класс расширяет BaseReportUploader, добавляя специфичную функциональность
    для работы с Google Sheets, включая подключение к API, загрузку данных
    и работу с конфигурацией таблицы.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None, logger=None):
        """
        Инициализация загрузчика Google Sheets с конфигурацией

        Args:
            config: Конфигурация загрузчика Google Sheets
            logger: Объект логгера (если не передан, будет использован внутренний логгер)
        """
        super().__init__(config, logger=logger)

        if self.logger:
            self.logger.trace("Попали в метод GoogleSheetsUploader.__init__")
            self.logger.debug(f"Инициализация GoogleSheetsUploader с конфигурацией: {list(config.keys()) if config else 'пустая конфигурация'}")

        # Инициализируем Google Sheets Reporter
        self.sheets_reporter = None

        # Инициализируем параметры подключения
        self.credentials_path = self.config.get("CREDENTIALS_PATH", "")
        self.spreadsheet_id = self.config.get("SPREADSHEET_ID", "")
        self.worksheet_name = self.config.get("WORKSHEET_NAME", "Sheet1")
        self.table_config = self.config.get("TABLE_CONFIG")

    def _establish_connection(self) -> bool:
        """
        Установление подключения к Google Sheets API

        Returns:
            bool: True если подключение установлено
        """
        if self.logger:
            self.logger.trace("Попали в метод GoogleSheetsUploader._establish_connection")
            self.logger.debug(f"Попытка подключения к Google Sheets: {self.spreadsheet_id}, лист: {self.worksheet_name}")

        try:
            self.logger.info(f"Подключение к Google Sheets: {self.spreadsheet_id}, лист: {self.worksheet_name}")

            # Проверяем существование файла учетных данных
            if not Path(self.credentials_path).exists():
                self.logger.error(f"Файл учетных данных не найден: {self.credentials_path}")
                return False

            # Создаем экземпляр GoogleSheetsReporter с переданными параметрами
            self.sheets_reporter = GoogleSheetsReporter(
                credentials_path=self.credentials_path,
                spreadsheet_name=self.spreadsheet_id,
                worksheet_name=self.worksheet_name,
                table_config=self.table_config
            )

            self.logger.info("Подключение к Google Sheets успешно установлено")
            return True

        except Exception as e:
            self.logger.error(f"Ошибка при подключении к Google Sheets: {e}")
            return False

    def _close_connection(self) -> bool:
        """
        Закрытие подключения к Google Sheets API

        Returns:
            bool: True если подключение закрыто
        """
        if self.logger:
            self.logger.trace("Попали в метод GoogleSheetsUploader._close_connection")
            self.logger.debug("Начало процесса закрытия подключения к Google Sheets")

        try:
            if self.sheets_reporter:
                # Нет необходимости в явном отключении для Google Sheets
                self.sheets_reporter = None
                self.logger.info("Подключение к Google Sheets закрыто")

            return True

        except Exception as e:
            self.logger.error(f"Ошибка при закрытии подключения к Google Sheets: {e}")
            return False

    def _perform_upload(self, data: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        """
        Выполнение загрузки данных в Google Sheets

        Args:
            data: Данные для загрузки
            **kwargs: Дополнительные параметры

        Returns:
            Dict с результатами загрузки
        """
        if self.logger:
            self.logger.trace("Попали в метод GoogleSheetsUploader._perform_upload")
            self.logger.debug(f"Попытка загрузки данных в Google Sheets: {list(data.keys()) if isinstance(data, dict) else type(data)}")
            self.logger.debug(f"Стратегия загрузки: {kwargs.get('strategy', self.config.get('UPLOAD_STRATEGY', 'update_or_append'))}")
            self.logger.debug(f"Конфигурация таблицы: {self.table_config.worksheet_name if self.table_config else 'None'}")

        try:
            if not self.sheets_reporter:
                self.logger.error("Нет подключения к Google Sheets")
                return {"success": False, "error": "Нет подключения к Google Sheets"}

            # Получаем стратегию загрузки из конфигурации или параметров
            strategy = kwargs.get("strategy", self.config.get("UPLOAD_STRATEGY", "update_or_append"))

            # Выполняем загрузку данных с использованием GoogleSheetsReporter
            result = self.sheets_reporter.update_or_append_data_with_config(
                data=data,
                config=self.table_config,
                strategy=strategy
            )

            if self.logger:
                self.logger.debug(f"Результат загрузки в Google Sheets: {result}")

            return result

        except Exception as e:
            self.logger.error(f"Ошибка при загрузке данных в Google Sheets: {e}")
            if self.logger:
                import traceback
                self.logger.debug(f"Полный стек трейса ошибки: {traceback.format_exc()}")
            return {"success": False, "error": str(e)}

    def _perform_upload_process(self, **kwargs) -> Dict[str, Any]:
        """
        Выполнение процесса загрузки отчетов в Google Sheets
        
        Args:
            **kwargs: Дополнительные параметры
            
        Returns:
            Dict с результатами загрузки
        """
        try:
            self.logger.info("Начало процесса загрузки отчетов в Google Sheets...")
            
            # Получаем путь к данным отчета из конфигурации
            source_data_path = self.config.get("SOURCE_DATA_PATH", "")
            
            if not source_data_path:
                return {"success": False, "error": "Не указан путь к исходным данным"}
            
            # В зависимости от реализации, можем загружать данные из файла или использовать переданные
            # В реальной реализации здесь может быть логика загрузки данных из различных источников
            
            # Для примера, просто возвращаем успешный результат
            # Реальная реализация будет зависеть от конкретной задачи
            result = {
                "success": True,
                "message": "Процесс загрузки отчетов в Google Sheets завершен",
                "uploaded_count": 0,
                "failed_count": 0
            }
            
            self.logger.info("Процесс загрузки отчетов в Google Sheets завершен")
            return result
            
        except Exception as e:
            self.logger.error(f"Ошибка в процессе загрузки отчетов в Google Sheets: {e}")
            return {"success": False, "error": str(e)}

    def upload_multiple_reports(self, reports_data: list, **kwargs) -> Dict[str, Any]:
        """
        Загрузка нескольких отчетов в Google Sheets

        Args:
            reports_data: Список данных отчетов для загрузки
            **kwargs: Дополнительные параметры

        Returns:
            Dict с результатами загрузки нескольких отчетов
        """
        if self.logger:
            self.logger.trace("Попали в метод GoogleSheetsUploader.upload_multiple_reports")
            self.logger.debug(f"Попытка загрузки {len(reports_data) if reports_data else 0} отчетов в Google Sheets")

        if not self.connected:
            return {"success": False, "error": "Нет подключения к Google Sheets"}

        results = {
            "success": True,
            "uploaded": 0,
            "failed": 0,
            "details": []
        }

        for i, report_data in enumerate(reports_data):
            try:
                if self.logger:
                    self.logger.debug(f"Обработка отчета {i}: {list(report_data.keys()) if isinstance(report_data, dict) else type(report_data)}")

                # Добавляем метаданные отчета
                processed_data = self._add_report_metadata(report_data)

                # Загружаем отдельный отчет
                result = self.upload_data(processed_data, **kwargs)

                if result["success"]:
                    results["uploaded"] += 1
                else:
                    results["failed"] += 1

                results["details"].append({
                    "index": i,
                    "data_sample": str(report_data)[:100],  # Обрезаем для логирования
                    "result": result
                })

            except Exception as e:
                self.logger.error(f"Ошибка при загрузке отчета {i}: {e}")
                results["failed"] += 1
                results["details"].append({
                    "index": i,
                    "data_sample": str(report_data)[:100],
                    "result": {"success": False, "error": str(e)}
                })

        if self.logger:
            self.logger.debug(f"Результат пакетной загрузки: {results}")

        return results

    def get_sheet_info(self) -> Dict[str, Any]:
        """
        Получение информации о Google Sheets

        Returns:
            Dict с информацией о таблице
        """
        if self.logger:
            self.logger.trace("Попали в метод GoogleSheetsUploader.get_sheet_info")
            self.logger.debug(f"Попытка получения информации о таблице: {self.spreadsheet_id}, лист: {self.worksheet_name}")

        if not self.connected or not self.sheets_reporter:
            return {"success": False, "error": "Нет подключения к Google Sheets"}

        try:
            # Получаем заголовки таблицы
            headers = self.sheets_reporter.get_table_headers()

            # Получаем последнюю строку с данными
            last_row = self.sheets_reporter.get_last_row_with_data()

            info = {
                "success": True,
                "spreadsheet_id": self.spreadsheet_id,
                "worksheet_name": self.worksheet_name,
                "headers": headers,
                "last_data_row": last_row,
                "total_rows_with_data": last_row - 1 if last_row > 1 else 0  # минус строка заголовков
            }

            if self.logger:
                self.logger.debug(f"Информация о таблице получена: {info}")

            return info

        except Exception as e:
            self.logger.error(f"Ошибка при получении информации о таблице: {e}")
            return {"success": False, "error": str(e)}