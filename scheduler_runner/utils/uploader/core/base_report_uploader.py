"""
Базовый класс для загрузки отчетов в различные системы

Архитектура:
- Наследуется от BaseUploader
- Добавляет специфичную логику для работы с отчетами
- Включает методы валидации, форматирования и обработки данных отчетов
- Поддерживает различные форматы отчетов (JSON, CSV, XML и др.)
- Переопределяет метод upload_data для добавления специфичной логики загрузки отчетов
- Включает методы загрузки отчетов из различных источников
- Поддерживает передачу аргументов командной строки, включая дату отчета
- Включает методы для обработки метаданных отчетов
- Поддерживает мульти-шаговую загрузку с различными стратегиями

Изменения в версии 1.0.0:
- Создан базовый класс BaseReportUploader
- Добавлены методы загрузки отчетов из файлов
- Добавлены методы обработки метаданных отчетов
- Добавлена поддержка различных форматов отчетов
"""

import json
import csv
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, Any, Optional, List, Union
from datetime import datetime
import argparse

from .base_uploader import BaseUploader
from scheduler_runner.utils.logging import configure_logger


class BaseReportUploader(BaseUploader):
    """
    Базовый класс для загрузки отчетов в различные системы.
    
    Этот класс расширяет BaseUploader, добавляя специфичную функциональность
    для работы с отчетами, включая загрузку из файлов, обработку метаданных
    и поддержку различных форматов.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None, logger=None):
        """
        Инициализация загрузчика отчетов с конфигурацией

        Args:
            config: Конфигурация загрузчика отчетов
            logger: Объект логгера (если не передан, будет использован внутренний логгер)
        """
        super().__init__(config, logger=logger)

        if self.logger:
            self.logger.trace("Попали в метод BaseReportUploader.__init__")
            self.logger.debug(f"Инициализация BaseReportUploader с конфигурацией: {list(config.keys()) if config else 'пустая конфигурация'}")

        # Инициализируем аргументы командной строки
        self.args = None
        self.report_date = None
        self.pvz_id = None

    def _parse_arguments(self, args=None):
        """
        Разбор аргументов командной строки
        
        Args:
            args: Список аргументов для разбора. Если None, используются sys.argv[1:]
        """
        parser = argparse.ArgumentParser(
            description="Загрузчик отчетов в различные системы",
            epilog="Пример: python script.py --report_date 2026-01-02 --detailed_logs"
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

        self.args = parser.parse_args(args)
        
        # Обновляем дату отчета
        self._update_report_date()
        
        # Обновляем уровень детализации логов
        if self.args.detailed_logs:
            self.logger = configure_logger(
                user=self.config.get("USER", "system"),
                task_name=self.config.get("TASK_NAME", "BaseReportUploader"),
                detailed=True
            )

    def _update_report_date(self):
        """
        Обновление даты отчета на основе аргументов командной строки или конфигурации
        """
        # Приоритет: аргумент командной строки > конфигурация > текущая дата
        if self.args and self.args.report_date:
            self.report_date = self.args.report_date
        elif self.config.get("REPORT_DATE"):
            self.report_date = self.config["REPORT_DATE"]
        else:
            self.report_date = datetime.now().strftime('%Y-%m-%d')

        # Валидация формата даты
        try:
            datetime.strptime(self.report_date, '%Y-%m-%d')
        except ValueError:
            raise ValueError(f"Некорректный формат даты: {self.report_date}. Ожидается YYYY-MM-DD")

    def load_report_from_file(self, file_path: Union[str, Path], format_type: str = "auto") -> Dict[str, Any]:
        """
        Загрузка отчета из файла
        
        Args:
            file_path: Путь к файлу отчета
            format_type: Тип формата файла ('json', 'csv', 'xml', 'auto')
            
        Returns:
            Данные отчета в формате словаря
        """
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise FileNotFoundError(f"Файл отчета не найден: {file_path}")
        
        # Определяем формат, если не указан
        if format_type == "auto":
            suffix = file_path.suffix.lower()
            if suffix == ".json":
                format_type = "json"
            elif suffix == ".csv":
                format_type = "csv"
            elif suffix in [".xml", ".xsl", ".xlsx"]:
                format_type = "xml"  # для простоты считаем xlsx как xml
            else:
                raise ValueError(f"Неизвестный формат файла: {suffix}")
        
        self.logger.info(f"Загрузка отчета из файла: {file_path} (формат: {format_type})")
        
        try:
            if format_type == "json":
                return self._load_json_report(file_path)
            elif format_type == "csv":
                return self._load_csv_report(file_path)
            elif format_type == "xml":
                return self._load_xml_report(file_path)
            else:
                raise ValueError(f"Неподдерживаемый формат: {format_type}")
        except Exception as e:
            self.logger.error(f"Ошибка при загрузке отчета из файла {file_path}: {e}")
            raise

    def _load_json_report(self, file_path: Path) -> Dict[str, Any]:
        """
        Загрузка JSON отчета
        
        Args:
            file_path: Путь к JSON файлу
            
        Returns:
            Данные отчета
        """
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        self.logger.info(f"Загружен JSON отчет с {len(data) if isinstance(data, (list, dict)) else 'unknown'} элементами")
        return data

    def _load_csv_report(self, file_path: Path) -> Dict[str, Any]:
        """
        Загрузка CSV отчета
        
        Args:
            file_path: Путь к CSV файлу
            
        Returns:
            Данные отчета
        """
        data = []
        with open(file_path, 'r', encoding='utf-8', newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                data.append(dict(row))
        
        self.logger.info(f"Загружен CSV отчет с {len(data)} строками")
        return {"data": data, "format": "csv"}

    def _load_xml_report(self, file_path: Path) -> Dict[str, Any]:
        """
        Загрузка XML отчета
        
        Args:
            file_path: Путь к XML файлу
            
        Returns:
            Данные отчета
        """
        tree = ET.parse(file_path)
        root = tree.getroot()
        
        # Преобразуем XML в словарь
        def xml_to_dict(element):
            result = {}
            # Добавляем атрибуты
            result.update(element.attrib)
            # Добавляем текст, если есть
            if element.text and element.text.strip():
                if len(element) == 0:  # Если нет дочерних элементов
                    return element.text.strip()
                else:
                    result["text"] = element.text.strip()
            # Добавляем дочерние элементы
            for child in element:
                child_data = xml_to_dict(child)
                if child.tag in result:
                    # Если тег уже существует, делаем список
                    if not isinstance(result[child.tag], list):
                        result[child.tag] = [result[child.tag]]
                    result[child.tag].append(child_data)
                else:
                    result[child.tag] = child_data
            return result
        
        data = {root.tag: xml_to_dict(root)}
        
        self.logger.info(f"Загружен XML отчет с корневым элементом: {root.tag}")
        return data

    def upload_report(self, report_data: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        """
        Загрузка отчета в целевую систему
        
        Args:
            report_data: Данные отчета для загрузки
            **kwargs: Дополнительные параметры
            
        Returns:
            Dict с результатами загрузки отчета
        """
        if not self.connected:
            self.logger.error("Нет подключения к целевой системе")
            return {"success": False, "error": "Нет подключения к целевой системе"}
        
        try:
            self.logger.info("Начало загрузки отчета...")
            
            # Добавляем метаданные отчета
            processed_data = self._add_report_metadata(report_data)
            
            # Выполняем загрузку
            result = self.upload_data(processed_data, **kwargs)
            
            self.logger.info(f"Загрузка отчета завершена: {result}")
            return result
            
        except Exception as e:
            self.logger.error(f"Ошибка при загрузке отчета: {e}")
            return {"success": False, "error": str(e)}

    def _add_report_metadata(self, report_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Добавление метаданных отчета к данным

        Args:
            report_data: Исходные данные отчета

        Returns:
            Данные отчета с добавленными метаданными
        """
        if self.logger:
            self.logger.trace("Попали в метод BaseReportUploader._add_report_metadata")
            self.logger.debug(f"Добавление метаданных к данным отчета: {list(report_data.keys()) if isinstance(report_data, dict) else type(report_data)}")

        metadata = {
            "_report_date": self.report_date,
            "_pvz_id": self.pvz_id or self.config.get("PVZ_ID"),
            "_upload_timestamp": datetime.now().isoformat(),
            "_source_system": self.config.get("SOURCE_SYSTEM", "unknown"),
            "_target_system": self.config.get("TARGET_SYSTEM", "unknown")
        }

        if self.logger:
            self.logger.debug(f"Метаданные отчета: {list(metadata.keys())}")

        # Объединяем метаданные с данными отчета
        result = {**metadata, **report_data}

        if self.logger:
            self.logger.debug(f"Результат объединения: {list(result.keys()) if isinstance(result, dict) else type(result)}")

        return result

    def upload_report_from_file(self, file_path: Union[str, Path], format_type: str = "auto", **kwargs) -> Dict[str, Any]:
        """
        Загрузка отчета из файла в целевую систему
        
        Args:
            file_path: Путь к файлу отчета
            format_type: Тип формата файла ('json', 'csv', 'xml', 'auto')
            **kwargs: Дополнительные параметры
            
        Returns:
            Dict с результатами загрузки отчета из файла
        """
        try:
            # Загружаем данные отчета из файла
            report_data = self.load_report_from_file(file_path, format_type)
            
            # Загружаем отчет
            result = self.upload_report(report_data, **kwargs)
            
            return result
        except Exception as e:
            self.logger.error(f"Ошибка при загрузке отчета из файла {file_path}: {e}")
            return {"success": False, "error": str(e)}

    def validate_report_structure(self, report_data: Dict[str, Any], schema: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Валидация структуры отчета по схеме
        
        Args:
            report_data: Данные отчета для валидации
            schema: Схема валидации (если None, используется схема из конфигурации)
            
        Returns:
            Dict с результатами валидации
        """
        if schema is None:
            schema = self.config.get("VALIDATION_SCHEMA", {})
        
        if not schema:
            return {"success": True, "warnings": ["Схема валидации не указана"]}
        
        try:
            # Простая валидация структуры (в реальном приложении может быть сложнее)
            validation_errors = []
            validation_warnings = []
            
            # Проверяем обязательные поля
            required_fields = schema.get("required", [])
            for field in required_fields:
                if field not in report_data:
                    validation_errors.append(f"Отсутствует обязательное поле: {field}")
            
            # Проверяем типы данных
            field_types = schema.get("types", {})
            for field, expected_type in field_types.items():
                if field in report_data:
                    actual_value = report_data[field]
                    if not isinstance(actual_value, eval(expected_type)):
                        validation_warnings.append(f"Поле {field} имеет тип {type(actual_value).__name__}, ожидался {expected_type}")
            
            if validation_errors:
                return {
                    "success": False,
                    "errors": validation_errors,
                    "warnings": validation_warnings
                }
            else:
                return {
                    "success": True,
                    "warnings": validation_warnings
                }
                
        except Exception as e:
            self.logger.error(f"Ошибка при валидации структуры отчета: {e}")
            return {"success": False, "errors": [str(e)]}

    def format_report_output(self, data: Dict[str, Any], output_format: str = 'json', **kwargs) -> Union[Dict, str]:
        """
        Форматирование данных отчета в указанный формат
        
        Args:
            data: Данные отчета
            output_format: Формат вывода ('json', 'csv', 'xml')
            **kwargs: Дополнительные параметры форматирования
            
        Returns:
            Отформатированные данные
        """
        try:
            if output_format.lower() == 'json':
                return self._format_as_json(data, **kwargs)
            elif output_format.lower() == 'csv':
                return self._format_as_csv(data, **kwargs)
            elif output_format.lower() == 'xml':
                return self._format_as_xml(data, **kwargs)
            else:
                raise ValueError(f"Неподдерживаемый формат вывода: {output_format}")
        except Exception as e:
            self.logger.error(f"Ошибка при форматировании данных: {e}")
            raise

    def _format_as_json(self, data: Dict[str, Any], **kwargs) -> Dict:
        """Форматирование в JSON"""
        indent = kwargs.get('indent', 2)
        ensure_ascii = kwargs.get('ensure_ascii', False)
        
        # Возвращаем данные как есть, так как они уже в формате словаря
        return data

    def _format_as_csv(self, data: Dict[str, Any], **kwargs) -> str:
        """Форматирование в CSV"""
        import io
        import csv
        
        output = io.StringIO()
        fieldnames = list(data.keys())
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        
        writer.writeheader()
        writer.writerow(data)
        
        return output.getvalue()

    def _format_as_xml(self, data: Dict[str, Any], **kwargs) -> str:
        """Форматирование в XML"""
        def dict_to_xml(d, root_name="report"):
            import xml.etree.ElementTree as ET
            from xml.dom import minidom
            
            def build_xml(element, data):
                if isinstance(data, dict):
                    for key, value in data.items():
                        # Заменяем недопустимые символы в тегах
                        safe_key = str(key).replace(" ", "_").replace("-", "_")
                        child = ET.SubElement(element, safe_key)
                        build_xml(child, value)
                elif isinstance(data, list):
                    for i, item in enumerate(data):
                        child = ET.SubElement(element, "item")
                        child.set("index", str(i))
                        build_xml(child, item)
                else:
                    element.text = str(data)
            
            root = ET.Element(root_name)
            build_xml(root, d)
            
            # Форматируем XML для лучшего вида
            rough_string = ET.tostring(root, encoding='unicode')
            reparsed = minidom.parseString(rough_string)
            return reparsed.toprettyxml(indent="  ")[23:]  # Убираем декларацию XML
        
        return dict_to_xml(data)

    def get_report_statistics(self, report_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Получение статистики по отчету
        
        Args:
            report_data: Данные отчета
            
        Returns:
            Статистика по отчету
        """
        stats = {
            "total_records": 0,
            "fields_count": 0,
            "size_bytes": len(json.dumps(report_data, ensure_ascii=False).encode('utf-8')),
            "has_nested_objects": False,
            "data_types": {},
            "null_fields": []
        }
        
        def analyze_data(obj, level=0):
            if isinstance(obj, dict):
                stats["fields_count"] += len(obj)
                for key, value in obj.items():
                    if value is None:
                        stats["null_fields"].append(key)
                    analyze_data(value, level + 1)
                if level > 0:  # Не считаем корневой уровень
                    stats["has_nested_objects"] = True
            elif isinstance(obj, list):
                stats["total_records"] += len(obj)
                for item in obj:
                    analyze_data(item, level + 1)
            else:
                obj_type = type(obj).__name__
                if obj_type in stats["data_types"]:
                    stats["data_types"][obj_type] += 1
                else:
                    stats["data_types"][obj_type] = 1
        
        analyze_data(report_data)
        
        if stats["total_records"] == 0 and isinstance(report_data, dict):
            # Если это не список, считаем как одну запись
            stats["total_records"] = 1
        
        return stats

    def run_uploader(self, **kwargs) -> Dict[str, Any]:
        """
        Запуск процесса загрузки отчетов
        
        Args:
            **kwargs: Дополнительные параметры
            
        Returns:
            Dict с результатами выполнения загрузчика
        """
        try:
            self.logger.info("Запуск процесса загрузки отчетов...")
            
            # Подключаемся к целевой системе
            if not self.connect():
                return {"success": False, "error": "Не удалось подключиться к целевой системе"}
            
            # Выполняем загрузку (реализуется в дочерних классах)
            result = self._perform_upload_process(**kwargs)
            
            # Отключаемся от целевой системы
            self.disconnect()
            
            self.logger.info(f"Процесс загрузки завершен: {result}")
            return result
            
        except Exception as e:
            self.logger.error(f"Критическая ошибка в процессе загрузки: {e}")
            return {"success": False, "error": str(e)}

    def _perform_upload_process(self, **kwargs) -> Dict[str, Any]:
        """
        Выполнение процесса загрузки (должен быть реализован в дочерних классах)
        
        Args:
            **kwargs: Дополнительные параметры
            
        Returns:
            Dict с результатами загрузки
        """
        raise NotImplementedError("Метод _perform_upload_process должен быть реализован в дочернем классе")