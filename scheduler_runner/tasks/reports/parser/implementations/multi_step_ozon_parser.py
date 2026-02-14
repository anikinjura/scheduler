"""
Многошаговый парсер для сбора данных из системы Ozon

Этот парсер реализует многошаговый процесс сбора данных:
1. Данные по "giveout" - подотчет по выдачам
2. Данные по "Direct flow" - подотчет прямому потоку
3. Данные по "Return flow" - подотчет возвратному потоку
"""
__version__ = '0.0.1'

from datetime import datetime
from typing import Dict, Any

from ..core.ozon_report_parser import OzonReportParser


class MultiStepOzonParser(OzonReportParser):
    """
    Многошаговый парсер для сбора данных из системы Ozon
    """

    def __init__(self, config: Dict[str, Any], args=None, logger=None):
        """
        Инициализация многошагового парсера Ozon

        Args:
            config: Конфигурационный словарь с параметрами для работы парсера
            args: Аргументы командной строки (если не переданы, будут разобраны из sys.argv)
            logger: Объект логгера (если не передан, будет использован внутренний логгер)
        """
        if logger:
            logger.trace("Попали в метод MultiStepOzonParser.__init__")
        super().__init__(config, args, logger)

    def get_report_type(self) -> str:
        """
        Переопределяем абстрактный метод базового класса BaseReportParser.
        Метод возвращает тип отчета, который используется для идентификации и именования файлов.

        Returns:
            str: Тип отчета
        """
        if self.logger:
            self.logger.trace("Попали в метод MultiStepOzonParser.get_report_type")
        report_type = self.config.get("report_type", "multi_step_ozon_report")
        if self.logger:
            self.logger.debug(f"Тип отчета: {report_type}")
        return report_type


    def extract_report_data(self) -> Dict[str, Any]:
        """
        Переопределяем абстрактный метод базового класса BaseReportParser.
        Метод извлекает данные в зависимости от конфигурации мульти-шаговой обработки.
        Теперь все данные, включая общую информацию, обрабатываются через мульти-шаговую логику.

        Returns:
            Dict[str, Any]: Словарь с данными отчета
        """
        if self.logger:
            self.logger.trace("Попали в метод MultiStepOzonParser.extract_report_data")
        # Получаем результаты мульти-шаговой обработки
        multi_step_results = self.config.get('last_collected_data', {})
        if self.logger:
            self.logger.debug(f"Результаты мульти-шаговой обработки: {list(multi_step_results.keys())}")

        # Удаляем служебные поля из результата, чтобы не попали в финальный отчет
        cleaned_results = {}
        for key, value in multi_step_results.items():
            if isinstance(value, dict):
                # Удаляем служебные поля из вложенных словарей
                if '__STEP_SOURCE_URL__' in value:
                    if self.logger:
                        self.logger.debug(f"Удаление служебного поля __STEP_SOURCE_URL__ из результата для ключа {key}")
                    del value['__STEP_SOURCE_URL__']
                cleaned_results[key] = value
            else:
                cleaned_results[key] = value

        if self.logger:
            self.logger.debug(f"Результаты после очистки: {list(cleaned_results.keys())}")

        # Для совместимости с существующим кодом, извлекаем специфичные значения
        # из вложенной структуры summary, если она есть
        if 'summary' in cleaned_results:
            summary_data = cleaned_results['summary']
            if self.logger:
                self.logger.debug(f"Обработка данных summary: {list(summary_data.keys())}")
            # Извлекаем конкретные значения из summary и добавляем их на верхний уровень для совместимости
            if 'giveout' in summary_data:
                cleaned_results['giveout_count'] = summary_data['giveout']
                if self.logger:
                    self.logger.debug(f"Добавлено значение giveout_count: {summary_data['giveout']}")
            if 'direct_flow_total' in summary_data:
                cleaned_results['direct_flow_total'] = summary_data['direct_flow_total']
                if self.logger:
                    self.logger.debug(f"Добавлено значение direct_flow_total: {summary_data['direct_flow_total']}")
            if 'return_flow_total' in summary_data:
                cleaned_results['return_flow_total'] = summary_data['return_flow_total']
                if self.logger:
                    self.logger.debug(f"Добавлено значение return_flow_total: {summary_data['return_flow_total']}")

        if self.logger:
            self.logger.debug(f"Финальные результаты извлечения данных: {list(cleaned_results.keys())}")
        return cleaned_results

    def login(self) -> bool:
        """
        Переопределяем абстрактный метод базового класса BaseParser.
        Метод реализует логику аутентификации в системе. Для тестирования
        используем сохраненную сессию, поэтому просто возвращаем True.

        Returns:
            bool: True, если вход прошел успешно
        """
        if self.logger:
            self.logger.trace("Попали в метод MultiStepOzonParser.login")
            # Проверим, действительно ли у нас есть активная сессия браузера
            if hasattr(self, 'driver') and self.driver:
                try:
                    # Проверим, можно ли получить URL текущей страницы
                    current_url = self.driver.current_url
                    self.logger.info(f"Текущий URL сессии: {current_url}")
                    
                    # Проверим, можно ли получить заголовок страницы
                    title = self.driver.title
                    self.logger.info(f"Заголовок страницы: {title}")
                    
                    # Проверим, активна ли сессия
                    if self.driver.session_id:
                        self.logger.info("Сессия браузера активна, ID сессии: {}".format(self.driver.session_id[:10] + "..."))
                    else:
                        self.logger.warning("ID сессии отсутствует")
                        
                except Exception as e:
                    self.logger.error(f"Ошибка при проверке активности сессии: {e}")
            
            self.logger.info("Пропускаем авторизацию - используется сохраненная сессия")
        return True

    def logout(self) -> bool:
        """
        Переопределяем абстрактный метод базового класса BaseParser.
        Метод реализует логику завершения сессии. Для тестирования
        не требуется явный выход, поэтому просто возвращаем True.

        Returns:
            bool: True, если выход прошел успешно
        """
        if self.logger:
            self.logger.trace("Попали в метод MultiStepOzonParser.logout")
            self.logger.info("Пропускаем выход - Ozon не требует явного выхода")
        return True


def main(logger=None):
    """
    Основная функция запуска многошагового парсера Ozon

    Args:
        logger: Объект логгера (если не передан, будет использован внутренний логгер)
    """
    if logger:
        logger.trace("Попали в функцию MultiStepOzonParser.main")

    # Импортируем конфигурацию
    from ..configs.implementations.multi_step_ozon_config import MULTI_STEP_OZON_CONFIG

    # Используем подготовленную конфигурацию
    config = MULTI_STEP_OZON_CONFIG.copy()

    # Создание экземпляра парсера с передачей аргументов командной строки и логгера
    parser = MultiStepOzonParser(config, logger=logger)

    # Запуск парсера
    try:
        result = parser.run_parser(save_to_file=True, output_format='json')
        if logger:
            logger.info("Многошаговый парсинг Ozon завершен успешно.")
        else:
            print(f"Многошаговый парсинг Ozon завершен успешно.")
        print(f"Результат: {result}")
        if logger:
            logger.info(f"Результат: {result}")

        # Также выведем краткую информацию
        print("\n--- SUMMARY RESULTS ---")
        location_info = result.get('location_info', 'N/A')
        extraction_time = result.get('extraction_timestamp', 'N/A')
        giveout_count = result.get('giveout_count', 'N/A')
        direct_flow_total = result.get('direct_flow_total', 'N/A')
        return_flow_total = result.get('return_flow_total', 'N/A')
        file_saved_to = config['output_config']['dir']
        execution_date = config.get('execution_date', 'N/A')

        print(f"Location Info: {location_info}")
        print(f"Extraction Time: {extraction_time}")
        print(f"Giveout Count: {giveout_count}")
        print(f"Direct Flow Total: {direct_flow_total}")
        print(f"Return Flow Total: {return_flow_total}")
        print(f"File saved to: {file_saved_to}")
        print(f"Execution Date: {execution_date}")

        if logger:
            logger.info(f"Location Info: {location_info}")
            logger.info(f"Extraction Time: {extraction_time}")
            logger.info(f"Giveout Count: {giveout_count}")
            logger.info(f"Direct Flow Total: {direct_flow_total}")
            logger.info(f"Return Flow Total: {return_flow_total}")
            logger.info(f"File saved to: {file_saved_to}")
            logger.info(f"Execution Date: {execution_date}")

    except Exception as e:
        if logger:
            logger.error(f"Произошла ошибка при многошаговом парсинге: {e}", exc_info=True)
        else:
            print(f"Произошла ошибка при многошаговом парсинге: {e}")


if __name__ == "__main__":
    main()