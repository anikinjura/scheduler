#!/usr/bin/env python3
"""
OzonGiveoutReportParser.py

Парсер для извлечения данных о выдачах из системы Ozon.
"""
__version__ = '1.0.0'

import sys
import os
import re

# Добавляем путь к модулю конфигурации
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from scheduler_runner.tasks.reports.parser_base.BaseReportParser import BaseReportParser
from scheduler_runner.tasks.reports.config.scripts.OzonGiveoutReportParser_config import SCRIPT_CONFIG
from typing import Dict, Any


class OzonGiveoutReportParser(BaseReportParser):
    DATA_SOURCE_NAME = "Ozon"
    
    def __init__(self, config, logger=None):
        super().__init__(config, logger)
    
    # === АБСТРАКТНЫЕ МЕТОДЫ ===
    
    def get_report_type(self) -> str:
        """Возвращает тип отчета"""
        return 'giveout'
    
    def get_report_schema(self) -> Dict[str, Any]:
        """Возвращает схему данных отчета"""
        return self.config.get('REPORT_DATA_SCHEMA', {})
    
    def extract_specific_data(self) -> Dict[str, Any]:
        """Извлекает специфичные данные для отчета о выдачах"""
        # Сначала убедимся, что установлен правильный ПВЗ
        pvz_id = self.config.get('PVZ_ID', '')
        result = self.ensure_data_source(
            required_source=pvz_id,
            source_type='pvz'
        )

        if result['success']:
            if self.logger:
                self.logger.info(f"ПВЗ установлен успешно: {result['current_source']}")
        else:
            if self.logger:
                self.logger.warning(f"Не удалось установить ПВЗ: {result['message']}")

        specific_data = {}

        # Извлекаем информацию о ПВЗ (уже установленный)
        pvz_info = result['current_source']
        specific_data['pvz_info'] = pvz_info

        # Извлекаем количество выданных посылок
        issued_packages = self._extract_issued_packages()
        specific_data['issued_packages'] = issued_packages

        return specific_data

    # === ДОПОЛНИТЕЛЬНЫЕ МЕТОДЫ ===

    def _extract_issued_packages(self) -> int:
        """Извлекает количество выданных посылок"""
        giveout_selector = self.config.get('SELECTORS', {}).get('GIVEOUT_COUNT', '')

        if giveout_selector:
            giveout_text = self.extract_element_by_xpath(giveout_selector)

            if giveout_text:
                numbers = re.findall(r'\d+', giveout_text.replace(',', '').replace(' ', ''))
                if numbers:
                    return int(numbers[0])

        return 0


def main():
    """Основная функция запуска парсера"""
    parser = OzonGiveoutReportParser(SCRIPT_CONFIG)
    
    try:
        parser.run_parser_with_params(
            config_module=sys.modules[__name__],
            date_format='%Y-%m-%d',
            file_pattern='{report_type}_report_{pvz_id}_{date}.json',
            target_url_template=SCRIPT_CONFIG.get('ERP_URL_TEMPLATE'),
            source_name='Ozon Giveout Report'
        )
    except Exception as e:
        print(f"Ошибка при выполнении парсера: {e}")
        sys.exit(1)
    finally:
        parser.close()


if __name__ == "__main__":
    main()