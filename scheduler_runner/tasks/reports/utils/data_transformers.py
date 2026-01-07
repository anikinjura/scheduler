"""
data_transformers.py

Трансформеры данных для преобразования универсального формата
в специфические форматы для потребителей (MVP версия).

Особенности MVP:
- Только один трансформер для Google Sheets
- Простое преобразование без сложной логики
- Базовое форматирование дат

Author: anikinjura
Version: 3.0.0 (MVP)
"""

from datetime import datetime
from typing import Dict, Any, Optional


class DataTransformer:
    """
    Базовый класс для преобразования данных.
    В итерации 2 будут добавлены другие трансформеры.
    """

    def transform(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Преобразует данные из универсального формата.

        Args:
            raw_data: данные в универсальном формате

        Returns:
            Преобразованные данные для специфического потребителя
        """
        raise NotImplementedError


class GoogleSheetsTransformer(DataTransformer):
    """
    Преобразует данные для Google Sheets.

    Формат результата:
    {
        'id': '',
        'Дата': '05.01.2026',
        'ПВЗ': 'Название ПВЗ',
        'Количество выдач': 100,
        'Прямой поток': 50,
        'Возвратный поток': 25
    }
    """

    def transform(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Преобразует универсальные данные в формат для Google Sheets.

        Args:
            raw_data: универсальные данные от load_reports_data

        Returns:
            Данные в формате для Google Sheets
        """
        # Извлекаем дату отчета
        report_date = raw_data.get('_report_date', '')

        # Преобразуем формат даты из YYYY-MM-DD в DD.MM.YYYY
        formatted_date = self._format_date(report_date)

        # Извлекаем данные о выдаче
        issued_packages = raw_data.get('issued_packages') or raw_data.get('total_packages') or 0

        # Извлекаем данные о прямом потоке
        direct_flow = self._extract_direct_flow(raw_data)

        # Извлекаем данные о возвратном потоке
        return_flow = self._extract_return_flow(raw_data)

        # Получаем информацию о ПВЗ
        pvz_info = raw_data.get('pvz_info') or raw_data.get('_pvz_id') or ''

        return {
            'id': '',  # Будет заполнен формулой в таблице
            'Дата': formatted_date,
            'ПВЗ': pvz_info,
            'Количество выдач': issued_packages,
            'Прямой поток': direct_flow,
            'Возвратный поток': return_flow
        }

    def _format_date(self, date_str: str) -> str:
        """
        Форматирует дату из YYYY-MM-DD в DD.MM.YYYY.

        Args:
            date_str: дата в формате YYYY-MM-DD

        Returns:
            Дата в формате DD.MM.YYYY или пустая строка
        """
        if not date_str:
            return ''

        try:
            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
            return date_obj.strftime('%d.%m.%Y')
        except ValueError:
            return date_str  # Возвращаем как есть если формат не распознан

    def _extract_direct_flow(self, raw_data: Dict[str, Any]) -> int:
        """
        Извлекает количество прямого потока.

        Args:
            raw_data: универсальные данные

        Returns:
            Количество прямого потока
        """
        # Пробуем разные варианты извлечения
        if 'direct_flow_count' in raw_data:
            return raw_data['direct_flow_count'] or 0

        if 'direct_flow_data' in raw_data and isinstance(raw_data['direct_flow_data'], dict):
            return raw_data['direct_flow_data'].get('total_items_count', 0)

        return 0

    def _extract_return_flow(self, raw_data: Dict[str, Any]) -> int:
        """
        Извлекает количество возвратного потока.

        Args:
            raw_data: универсальные данные

        Returns:
            Количество возвратного потока
        """
        if 'return_flow_data' in raw_data and isinstance(raw_data['return_flow_data'], dict):
            return raw_data['return_flow_data'].get('total_items_count', 0)

        return 0