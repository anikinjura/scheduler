"""
reports_list.py

Определяет специфичные для задачи reports настройки ПВЗ, зависящие от PVZ_ID.

Используется для хранения информации о доступных ПВЗ и их специфичных настройках.

Пример использования:
    from .reports_list import CURRENT_PVZ_SETTINGS
    pvz_settings = CURRENT_PVZ_SETTINGS

Структура CURRENT_PVZ_SETTINGS:
    {
        'name': str,              # Название ПВЗ (совпадает с PVZ_ID)
        'ozon_pvz_code': str,     # Код ПВЗ в системе ОЗОН
        'wildberries_pvz_code': str, # Код ПВЗ в системе Wildberries (если применимо)
        'yandex_market_pvz_code': str, # Код ПВЗ в системе Яндекс Маркет (если применимо)
    }

Author: anikinjura
"""
__version__ = '1.0.0'

from config.base_config import PVZ_ID

# Для гибкости, используем PVZ_ID напрямую как наименование ПВЗ
# Если в будущем понадобятся специфичные настройки для разных маркетплейсов,
# можно будет расширить эту структуру
CURRENT_PVZ_SETTINGS = {
    'name': PVZ_ID,  # Используем значение из PVZ_ID как имя
    'ozon_pvz_code': PVZ_ID,  # Для ОЗОН используем PVZ_ID как код ПВЗ
    'wildberries_pvz_code': f'WB_{PVZ_ID.split("_")[-1]}' if '_' in PVZ_ID else f'WB_{PVZ_ID}',  # Для Wildberries формируем код
    'yandex_market_pvz_code': f'YM_{PVZ_ID.split("_")[-1]}' if '_' in PVZ_ID else f'YM_{PVZ_ID}',  # Для Яндекс Маркета формируем код
    'expected_ozon_pvz': PVZ_ID,  # Ожидаемый ПВЗ в ОЗОН (может отличаться от PVZ_ID, если нужно)
}