"""
Конфигурация lightweight discovery parser для сбора доступных ПВЗ Ozon.
"""
__version__ = "0.0.1"

from ..base_configs.ozon_report_config import OZON_BASE_CONFIG


OZON_AVAILABLE_PVZ_CONFIG = {
    **OZON_BASE_CONFIG,
    "report_type": "ozon_available_pvz",
    "base_url": "https://turbo-pvz.ozon.ru/orders",
    "target_url": "https://turbo-pvz.ozon.ru/orders",
    "output_config": {
        **OZON_BASE_CONFIG.get("output_config", {}),
        "dir": "./reports/ozon_available_pvz",
    },
    "PAGE_LOAD_DELAY": 3,
    "BROWSER_CLOSE_DELAY": 2,
}
