"""
Lightweight parser для discovery доступных ПВЗ в Ozon по сохраненной browser session.
"""
__version__ = "0.0.1"

from datetime import datetime
from typing import Any, Dict

from ..core.ozon_report_parser import OzonReportParser


class OzonAvailablePvzParser(OzonReportParser):
    def get_report_type(self) -> str:
        return self.config.get("report_type", "ozon_available_pvz")

    def extract_report_data(self) -> Dict[str, Any]:
        return self.config.get("last_collected_data", {})

    def login(self) -> bool:
        if self.logger:
            self.logger.trace("Попали в метод OzonAvailablePvzParser.login")
            self.logger.info("Пропускаем авторизацию - используется сохраненная сессия")
        return True

    def logout(self) -> bool:
        if self.logger:
            self.logger.trace("Попали в метод OzonAvailablePvzParser.logout")
            self.logger.info("Пропускаем logout - явный выход для discovery не требуется")
        return True

    def run_discovery(self, save_to_file: bool = False, output_format: str = "json") -> Dict[str, Any]:
        if self.logger:
            self.logger.trace("Попали в метод OzonAvailablePvzParser.run_discovery")

        try:
            if not self.setup_browser():
                raise Exception("Не удалось настроить браузер")

            if not self.login():
                raise Exception("Не удалось выполнить вход в систему")

            if not self.navigate_to_target():
                raise Exception("Не удалось перейти на стартовую страницу для discovery ПВЗ")

            current_pvz = self.get_current_pvz()
            available_pvz = self.collect_available_pvz()

            result = {
                "success": True,
                "mode": "available_pvz_discovery",
                "configured_pvz_id": self.config.get("additional_params", {}).get("location_id", ""),
                "current_pvz": current_pvz,
                "available_pvz_count": len(available_pvz),
                "available_pvz": available_pvz,
                "source_url": self.driver.current_url if self.driver else "",
                "extraction_timestamp": datetime.now().strftime(
                    self.config.get("datetime_format", "%Y-%m-%d %H:%M:%S")
                ),
            }
            self.config["last_collected_data"] = result

            if save_to_file:
                self.save_report(data=result, output_format=output_format)

            return result
        except Exception as exc:
            if self.logger:
                self.logger.error(f"Ошибка при discovery доступных ПВЗ: {exc}")
            return {
                "success": False,
                "mode": "available_pvz_discovery",
                "configured_pvz_id": self.config.get("additional_params", {}).get("location_id", ""),
                "error": str(exc),
            }
        finally:
            try:
                self.logout()
            finally:
                self._close_parser_session()
