import argparse
import json

from config.base_config import PVZ_ID
from scheduler_runner.utils.parser import create_parser_logger, invoke_available_pvz_discovery


def parse_args():
    parser = argparse.ArgumentParser(description="Manual smoke для discovery доступных ПВЗ Ozon")
    parser.add_argument("--pvz", default=PVZ_ID, help="Базовый ПВЗ из локальной конфигурации")
    parser.add_argument("--pretty", action="store_true", help="Печатать JSON с отступами")
    parser.add_argument(
        "--save_to_file",
        action="store_true",
        help="Сохранять результат discovery в reports/ozon_available_pvz",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    logger = create_parser_logger()
    result = invoke_available_pvz_discovery(
        pvz_id=args.pvz,
        logger=logger,
        save_to_file=args.save_to_file,
        output_format="json",
    )
    print(json.dumps(result, ensure_ascii=False, indent=2 if args.pretty else None))
    return 0 if result.get("success") else 1


if __name__ == "__main__":
    raise SystemExit(main())
