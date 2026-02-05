"""
Пример использования изолированного микросервиса уведомлений

Этот пример демонстрирует, как использовать микросервис уведомлений
с передачей всех необходимых параметров извне.
"""
__version__ = '1.0.0'

import sys
from pathlib import Path

# Добавляем корень проекта в путь Python
project_root = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(project_root))

from scheduler_runner.utils.notifications import send_notification, test_connection


def example_usage():
    """
    Пример использования изолированного микросервиса уведомлений
    """
    print("=== Пример использования изолированного микросервиса уведомлений ===")
    
    # Параметры подключения (должны быть получены из внешней конфигурации)
    connection_params = {
        "TELEGRAM_BOT_TOKEN": "YOUR_TELEGRAM_BOT_TOKEN_HERE",
        "TELEGRAM_CHAT_ID": "YOUR_TELEGRAM_CHAT_ID_HERE",
    }
    
    # Проверяем подключение
    print("Проверка подключения...")
    connection_result = test_connection(connection_params)
    print(f"Результат проверки подключения: {connection_result}")
    
    if not connection_result["success"]:
        print("Не удалось подключиться. Проверьте параметры подключения.")
        return
    
    # Подготовим сообщение для отправки
    message = "Привет! Это тестовое сообщение от изолированного микросервиса уведомлений."
    
    # Отправляем уведомление
    print("Отправка уведомления...")
    result = send_notification(
        message=message,
        connection_params=connection_params
    )
    
    print(f"Результат отправки: {result}")
    

def example_with_templates():
    """
    Пример использования с шаблонами
    """
    print("\n=== Пример использования с шаблонами ===")
    
    # Параметры подключения
    connection_params = {
        "TELEGRAM_BOT_TOKEN": "YOUR_TELEGRAM_BOT_TOKEN_HERE",
        "TELEGRAM_CHAT_ID": "YOUR_TELEGRAM_CHAT_ID_HERE",
    }
    
    # Шаблоны сообщений
    templates = {
        "welcome": "Добро пожаловать, {name}! Ваш ID: {user_id}",
        "notification": "Уведомление: {title}\n{body}"
    }
    
    # Подготовим сообщение с использованием шаблона
    message = {
        "template": "welcome",
        "data": {
            "name": "Иван",
            "user_id": "12345"
        }
    }
    
    # Отправляем уведомление с шаблоном
    result = send_notification(
        message=message,
        connection_params=connection_params,
        templates=templates
    )
    
    print(f"Результат отправки с шаблоном: {result}")
    

def example_with_external_logger():
    """
    Пример использования с внешним логгером
    """
    print("\n=== Пример использования с внешним логгером ===")
    
    # Импортируем логгер из основной системы
    from scheduler_runner.utils.logging import configure_logger
    logger = configure_logger(user="external_user", task_name="ExternalNotificationTask")
    
    # Параметры подключения
    connection_params = {
        "TELEGRAM_BOT_TOKEN": "YOUR_TELEGRAM_BOT_TOKEN_HERE",
        "TELEGRAM_CHAT_ID": "YOUR_TELEGRAM_CHAT_ID_HERE",
    }
    
    # Подготовим сообщение
    message = "Сообщение, отправленное с использованием внешнего логгера"
    
    # Отправляем уведомление с внешним логгером
    result = send_notification(
        message=message,
        connection_params=connection_params,
        logger=logger
    )
    
    print(f"Результат отправки с внешним логгером: {result}")


if __name__ == "__main__":
    example_usage()
    example_with_templates()
    example_with_external_logger()