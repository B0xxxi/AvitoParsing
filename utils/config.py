import os
import logging
from typing import Dict, Any
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

def load_config(config_path: str = None) -> Dict[str, Any]:
    """
    Загрузка конфигурации из .env файла
    
    Args:
        config_path: Путь к файлу конфигурации
        
    Returns:
        Dict[str, Any]: Словарь с настройками
    """
    # Если путь не указан, пробуем загрузить из стандартных мест
    if not config_path:
        # Пробуем сначала загрузить из config.env, затем из .env
        if os.path.exists('config.env'):
            config_path = 'config.env'
        elif os.path.exists('.env'):
            config_path = '.env'
        else:
            logger.warning("Файл конфигурации не найден, используются значения по умолчанию")
    
    # Загружаем переменные окружения из .env файла
    if config_path and os.path.exists(config_path):
        logger.info(f"Загрузка конфигурации из {config_path}")
        load_dotenv(config_path)
    else:
        logger.warning(f"Файл конфигурации {config_path} не найден")
    
    # Собираем все переменные окружения в словарь
    config = {}
    for key, value in os.environ.items():
        config[key] = value
    
    return config 