import os
import logging
import sys
from datetime import datetime
from typing import Optional

def setup_logger(log_level: str = "INFO", log_file: Optional[str] = None) -> logging.Logger:
    """
    Настройка логирования
    
    Args:
        log_level: Уровень логирования (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Путь к файлу для записи логов
        
    Returns:
        logging.Logger: Настроенный объект логирования
    """
    # Создаем директорию для логов если она не существует
    if log_file:
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)
    
    # Устанавливаем уровень логирования
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        print(f"Неверный уровень логирования: {log_level}")
        numeric_level = logging.INFO
    
    # Формат сообщений лога
    log_format = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"
    
    # Настройка обработчиков
    handlers = []
    
    # Консольный обработчик
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter(log_format, date_format))
    handlers.append(console_handler)
    
    # Файловый обработчик
    if log_file:
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(logging.Formatter(log_format, date_format))
        handlers.append(file_handler)
    
    # Настройка корневого логгера
    logging.basicConfig(
        level=numeric_level,
        format=log_format,
        datefmt=date_format,
        handlers=handlers
    )
    
    # Создаем и возвращаем логгер
    logger = logging.getLogger('avito_parser')
    logger.setLevel(numeric_level)
    
    return logger 