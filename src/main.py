#!/usr/bin/env python3
import os
import sys
import argparse
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional

# Добавляем корневую директорию проекта в PYTHONPATH
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.parser import AvitoParser
from src.storage import StorageFactory
from src.analyzer import AvitoAnalyzer
from utils.config import load_config
from utils.logger import setup_logger

def parse_arguments():
    """
    Парсинг аргументов командной строки
    
    Returns:
        argparse.Namespace: Объект с аргументами командной строки
    """
    parser = argparse.ArgumentParser(description='Монитор-анализатор цен Avito')
    
    parser.add_argument('--url', type=str, help='URL страницы поиска Avito')
    parser.add_argument('--output', type=str, choices=['csv', 'postgres'], default='csv',
                      help='Формат вывода данных (csv, postgres)')
    parser.add_argument('--limit', type=int, default=100,
                      help='Максимальное количество объявлений для парсинга')
    parser.add_argument('--config', type=str, default=None,
                      help='Путь к файлу конфигурации')
    parser.add_argument('--log-level', type=str, default='INFO',
                      choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                      help='Уровень логирования')
    parser.add_argument('--compare', type=str, default=None,
                      help='Имя CSV файла для сравнения (применимо только для output=csv)')
    
    return parser.parse_args()

def main():
    """
    Основная функция программы
    """
    # Парсинг аргументов командной строки
    args = parse_arguments()
    
    # Настройка логирования
    log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs')
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    log_file = os.path.join(log_dir, f'avito_parser_{datetime.now().strftime("%Y-%m-%d")}.log')
    logger = setup_logger(args.log_level, log_file)
    
    logger.info(f"Запуск монитора-анализатора цен Avito")
    
    # Проверка наличия URL
    if not args.url:
        logger.error("URL не указан. Используйте --url для указания ссылки на страницу поиска Avito")
        print("Ошибка: URL не указан. Используйте --url для указания ссылки на страницу поиска Avito")
        return 1
    
    # Загрузка конфигурации
    config = load_config(args.config)
    
    # Создание парсера
    parser = AvitoParser(config)
    
    # Парсинг данных
    logger.info(f"Начало парсинга данных с URL: {args.url}")
    data = parser.parse_search_page(args.url, limit=args.limit, local_first=True)
    
    if not data:
        logger.error("Не удалось получить данные объявлений")
        print("Ошибка: Не удалось получить данные объявлений")
        return 1
    
    # Анализ данных
    analyzer = AvitoAnalyzer()
    
    # Проверим и нормализуем формат данных перед сохранением
    for item in data:
        # Убедимся, что все необходимые поля присутствуют
        required_fields = ['id', 'title', 'url', 'price', 'date', 'location']
        for field in required_fields:
            if field not in item:
                logger.warning(f"Отсутствует поле {field} в данных, добавляем значение по умолчанию")
                if field == 'price':
                    item[field] = 0
                else:
                    item[field] = ""
        
        # Преобразуем числовые поля
        try:
            item['price'] = float(item['price']) if item['price'] else 0
        except (ValueError, TypeError):
            item['price'] = 0
    
    # Создание хранилища данных
    storage = StorageFactory.get_storage(args.output, config)
    
    # Загрузка предыдущих данных для сравнения
    previous_data = None
    if args.compare and args.output.lower() == 'csv':
        logger.info(f"Загрузка предыдущих данных из {args.compare} для сравнения")
        csv_storage = StorageFactory.get_storage('csv', config)
        previous_data = csv_storage.load_data(filename=args.compare)
    
    # Генерация сводки
    summary = analyzer.generate_summary(data, previous_data)
    
    # Вывод сводки
    print("\n" + summary + "\n")
    
    # Сохранение данных
    logger.info(f"Сохранение данных в формате {args.output}")
    
    # Проверим количество элементов с ценами
    items_with_price = sum(1 for item in data if item.get('price', 0) > 0)
    logger.info(f"Количество объявлений с ценами: {items_with_price} из {len(data)}")
    
    # Проверим формат первого элемента для отладки
    if data:
        logger.info(f"Пример данных для сохранения: {data[0]}")
    
    success = storage.save_data(data)
    
    if not success:
        logger.error("Не удалось сохранить данные")
        print("Ошибка: Не удалось сохранить данные")
        return 1
    
    logger.info("Программа успешно завершена")
    return 0

if __name__ == "__main__":
    sys.exit(main()) 