import os
import csv
import json
import logging
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any, Optional
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)

class DataStorage(ABC):
    """Абстрактный класс для хранения данных"""
    
    @abstractmethod
    def save_data(self, data: List[Dict[str, Any]]) -> bool:
        """
        Сохранить данные в хранилище
        
        Args:
            data: Список словарей с данными объявлений
            
        Returns:
            bool: Успешность сохранения
        """
        pass
    
    @abstractmethod
    def load_data(self, **kwargs) -> List[Dict[str, Any]]:
        """
        Загрузить данные из хранилища
        
        Returns:
            List[Dict[str, Any]]: Список словарей с данными объявлений
        """
        pass


class CSVStorage(DataStorage):
    """Класс для хранения данных в CSV файлах"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Инициализация хранилища CSV
        
        Args:
            config: Словарь с настройками
        """
        self.config = config
        self.directory = config.get("CSV_DIRECTORY", "./data")
        self.filename_template = config.get("CSV_FILENAME_TEMPLATE", "avito_data_{date}.csv")
        
        # Создаем директорию, если она не существует
        if not os.path.exists(self.directory):
            os.makedirs(self.directory)
    
    def _get_filename(self, custom_name: Optional[str] = None) -> str:
        """
        Получить имя файла для сохранения данных
        
        Args:
            custom_name: Пользовательское имя файла
            
        Returns:
            str: Путь к файлу
        """
        if custom_name:
            return os.path.join(self.directory, custom_name)
        
        date_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        filename = self.filename_template.format(date=date_str)
        return os.path.join(self.directory, filename)
    
    def save_data(self, data: List[Dict[str, Any]], filename: Optional[str] = None) -> bool:
        """
        Сохранить данные в CSV файл
        
        Args:
            data: Список словарей с данными объявлений
            filename: Имя файла для сохранения
            
        Returns:
            bool: Успешность сохранения
        """
        if not data:
            logger.warning("Нет данных для сохранения в CSV")
            return False
        
        filepath = self._get_filename(filename)
        
        try:
            # Определяем обязательные поля и их порядок
            required_fields = ['id', 'title', 'price', 'date', 'location', 'url']
            
            # Нормализуем данные
            normalized_data = []
            for item in data:
                normalized_item = {}
                for field in required_fields:
                    if field in item:
                        # Преобразуем числовые значения в строки с запятой
                        if field == 'price' and isinstance(item[field], (int, float)):
                            normalized_item[field] = str(item[field]).replace('.', ',')
                        else:
                            normalized_item[field] = item[field]
                    else:
                        if field == 'price':
                            normalized_item[field] = "0"
                        else:
                            normalized_item[field] = ""
                normalized_data.append(normalized_item)
            
            # Сохраняем данные с помощью модуля csv
            with open(filepath, 'w', newline='', encoding='utf-8-sig') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=required_fields)
                writer.writeheader()
                writer.writerows(normalized_data)
            
            # Проверим, что файл создан
            if not os.path.exists(filepath):
                logger.error(f"Файл {filepath} не был создан")
                return False
                
            file_size = os.path.getsize(filepath)
            logger.info(f"Данные успешно сохранены в {filepath} (размер: {file_size} байт)")
            return True
        except Exception as e:
            logger.error(f"Ошибка при сохранении данных в CSV: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return False
    
    def load_data(self, filename: Optional[str] = None, **kwargs) -> List[Dict[str, Any]]:
        """
        Загрузить данные из CSV файла
        
        Args:
            filename: Имя файла для загрузки
            
        Returns:
            List[Dict[str, Any]]: Список словарей с данными объявлений
        """
        if not filename:
            # Если имя файла не указано, берем самый последний файл
            files = [f for f in os.listdir(self.directory) if f.endswith(".csv")]
            if not files:
                logger.warning(f"В директории {self.directory} не найдено CSV файлов")
                return []
            
            files.sort(reverse=True)  # Сортируем по убыванию, чтобы самый свежий был первым
            filename = files[0]
        
        filepath = os.path.join(self.directory, filename)
        
        try:
            if not os.path.exists(filepath):
                logger.error(f"Файл {filepath} не существует")
                return []
            
            # Загружаем данные из CSV
            df = pd.read_csv(filepath, encoding='utf-8-sig')
            
            # Преобразуем DataFrame в список словарей
            data = df.to_dict('records')
            
            logger.info(f"Данные успешно загружены из {filepath}")
            return data
        except Exception as e:
            logger.error(f"Ошибка при загрузке данных из CSV: {str(e)}")
            return []


class PostgresStorage(DataStorage):
    """Класс для хранения данных в PostgreSQL"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Инициализация хранилища PostgreSQL
        
        Args:
            config: Словарь с настройками
        """
        self.config = config
        self.host = config.get("DB_HOST", "localhost")
        self.port = config.get("DB_PORT", 5432)
        self.dbname = config.get("DB_NAME", "avito_data")
        self.user = config.get("DB_USER", "postgres")
        self.password = config.get("DB_PASSWORD", "")
        self.table_name = "avito_items"
    
    def _get_connection(self):
        """
        Получить соединение с базой данных
        
        Returns:
            Connection: Объект соединения с базой данных
        """
        try:
            import psycopg2
            conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                dbname=self.dbname,
                user=self.user,
                password=self.password
            )
            return conn
        except ImportError:
            logger.error("Модуль psycopg2 не установлен. Установите его с помощью: pip install psycopg2-binary")
            return None
        except Exception as e:
            logger.error(f"Ошибка подключения к PostgreSQL: {str(e)}")
            return None
    
    def _create_table_if_not_exists(self, conn):
        """
        Создать таблицу, если она не существует
        
        Args:
            conn: Соединение с базой данных
        """
        try:
            cursor = conn.cursor()
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.table_name} (
                    id VARCHAR(50) PRIMARY KEY,
                    title TEXT NOT NULL,
                    url TEXT,
                    price FLOAT,
                    date VARCHAR(100),
                    location TEXT,
                    parse_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()
            cursor.close()
        except Exception as e:
            logger.error(f"Ошибка при создании таблицы: {str(e)}")
            conn.rollback()
    
    def save_data(self, data: List[Dict[str, Any]]) -> bool:
        """
        Сохранить данные в PostgreSQL
        
        Args:
            data: Список словарей с данными объявлений
            
        Returns:
            bool: Успешность сохранения
        """
        if not data:
            logger.warning("Нет данных для сохранения в PostgreSQL")
            return False
        
        conn = self._get_connection()
        if not conn:
            return False
        
        try:
            self._create_table_if_not_exists(conn)
            
            cursor = conn.cursor()
            
            # Подготовка данных и вставка
            for item in data:
                # Вставка с ON CONFLICT DO UPDATE для обновления существующих записей
                cursor.execute(f"""
                    INSERT INTO {self.table_name} (id, title, url, price, date, location)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        title = EXCLUDED.title,
                        url = EXCLUDED.url,
                        price = EXCLUDED.price,
                        date = EXCLUDED.date,
                        location = EXCLUDED.location,
                        parse_date = CURRENT_TIMESTAMP
                """, (
                    item.get('id', ''),
                    item.get('title', ''),
                    item.get('url', ''),
                    item.get('price', 0),
                    item.get('date', ''),
                    item.get('location', '')
                ))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"Данные успешно сохранены в PostgreSQL, таблица {self.table_name}")
            return True
        except Exception as e:
            logger.error(f"Ошибка при сохранении данных в PostgreSQL: {str(e)}")
            if conn:
                conn.rollback()
                conn.close()
            return False
    
    def load_data(self, limit: int = 1000, **kwargs) -> List[Dict[str, Any]]:
        """
        Загрузить данные из PostgreSQL
        
        Args:
            limit: Максимальное количество записей для загрузки
            
        Returns:
            List[Dict[str, Any]]: Список словарей с данными объявлений
        """
        conn = self._get_connection()
        if not conn:
            return []
        
        try:
            cursor = conn.cursor()
            
            # Проверяем существование таблицы
            cursor.execute(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = '{self.table_name}'
                )
            """)
            table_exists = cursor.fetchone()[0]
            
            if not table_exists:
                logger.warning(f"Таблица {self.table_name} не существует")
                cursor.close()
                conn.close()
                return []
            
            # Загружаем данные
            cursor.execute(f"""
                SELECT id, title, url, price, date, location, parse_date
                FROM {self.table_name}
                ORDER BY parse_date DESC
                LIMIT {limit}
            """)
            
            rows = cursor.fetchall()
            
            # Преобразуем результат в список словарей
            columns = ['id', 'title', 'url', 'price', 'date', 'location', 'parse_date']
            result = []
            
            for row in rows:
                item = {}
                for i, col in enumerate(columns):
                    item[col] = row[i]
                result.append(item)
            
            cursor.close()
            conn.close()
            
            logger.info(f"Данные успешно загружены из PostgreSQL, таблица {self.table_name}")
            return result
        except Exception as e:
            logger.error(f"Ошибка при загрузке данных из PostgreSQL: {str(e)}")
            if conn:
                conn.close()
            return []


class StorageFactory:
    """Фабрика для создания хранилищ данных"""
    
    @staticmethod
    def get_storage(storage_type: str, config: Dict[str, Any]) -> DataStorage:
        """
        Получить объект хранилища данных
        
        Args:
            storage_type: Тип хранилища ('csv', 'postgres')
            config: Словарь с настройками
            
        Returns:
            DataStorage: Объект хранилища данных
        """
        if storage_type.lower() == 'csv':
            return CSVStorage(config)
        elif storage_type.lower() == 'postgres':
            return PostgresStorage(config)
        else:
            logger.warning(f"Неизвестный тип хранилища: {storage_type}, используем CSV по умолчанию")
            return CSVStorage(config) 