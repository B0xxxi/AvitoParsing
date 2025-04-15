import requests
import time
import random
import logging
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from tqdm import tqdm
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)

class AvitoParser:
    """Класс для парсинга объявлений с Avito"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Инициализация парсера Avito
        
        Args:
            config: Словарь с настройками парсера
        """
        self.config = config
        self.user_agent = UserAgent()
        self.session = requests.Session()
        self.timeout = int(config.get("REQUEST_TIMEOUT", 10))
        self.max_retries = int(config.get("MAX_RETRIES", 3))
        self.delay = int(config.get("DELAY_BETWEEN_REQUESTS", 2))
        self.rotate_ua = config.get("USER_AGENT_ROTATE", "true").lower() == "true"
    
    def _get_headers(self) -> Dict[str, str]:
        """
        Генерация заголовков для запроса
        
        Returns:
            Dict[str, str]: Заголовки для HTTP запроса
        """
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Cache-Control": "max-age=0",
        }
        
        if self.rotate_ua:
            headers["User-Agent"] = self.user_agent.random
        else:
            headers["User-Agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        
        return headers
    
    def _make_request(self, url: str) -> Optional[str]:
        """
        Выполнение HTTP запроса с повторными попытками
        
        Args:
            url: URL для запроса
            
        Returns:
            Optional[str]: HTML содержимое страницы или None в случае ошибки
        """
        for attempt in range(self.max_retries):
            try:
                response = self.session.get(
                    url, 
                    headers=self._get_headers(),
                    timeout=self.timeout
                )
                response.raise_for_status()
                return response.text
            except requests.RequestException as e:
                logger.warning(f"Попытка {attempt + 1}/{self.max_retries} не удалась: {str(e)}")
                if attempt < self.max_retries - 1:
                    sleep_time = self.delay * (attempt + 1) + random.uniform(0, 1)
                    logger.info(f"Ожидание {sleep_time:.2f} секунд перед повтором...")
                    time.sleep(sleep_time)
        
        logger.error(f"Не удалось получить данные с {url} после {self.max_retries} попыток")
        return None
    
    def _parse_item(self, item_element) -> Dict[str, Any]:
        """
        Извлечение данных из элемента объявления
        
        Args:
            item_element: Элемент BeautifulSoup с объявлением
            
        Returns:
            Dict[str, Any]: Извлеченные данные объявления
        """
        try:
            # Структура объявлений может меняться, поэтому используем try/except для каждого поля
            item_data = {
                'id': '',
                'title': '',
                'url': '',
                'price': 0,
                'date': '',
                'location': ''
            }
            
            # ID объявления
            item_id = item_element.get('data-item-id', '')
            if not item_id:
                item_id = item_element.get('id', '').replace('i', '')
            item_data['id'] = item_id
            
            # Заголовок объявления
            title_element = item_element.select_one('[itemprop="name"]') or item_element.select_one('.title-root') or item_element.select_one('[data-marker="item-title"]')
            item_data['title'] = title_element.text.strip() if title_element else "Нет заголовка"
            
            # Ссылка на объявление
            link_element = item_element.select_one('a[href^="/"]') or item_element.select_one('a[itemprop="url"]')
            if link_element and 'href' in link_element.attrs:
                item_data['url'] = f"https://www.avito.ru{link_element['href']}"
            else:
                item_data['url'] = ""
            
            # Цена
            price_element = (
                item_element.select_one('[data-marker="item-price"]') or 
                item_element.select_one('[itemprop="price"]') or 
                item_element.select_one('.styles-module-root-_KFFt') or
                item_element.select_one('.price-text') or
                item_element.select_one('[class*="price"]')
            )
            
            if price_element:
                price_text = price_element.text.strip()
                # Удаляем все нецифровые символы кроме точки
                price = ''.join(filter(lambda x: x.isdigit() or x == '.', price_text))
                try:
                    item_data['price'] = float(price) if price else 0
                except ValueError:
                    item_data['price'] = 0
            else:
                # Ищем цену в исходном HTML
                html_str = str(item_element)
                # Ищем ценовые паттерны
                price_patterns = [
                    r'data-price="(\d+)"',
                    r'price":"(\d+)"',
                    r'price":(\d+)',
                    r'price=(\d+)'
                ]
                for pattern in price_patterns:
                    import re
                    match = re.search(pattern, html_str)
                    if match:
                        try:
                            item_data['price'] = float(match.group(1))
                            break
                        except (ValueError, IndexError):
                            pass
                
                if item_data['price'] == 0:
                    logger.debug(f"Не удалось извлечь цену для объявления {item_data['id']}")
                    
            # Если цена все еще 0, попробуем поискать цену в тексте объявления
            if item_data['price'] == 0:
                full_text = item_element.get_text(strip=True)
                # Ищем числа с пробелами или без, с руб/₽ или без
                import re
                price_match = re.search(r'(\d[\d\s]*\d|\d+)[.,]?\d*\s*(руб|₽)?', full_text)
                if price_match:
                    price_str = price_match.group(1).replace(' ', '')
                    try:
                        item_data['price'] = float(price_str)
                    except ValueError:
                        pass
            
            # Дата публикации
            date_element = item_element.select_one('[data-marker="item-date"]')
            item_data['date'] = date_element.text.strip() if date_element else ""
            
            # Расположение - ищем настоящий город/адрес
            location_element = item_element.select_one('[data-marker="item-location"]') or item_element.select_one('[class*="geo"]')
            if location_element:
                location_text = location_element.text.strip()
                # Фильтруем строки доставки
                if "доставка" in location_text.lower():
                    # Ищем другие элементы локации
                    alt_location = item_element.select_one('[class*="location"]')
                    if alt_location and "доставка" not in alt_location.text.lower():
                        item_data['location'] = alt_location.text.strip()
                    else:
                        item_data['location'] = "Не указано"
                else:
                    item_data['location'] = location_text
            else:
                item_data['location'] = "Не указано"
            
            return item_data
        except Exception as e:
            logger.error(f"Ошибка при извлечении данных объявления: {str(e)}")
            return {
                'id': '',
                'title': '',
                'url': '',
                'price': 0,
                'date': '',
                'location': '',
                'error': str(e)
            }
    
    def parse_search_page(self, url: str, limit: int = 100, local_first: bool = True) -> List[Dict[str, Any]]:
        """
        Парсинг страницы поиска Avito
        
        Args:
            url: URL страницы поиска
            limit: Максимальное количество объявлений для парсинга
            local_first: Показать сначала в выбранном регионе
            
        Returns:
            List[Dict[str, Any]]: Список с данными объявлений
        """
        # Добавляем параметр для показа сначала в выбранном регионе
        if local_first and "?" in url and "&localPriority=1" not in url:
            url += "&localPriority=1"
        elif local_first and "?" not in url:
            url += "?localPriority=1"
        
        all_items = []
        page_num = 1
        items_count = 0
        
        logger.info(f"Начинаем парсинг объявлений с URL: {url}")
        
        while items_count < limit:
            page_url = f"{url}&p={page_num}" if "?" in url else f"{url}?p={page_num}"
            logger.info(f"Парсинг страницы {page_num}: {page_url}")
            
            html_content = self._make_request(page_url)
            if not html_content:
                logger.error(f"Не удалось получить контент страницы {page_url}")
                break
            
            soup = BeautifulSoup(html_content, 'lxml')
            
            # Поиск блоков с объявлениями
            items = soup.select('[data-marker="item"]')
            
            if not items:
                logger.warning(f"На странице {page_url} не найдено объявлений")
                break
            
            for item in tqdm(items, desc=f"Парсинг объявлений на странице {page_num}"):
                if items_count >= limit:
                    break
                
                item_data = self._parse_item(item)
                all_items.append(item_data)
                items_count += 1
            
            if items_count >= limit:
                logger.info(f"Достигнут лимит в {limit} объявлений")
                break
            
            # Если на странице меньше элементов, чем ожидалось, значит это последняя страница
            if len(items) < 50:  # Обычно на странице 50 объявлений
                logger.info(f"Последняя страница {page_num} с {len(items)} объявлениями")
                break
            
            page_num += 1
            # Задержка между запросами страниц
            time.sleep(self.delay + random.uniform(0, 2))
        
        logger.info(f"Парсинг завершен. Получено {len(all_items)} объявлений")
        return all_items 