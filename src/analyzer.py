import pandas as pd
import numpy as np
import logging
from typing import List, Dict, Any, Optional, Tuple

logger = logging.getLogger(__name__)

class AvitoAnalyzer:
    """Класс для анализа данных объявлений Avito"""
    
    def __init__(self):
        """Инициализация анализатора данных"""
        pass
    
    @staticmethod
    def get_price_statistics(data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Анализ цен объявлений и расчет статистики
        
        Args:
            data: Список словарей с данными объявлений
            
        Returns:
            Dict[str, Any]: Словарь со статистикой цен
        """
        if not data:
            logger.warning("Нет данных для анализа цен")
            return {
                "count": 0,
                "avg_price": 0,
                "median_price": 0,
                "min_price": 0,
                "max_price": 0,
                "std_price": 0
            }
        
        # Извлекаем цены из данных
        prices = [item.get('price', 0) for item in data]
        
        # Очищаем от нулевых значений для статистики
        non_zero_prices = [p for p in prices if p > 0]
        
        if not non_zero_prices:
            logger.warning("Все цены равны нулю, невозможно рассчитать статистику")
            return {
                "count": len(data),
                "avg_price": 0,
                "median_price": 0,
                "min_price": 0,
                "max_price": 0,
                "std_price": 0,
                "zero_prices_count": len(data)
            }
        
        # Рассчитываем статистику
        count = len(data)
        avg_price = np.mean(non_zero_prices)
        median_price = np.median(non_zero_prices)
        min_price = min(non_zero_prices)
        max_price = max(non_zero_prices)
        std_price = np.std(non_zero_prices)
        zero_prices_count = len(prices) - len(non_zero_prices)
        
        return {
            "count": count,
            "avg_price": avg_price,
            "median_price": median_price,
            "min_price": min_price,
            "max_price": max_price,
            "std_price": std_price,
            "zero_prices_count": zero_prices_count
        }
    
    @staticmethod
    def get_location_distribution(data: List[Dict[str, Any]], limit: int = 10) -> Dict[str, int]:
        """
        Анализ распределения объявлений по локациям
        
        Args:
            data: Список словарей с данными объявлений
            limit: Максимальное количество локаций в результате
            
        Returns:
            Dict[str, int]: Словарь с количеством объявлений по локациям
        """
        if not data:
            logger.warning("Нет данных для анализа локаций")
            return {}
        
        # Извлекаем локации из данных
        locations = [item.get('location', '').strip() for item in data]
        
        # Считаем количество объявлений по локациям
        location_counts = {}
        for loc in locations:
            if not loc:
                continue
            location_counts[loc] = location_counts.get(loc, 0) + 1
        
        # Сортируем по убыванию частоты и ограничиваем количество
        sorted_locations = sorted(location_counts.items(), key=lambda x: x[1], reverse=True)
        top_locations = dict(sorted_locations[:limit])
        
        return top_locations
    
    @staticmethod
    def find_outliers(data: List[Dict[str, Any]], z_threshold: float = 2.0) -> List[Dict[str, Any]]:
        """
        Поиск выбросов по цене (объявления с аномально высокой или низкой ценой)
        
        Args:
            data: Список словарей с данными объявлений
            z_threshold: Z-score порог для определения выбросов
            
        Returns:
            List[Dict[str, Any]]: Список объявлений-выбросов
        """
        if not data:
            logger.warning("Нет данных для поиска выбросов")
            return []
        
        # Создаем DataFrame из данных
        df = pd.DataFrame(data)
        
        # Оставляем только записи с ненулевыми ценами
        df_non_zero = df[df['price'] > 0].copy()
        
        if df_non_zero.empty:
            logger.warning("Нет ненулевых цен для анализа выбросов")
            return []
        
        # Рассчитываем Z-score для цен
        mean_price = df_non_zero['price'].mean()
        std_price = df_non_zero['price'].std()
        
        if std_price == 0:
            logger.warning("Стандартное отклонение цен равно нулю, невозможно найти выбросы")
            return []
        
        df_non_zero['z_score'] = (df_non_zero['price'] - mean_price) / std_price
        
        # Находим выбросы
        outliers_df = df_non_zero[abs(df_non_zero['z_score']) > z_threshold]
        
        # Преобразуем обратно в список словарей
        outliers = outliers_df.drop(columns=['z_score']).to_dict('records')
        
        return outliers
    
    @staticmethod
    def compare_with_previous(current_data: List[Dict[str, Any]], previous_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Сравнение текущих данных с предыдущими
        
        Args:
            current_data: Текущие данные объявлений
            previous_data: Предыдущие данные объявлений
            
        Returns:
            Dict[str, Any]: Результаты сравнения
        """
        if not current_data or not previous_data:
            logger.warning("Недостаточно данных для сравнения")
            return {"comparison_available": False}
        
        # Получаем статистику для обоих наборов данных
        current_stats = AvitoAnalyzer.get_price_statistics(current_data)
        previous_stats = AvitoAnalyzer.get_price_statistics(previous_data)
        
        # Рассчитываем изменения
        avg_price_change = current_stats["avg_price"] - previous_stats["avg_price"]
        avg_price_change_percent = (avg_price_change / previous_stats["avg_price"] * 100) if previous_stats["avg_price"] > 0 else 0
        
        median_price_change = current_stats["median_price"] - previous_stats["median_price"]
        median_price_change_percent = (median_price_change / previous_stats["median_price"] * 100) if previous_stats["median_price"] > 0 else 0
        
        # Формируем результаты сравнения
        comparison = {
            "comparison_available": True,
            "current_count": current_stats["count"],
            "previous_count": previous_stats["count"],
            "count_change": current_stats["count"] - previous_stats["count"],
            "avg_price_change": avg_price_change,
            "avg_price_change_percent": avg_price_change_percent,
            "median_price_change": median_price_change,
            "median_price_change_percent": median_price_change_percent,
            "max_price_change": current_stats["max_price"] - previous_stats["max_price"],
            "min_price_change": current_stats["min_price"] - previous_stats["min_price"]
        }
        
        return comparison
    
    @staticmethod
    def generate_summary(data: List[Dict[str, Any]], previous_data: Optional[List[Dict[str, Any]]] = None) -> str:
        """
        Генерация текстовой сводки анализа данных
        
        Args:
            data: Список словарей с данными объявлений
            previous_data: Предыдущие данные для сравнения
            
        Returns:
            str: Текстовая сводка с результатами анализа
        """
        if not data:
            return "Нет данных для анализа"
        
        # Получаем статистику цен
        stats = AvitoAnalyzer.get_price_statistics(data)
        
        # Формируем сводку
        summary = []
        summary.append("=== СВОДКА АНАЛИЗА ДАННЫХ AVITO ===")
        summary.append(f"Всего объявлений: {stats['count']}")
        summary.append(f"Средняя цена: {stats['avg_price']:.2f} руб.")
        summary.append(f"Медианная цена: {stats['median_price']:.2f} руб.")
        summary.append(f"Минимальная цена: {stats['min_price']:.2f} руб.")
        summary.append(f"Максимальная цена: {stats['max_price']:.2f} руб.")
        summary.append(f"Стандартное отклонение: {stats['std_price']:.2f} руб.")
        
        if stats.get('zero_prices_count', 0) > 0:
            summary.append(f"Объявления без цены: {stats['zero_prices_count']}")
        
        # Добавляем распределение по локациям
        locations = AvitoAnalyzer.get_location_distribution(data, limit=5)
        if locations:
            summary.append("\nРаспределение по локациям (топ-5):")
            for loc, count in locations.items():
                summary.append(f"- {loc}: {count} объявл.")
        
        # Добавляем сравнение с предыдущими данными
        if previous_data:
            comparison = AvitoAnalyzer.compare_with_previous(data, previous_data)
            if comparison.get("comparison_available", False):
                summary.append("\nСравнение с предыдущими данными:")
                summary.append(f"Изменение количества объявлений: {comparison['count_change']}")
                summary.append(f"Изменение средней цены: {comparison['avg_price_change']:.2f} руб. ({comparison['avg_price_change_percent']:.2f}%)")
                summary.append(f"Изменение медианной цены: {comparison['median_price_change']:.2f} руб. ({comparison['median_price_change_percent']:.2f}%)")
        
        # Добавляем выбросы цен
        outliers = AvitoAnalyzer.find_outliers(data)
        if outliers:
            summary.append(f"\nНайдено {len(outliers)} ценовых аномалий:")
            for i, outlier in enumerate(outliers[:3], 1):  # Показываем только первые 3
                summary.append(f"{i}. {outlier.get('title', '')} - {outlier.get('price', 0):.2f} руб.")
            
            if len(outliers) > 3:
                summary.append(f"... и еще {len(outliers) - 3}")
        
        return "\n".join(summary) 