# statistics.py
import logging
import json
import requests
from datetime import datetime, timedelta
import os

logger = logging.getLogger(__name__)

def get_user_stats(user_id, users_data):
    """Получает статистику для конкретного пользователя"""
    try:
        if users_data is None:
            logger.error("Не удалось получить данные пользователей для статистики")
            return None

        for user in users_data:
            if isinstance(user, dict) and user.get('user_id') == user_id:
                return user

        return None
    except Exception as e:
        logger.error(f"Ошибка при получении статистики пользователя: {e}")
        return None

def get_global_stats(users_data):
    """Получает общую статистику всех пользователей"""
    try:
        if users_data is None:
            logger.error("Не удалось получить данные пользователей для общей статистики")
            return None

        total_users = len(users_data)
        ru_users = 0
        en_users = 0

        for user in users_data:
            if isinstance(user, dict):
                if user.get('language') == 'RU':
                    ru_users += 1
                elif user.get('language') == 'EN':
                    en_users += 1

        return {
            "total_users": total_users,
            "ru_users": ru_users,
            "en_users": en_users
        }
    except Exception as e:
        logger.error(f"Ошибка при получении общей статистики: {e}")
        return None

def get_activity_stats(users_data):
    """Получает статистику пользователей по времени активности и регистрации"""
    try:
        if users_data is None:
            logger.error("Не удалось получить данные пользователей для статистики активности")
            return None

        current_time = datetime.now()

        # Для статистики по времени последнего захода
        active_today = 0
        active_week = 0
        active_month = 0
        active_more = 0

        # Для статистики по времени регистрации
        joined_today = 0
        joined_week = 0
        joined_month = 0
        joined_more = 0

        # Словари для хранения количества пользователей по месяцам их активности/регистрации
        months_activity = {}
        months_registration = {}
        
        # Для статистики по странам за разные периоды
        countries_24h = {"RU": 0, "EN": 0}
        countries_week = {"RU": 0, "EN": 0}
        countries_month = {"RU": 0, "EN": 0}

        for user in users_data:
            if isinstance(user, dict):
                user_language = user.get('language', 'unknown')
                
                # Анализ времени последнего захода
                if 'last_access' in user:
                    try:
                        last_access = datetime.strptime(user['last_access'], "%Y-%m-%d %H:%M:%S")
                        days_diff = (current_time - last_access).days
                        hours_diff = (current_time - last_access).total_seconds() / 3600

                        # Группировка по месяцам
                        month_key = last_access.strftime("%Y-%m")
                        if month_key not in months_activity:
                            months_activity[month_key] = 0
                        months_activity[month_key] += 1

                        # Группировка по периодам
                        if days_diff < 1:  # Сегодня
                            active_today += 1
                            if hours_diff <= 24 and user_language in countries_24h:
                                countries_24h[user_language] += 1
                        elif days_diff < 7:  # Неделя
                            active_week += 1
                            if user_language in countries_week:
                                countries_week[user_language] += 1
                        elif days_diff < 30:  # Месяц
                            active_month += 1
                            if user_language in countries_month:
                                countries_month[user_language] += 1
                        else:  # Более месяца
                            active_more += 1
                    except (ValueError, TypeError):
                        active_more += 1  # В случае ошибки, считаем как неактивного

                # Анализ времени регистрации
                if 'registration_time' in user:
                    try:
                        registration_time = datetime.strptime(user['registration_time'], "%Y-%m-%d %H:%M:%S")
                        days_diff = (current_time - registration_time).days

                        # Группировка по месяцам
                        month_key = registration_time.strftime("%Y-%m")
                        if month_key not in months_registration:
                            months_registration[month_key] = 0
                        months_registration[month_key] += 1

                        # Группировка по периодам
                        if days_diff < 1:  # Сегодня
                            joined_today += 1
                        elif days_diff < 7:  # Неделя
                            joined_week += 1
                        elif days_diff < 30:  # Месяц
                            joined_month += 1
                        else:  # Более месяца
                            joined_more += 1
                    except (ValueError, TypeError):
                        joined_more += 1  # В случае ошибки, считаем как старого пользователя

        # Сортируем словари по ключам (месяцам) для удобства
        months_activity = dict(sorted(months_activity.items()))
        months_registration = dict(sorted(months_registration.items()))

        # Получаем статистику по дням за последний месяц
        days_stats = get_daily_stats_for_month(users_data)

        return {
            "active": {
                "today": active_today,
                "week": active_week,
                "month": active_month,
                "more": active_more,
                "by_months": months_activity
            },
            "joined": {
                "today": joined_today,
                "week": joined_week,
                "month": joined_month,
                "more": joined_more,
                "by_months": months_registration
            },
            "countries": {
                "24h": countries_24h,
                "week": countries_week,
                "month": countries_month
            },
            "daily_stats": days_stats,
            "total_users": len(users_data)
        }
    except Exception as e:
        logger.error(f"Ошибка при получении статистики активности: {e}")
        return None

def get_daily_stats_for_month(users_data):
    """Собирает статистику по дням за последний месяц"""
    try:
        if not users_data:
            return {}
            
        current_time = datetime.now()
        month_ago = current_time - timedelta(days=30)
        
        # Словарь для хранения статистики по дням
        days_stats = {}
        
        # Инициализируем словарь для всех дней последнего месяца
        for i in range(30):
            day = (current_time - timedelta(days=i)).strftime("%Y-%m-%d")
            days_stats[day] = {"active": 0, "joined": 0, "RU": 0, "EN": 0}
        
        # Заполняем статистику
        for user in users_data:
            if isinstance(user, dict):
                user_language = user.get('language', 'unknown')
                
                # Активность
                if 'last_access' in user:
                    try:
                        last_access = datetime.strptime(user['last_access'], "%Y-%m-%d %H:%M:%S")
                        if last_access >= month_ago:
                            day_key = last_access.strftime("%Y-%m-%d")
                            if day_key in days_stats:
                                days_stats[day_key]["active"] += 1
                                if user_language in ["RU", "EN"]:
                                    days_stats[day_key][user_language] += 1
                    except (ValueError, TypeError):
                        pass
                
                # Регистрация
                if 'registration_time' in user:
                    try:
                        registration_time = datetime.strptime(user['registration_time'], "%Y-%m-%d %H:%M:%S")
                        if registration_time >= month_ago:
                            day_key = registration_time.strftime("%Y-%m-%d")
                            if day_key in days_stats:
                                days_stats[day_key]["joined"] += 1
                    except (ValueError, TypeError):
                        pass
        
        # Сортируем по датам (от новых к старым)
        return dict(sorted(days_stats.items(), reverse=True))
    except Exception as e:
        logger.error(f"Ошибка при получении статистики по дням: {e}")
        return {}

def generate_detailed_stats_file():
    """Генерирует текстовый файл с детальной статистикой по дням"""
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        filename = f"{today}.txt"
        
        with open(filename, "w", encoding="utf-8") as f:
            f.write(f"Детальная статистика по дням за последний месяц (создана {datetime.now().strftime('%Y-%m-%d %H:%M:%S')})\n\n")
            f.write("Формат данных: Дата, Активные пользователи, Новые пользователи, RU пользователи, EN пользователи\n\n")
            
            # Заполняем демонстрационными данными
            current_date = datetime.now()
            for i in range(30):
                date = current_date - timedelta(days=i)
                date_str = date.strftime("%Y-%m-%d")
                f.write(f"{date_str}: Активных: {i*2}, Новых: {i}, RU: {i}, EN: {i*2}\n")
        
        return filename
    except Exception as e:
        logger.error(f"Ошибка при генерации файла статистики: {e}")
        return None
