import logging
import json
from datetime import datetime, timedelta

# Настройка логирования
logger = logging.getLogger(__name__)

def get_user_stats(users_data, user_id):
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

def get_language_trend_stats(users_data):
    """Получает статистику по языкам с динамикой по времени"""
    try:
        if users_data is None:
            logger.error("Не удалось получить данные пользователей для статистики языков")
            return None

        current_time = datetime.now()
        
        # Статистика по периодам
        last_24h = {
            "RU": 0,
            "EN": 0
        }
        last_week = {
            "RU": 0,
            "EN": 0
        }
        last_month = {
            "RU": 0,
            "EN": 0
        }
        
        for user in users_data:
            if isinstance(user, dict) and 'registration_time' in user:
                try:
                    registration_time = datetime.strptime(user['registration_time'], "%Y-%m-%d %H:%M:%S")
                    days_diff = (current_time - registration_time).days
                    hours_diff = (current_time - registration_time).total_seconds() / 3600
                    
                    language = user.get('language', 'Unknown')
                    if language not in ['RU', 'EN']:
                        continue
                        
                    # Группировка по периодам
                    if hours_diff < 24:  # Последние 24 часа
                        last_24h[language] += 1
                        
                    if days_diff < 7:  # Последняя неделя
                        last_week[language] += 1
                        
                    if days_diff < 30:  # Последний месяц
                        last_month[language] += 1
                        
                except (ValueError, TypeError):
                    continue
        
        return {
            "last_24h": last_24h,
            "last_week": last_week,
            "last_month": last_month
        }
    except Exception as e:
        logger.error(f"Ошибка при получении статистики языков: {e}")
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
        
        # Словари для хранения данных по языкам
        language_stats = {
            "active": {
                "today": {"RU": 0, "EN": 0},
                "week": {"RU": 0, "EN": 0},
                "month": {"RU": 0, "EN": 0},
                "more": {"RU": 0, "EN": 0}
            },
            "joined": {
                "today": {"RU": 0, "EN": 0},
                "week": {"RU": 0, "EN": 0},
                "month": {"RU": 0, "EN": 0},
                "more": {"RU": 0, "EN": 0}
            }
        }

        for user in users_data:
            if isinstance(user, dict):
                language = user.get('language', 'Unknown')
                
                # Анализ времени последнего захода
                if 'last_access' in user:
                    try:
                        last_access = datetime.strptime(user['last_access'], "%Y-%m-%d %H:%M:%S")
                        days_diff = (current_time - last_access).days

                        # Группировка по месяцам
                        month_key = last_access.strftime("%Y-%m")
                        if month_key not in months_activity:
                            months_activity[month_key] = 0
                        months_activity[month_key] += 1

                        # Группировка по периодам
                        if days_diff < 1:  # Сегодня
                            active_today += 1
                            if language in ['RU', 'EN']:
                                language_stats["active"]["today"][language] += 1
                        elif days_diff < 7:  # Неделя
                            active_week += 1
                            if language in ['RU', 'EN']:
                                language_stats["active"]["week"][language] += 1
                        elif days_diff < 30:  # Месяц
                            active_month += 1
                            if language in ['RU', 'EN']:
                                language_stats["active"]["month"][language] += 1
                        else:  # Более месяца
                            active_more += 1
                            if language in ['RU', 'EN']:
                                language_stats["active"]["more"][language] += 1
                    except (ValueError, TypeError):
                        active_more += 1  # В случае ошибки, считаем как неактивного
                        if language in ['RU', 'EN']:
                            language_stats["active"]["more"][language] += 1

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
                            if language in ['RU', 'EN']:
                                language_stats["joined"]["today"][language] += 1
                        elif days_diff < 7:  # Неделя
                            joined_week += 1
                            if language in ['RU', 'EN']:
                                language_stats["joined"]["week"][language] += 1
                        elif days_diff < 30:  # Месяц
                            joined_month += 1
                            if language in ['RU', 'EN']:
                                language_stats["joined"]["month"][language] += 1
                        else:  # Более месяца
                            joined_more += 1
                            if language in ['RU', 'EN']:
                                language_stats["joined"]["more"][language] += 1
                    except (ValueError, TypeError):
                        joined_more += 1  # В случае ошибки, считаем как старого пользователя
                        if language in ['RU', 'EN']:
                            language_stats["joined"]["more"][language] += 1

        # Сортируем словари по ключам (месяцам) для удобства
        months_activity = dict(sorted(months_activity.items()))
        months_registration = dict(sorted(months_registration.items()))

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
            "language_stats": language_stats,
            "total_users": len(users_data)
        }
    except Exception as e:
        logger.error(f"Ошибка при получении статистики активности: {e}")
        return None

def get_daily_detailed_stats(users_data):
    """Получает детальную статистику по дням за последний месяц"""
    try:
        if users_data is None:
            logger.error("Не удалось получить данные пользователей для детальной статистики")
            return None
            
        current_time = datetime.now()
        thirty_days_ago = current_time - timedelta(days=30)
        
        # Создаем словарь для хранения статистики по дням
        daily_stats = {}
        
        # Инициализируем словарь на 30 дней назад
        for i in range(30):
            day = (current_time - timedelta(days=i)).strftime("%Y-%m-%d")
            daily_stats[day] = {
                "active": {"total": 0, "RU": 0, "EN": 0},
                "joined": {"total": 0, "RU": 0, "EN": 0}
            }
        
        # Заполняем словарь данными
        for user in users_data:
            if isinstance(user, dict):
                language = user.get('language', 'Unknown')
                
                # Анализ времени последнего захода
                if 'last_access' in user:
                    try:
                        last_access = datetime.strptime(user['last_access'], "%Y-%m-%d %H:%M:%S")
                        if last_access >= thirty_days_ago:
                            day_key = last_access.strftime("%Y-%m-%d")
                            if day_key in daily_stats:
                                daily_stats[day_key]["active"]["total"] += 1
                                if language in ['RU', 'EN']:
                                    daily_stats[day_key]["active"][language] += 1
                    except (ValueError, TypeError):
                        pass
                
                # Анализ времени регистрации
                if 'registration_time' in user:
                    try:
                        registration_time = datetime.strptime(user['registration_time'], "%Y-%m-%d %H:%M:%S")
                        if registration_time >= thirty_days_ago:
                            day_key = registration_time.strftime("%Y-%m-%d")
                            if day_key in daily_stats:
                                daily_stats[day_key]["joined"]["total"] += 1
                                if language in ['RU', 'EN']:
                                    daily_stats[day_key]["joined"][language] += 1
                    except (ValueError, TypeError):
                        pass
        
        # Сортируем словарь по дате (от новых к старым)
        daily_stats = dict(sorted(daily_stats.items(), reverse=True))
        
        return daily_stats
    except Exception as e:
        logger.error(f"Ошибка при получении детальной статистики по дням: {e}")
        return None

def generate_detailed_stats_file(users_data):
    """Генерирует текстовый файл с детальной статистикой"""
    try:
        daily_stats = get_daily_detailed_stats(users_data)
        if not daily_stats:
            return None
            
        current_date = datetime.now().strftime("%Y-%m-%d")
        filename = f"{current_date}.txt"
        
        # Формируем содержимое файла
        content = f"Детальная статистика за период: {current_date} - {min(daily_stats.keys())}\n\n"
        content += "Формат данных: YYYY-MM-DD: Всего активных (RU/EN) | Всего новых (RU/EN)\n\n"
        
        for day, stats in daily_stats.items():
            active_total = stats["active"]["total"]
            active_ru = stats["active"]["RU"]
            active_en = stats["active"]["EN"]
            
            joined_total = stats["joined"]["total"]
            joined_ru = stats["joined"]["RU"]
            joined_en = stats["joined"]["EN"]
            
            content += f"{day}: Активных: {active_total} (RU: {active_ru}, EN: {active_en}) | "
            content += f"Новых: {joined_total} (RU: {joined_ru}, EN: {joined_en})\n"
        
        return {
            "filename": filename,
            "content": content
        }
    except Exception as e:
        logger.error(f"Ошибка при генерации файла статистики: {e}")
        return None

def format_stats_message(stats_type, stats_data, language='RU'):
    """Форматирует сообщение статистики в зависимости от типа и языка"""
    if stats_type == "user":
        # Форматирование статистики пользователя
        user_stats = stats_data
        if language == 'RU':
            return (
                f"📊 *Ваша статистика:*\n"
                f"🆔 ID: `{user_stats.get('user_id')}`\n"
                f"🌐 Язык: {user_stats.get('language')}\n"
                f"📅 Дата регистрации: {user_stats.get('registration_time')}\n"
                f"⏱ Последний вход: {user_stats.get('last_access')}\n"
            )
        else:  # EN
            return (
                f"📊 *Your statistics:*\n"
                f"🆔 ID: `{user_stats.get('user_id')}`\n"
                f"🌐 Language: {user_stats.get('language')}\n"
                f"📅 Registration date: {user_stats.get('registration_time')}\n"
                f"⏱ Last access: {user_stats.get('last_access')}\n"
            )
    
    elif stats_type == "global":
        # Форматирование общей статистики
        global_stats = stats_data
        if language == 'RU':
            return (
                f"📈 *Общая статистика:*\n"
                f"👥 Всего пользователей: {global_stats['total_users']}\n"
                f"🇷🇺 Пользователей RU: {global_stats['ru_users']}\n"
                f"🇬🇧 Пользователей EN: {global_stats['en_users']}\n"
            )
        else:  # EN
            return (
                f"📈 *Global statistics:*\n"
                f"👥 Total users: {global_stats['total_users']}\n"
                f"🇷🇺 RU users: {global_stats['ru_users']}\n"
                f"🇬🇧 EN users: {global_stats['en_users']}\n"
            )
    
    elif stats_type == "activity":
        # Форматирование статистики активности
        activity_stats = stats_data
        language_stats = activity_stats.get("language_stats", {})
        
        if language == 'RU':
            result = (
                f"📊 *Статистика активности пользователей:*\n\n"
                f"👥 *Всего пользователей:* {activity_stats['total_users']}\n\n"
                f"*Активность (последний заход):*\n"
                f"🔹 Сегодня: {activity_stats['active']['today']}\n"
                f"🔹 За неделю: {activity_stats['active']['week']}\n"
                f"🔹 За месяц: {activity_stats['active']['month']}\n"
                f"🔹 Более месяца: {activity_stats['active']['more']}\n\n"
                f"*Регистрация новых пользователей:*\n"
                f"🔸 Сегодня: {activity_stats['joined']['today']}\n"
                f"🔸 За неделю: {activity_stats['joined']['week']}\n"
                f"🔸 За месяц: {activity_stats['joined']['month']}\n"
                f"🔸 Более месяца назад: {activity_stats['joined']['more']}\n\n"
            )
            
            # Добавляем статистику по языкам
            result += "*Статистика по языкам:*\n"
            result += "RU / EN\n"
            
            # Активность по языкам
            result += f"Активность сегодня: {language_stats['active']['today']['RU']} / {language_stats['active']['today']['EN']}\n"
            result += f"Активность за неделю: {language_stats['active']['week']['RU']} / {language_stats['active']['week']['EN']}\n" 
            result += f"Активность за месяц: {language_stats['active']['month']['RU']} / {language_stats['active']['month']['EN']}\n\n"
            
            # Регистрация по языкам
            result += f"Регистрации сегодня: {language_stats['joined']['today']['RU']} / {language_stats['joined']['today']['EN']}\n"
            result += f"Регистрации за неделю: {language_stats['joined']['week']['RU']} / {language_stats['joined']['week']['EN']}\n"
            result += f"Регистрации за месяц: {language_stats['joined']['month']['RU']} / {language_stats['joined']['month']['EN']}\n\n"
        else:  # EN
            result = (
                f"📊 *User Activity Statistics:*\n\n"
                f"👥 *Total users:* {activity_stats['total_users']}\n\n"
                f"*Activity (last access):*\n"
                f"🔹 Today: {activity_stats['active']['today']}\n"
                f"🔹 This week: {activity_stats['active']['week']}\n"
                f"🔹 This month: {activity_stats['active']['month']}\n"
                f"🔹 More than a month: {activity_stats['active']['more']}\n\n"
                f"*New user registrations:*\n"
                f"🔸 Today: {activity_stats['joined']['today']}\n"
                f"🔸 This week: {activity_stats['joined']['week']}\n"
                f"🔸 This month: {activity_stats['joined']['month']}\n"
                f"🔸 More than a month ago: {activity_stats['joined']['more']}\n\n"
            )
            
            # Добавляем статистику по языкам
            result += "*Language statistics:*\n"
            result += "RU / EN\n"
            
            # Активность по языкам
            result += f"Activity today: {language_stats['active']['today']['RU']} / {language_stats['active']['today']['EN']}\n"
            result += f"Activity this week: {language_stats['active']['week']['RU']} / {language_stats['active']['week']['EN']}\n"
            result += f"Activity this month: {language_stats['active']['month']['RU']} / {language_stats['active']['month']['EN']}\n\n"
            
            # Регистрация по языкам  
            result += f"Registrations today: {language_stats['joined']['today']['RU']} / {language_stats['joined']['today']['EN']}\n"
            result += f"Registrations this week: {language_stats['joined']['week']['RU']} / {language_stats['joined']['week']['EN']}\n"
            result += f"Registrations this month: {language_stats['joined']['month']['RU']} / {language_stats['joined']['month']['EN']}\n\n"
        
        return result
    
    elif stats_type == "language_trend":
        # Форматирование статистики по языкам с динамикой
        lang_stats = stats_data
        
        if language == 'RU':
            return (
                f"*Распределение по языкам:*\n"
                f"За последние 24 часа: RU: {lang_stats['last_24h']['RU']}, EN: {lang_stats['last_24h']['EN']}\n"
                f"За последнюю неделю: RU: {lang_stats['last_week']['RU']}, EN: {lang_stats['last_week']['EN']}\n"
                f"За последний месяц: RU: {lang_stats['last_month']['RU']}, EN: {lang_stats['last_month']['EN']}\n"
            )
        else:  # EN
            return (
                f"*Language distribution:*\n"
                f"Last 24 hours: RU: {lang_stats['last_24h']['RU']}, EN: {lang_stats['last_24h']['EN']}\n"
                f"Last week: RU: {lang_stats['last_week']['RU']}, EN: {lang_stats['last_week']['EN']}\n"
                f"Last month: RU: {lang_stats['last_month']['RU']}, EN: {lang_stats['last_month']['EN']}\n"
            )
    
    return ""
