# statistics.py

import logging
import json
import time
from datetime import datetime, timedelta
import requests
from telebot import types

# Настройка логирования
logger = logging.getLogger(__name__)

# Глобальные переменные для кэша
users_cache = None
last_cache_update = 0
CACHE_TTL = 7200  # Время жизни кэша в секундах (2 часа)

def get_users_data(JSONBIN_BIN_ID, JSONBIN_API_KEY, force_update=False):
    """Получает данные всех пользователей из JSONBin.io с кэшированием"""
    global users_cache, last_cache_update

    current_time = time.time()

    # Используем кэш, если он актуален и не требуется принудительное обновление
    if not force_update and users_cache is not None and (current_time - last_cache_update) < CACHE_TTL:
        return users_cache

    try:
        logger.debug("Запрашиваем данные из JSONBin...")
        url = f"https://api.jsonbin.io/v3/b/{JSONBIN_BIN_ID}"
        headers = {
            "X-Master-Key": JSONBIN_API_KEY,
            "X-Bin-Meta": "false"  # Получаем только содержимое без метаданных
        }
        response = requests.get(url, headers=headers)
        logger.debug(f"Ответ от JSONBin: {response.status_code}")

        if response.status_code == 200:
            try:
                data = response.json()
                if isinstance(data, dict) and 'users' in data:
                    users_data = data['users']
                else:
                    logger.warning("Неверная структура данных, инициализируем...")
                    users_data = []

                # Обновляем кэш
                users_cache = users_data
                last_cache_update = current_time
                logger.debug(f"Данные пользователей обновлены: {len(users_data)} записей")
                return users_data
            except json.JSONDecodeError:
                logger.error(f"Ошибка декодирования JSON: {response.text}")
                return users_cache or []
        else:
            logger.error(f"Ошибка получения данных из JSONBin: {response.status_code}, {response.text}")
            return users_cache or []  # Возвращаем старый кэш, если он есть
    except Exception as e:
        logger.error(f"Ошибка при получении данных из JSONBin: {e}")
        return users_cache or []

def get_user_language(user_id, JSONBIN_BIN_ID, JSONBIN_API_KEY):
    """Получает язык пользователя"""
    users = get_users_data(JSONBIN_BIN_ID, JSONBIN_API_KEY)
    if users is None:
        return 'RU'  # По умолчанию RU

    for user in users:
        if isinstance(user, dict) and user.get('user_id') == user_id:
            return user.get('language', 'RU')

    return 'RU'  # По умолчанию RU

def get_user_stats(user_id, JSONBIN_BIN_ID, JSONBIN_API_KEY):
    """Получает статистику для конкретного пользователя"""
    try:
        users = get_users_data(JSONBIN_BIN_ID, JSONBIN_API_KEY)
        if users is None:
            logger.error("Не удалось получить данные пользователей для статистики")
            return None

        for user in users:
            if isinstance(user, dict) and user.get('user_id') == user_id:
                return user

        return None
    except Exception as e:
        logger.error(f"Ошибка при получении статистики пользователя: {e}")
        return None

def get_global_stats(JSONBIN_BIN_ID, JSONBIN_API_KEY):
    """Получает общую статистику всех пользователей"""
    try:
        users = get_users_data(JSONBIN_BIN_ID, JSONBIN_API_KEY)
        if users is None:
            logger.error("Не удалось получить данные пользователей для общей статистики")
            return None

        total_users = len(users)
        ru_users = 0
        en_users = 0

        for user in users:
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

def get_activity_stats(JSONBIN_BIN_ID, JSONBIN_API_KEY):
    """Получает статистику пользователей по времени активности, регистрации и языкам"""
    try:
        users = get_users_data(JSONBIN_BIN_ID, JSONBIN_API_KEY)
        if users is None:
            logger.error("Не удалось получить данные пользователей для статистики активности")
            return None

        current_time = datetime.now()
        day_ago = current_time - timedelta(days=1)
        week_ago = current_time - timedelta(days=7)
        month_ago = current_time - timedelta(days=30)

        # Для статистики по времени последнего захода
        active_today = {"total": 0, "RU": 0, "EN": 0}
        active_week = {"total": 0, "RU": 0, "EN": 0}
        active_month = {"total": 0, "RU": 0, "EN": 0}
        active_more = {"total": 0, "RU": 0, "EN": 0}

        # Для статистики по времени регистрации
        joined_today = {"total": 0, "RU": 0, "EN": 0}
        joined_week = {"total": 0, "RU": 0, "EN": 0}
        joined_month = {"total": 0, "RU": 0, "EN": 0}
        joined_more = {"total": 0, "RU": 0, "EN": 0}

        # Ежедневная статистика за последний месяц
        daily_stats = {}
        for i in range(30):
            date = (current_time - timedelta(days=i)).strftime("%Y-%m-%d")
            daily_stats[date] = {"active": {"total": 0, "RU": 0, "EN": 0}, 
                               "joined": {"total": 0, "RU": 0, "EN": 0}}

        for user in users:
            if isinstance(user, dict):
                language = user.get('language', 'unknown')
                
                # Анализ времени последнего захода
                if 'last_access' in user:
                    try:
                        last_access = datetime.strptime(user['last_access'], "%Y-%m-%d %H:%M:%S")
                        
                        # Учет в ежедневной статистике
                        last_access_date = last_access.strftime("%Y-%m-%d")
                        if last_access_date in daily_stats:
                            daily_stats[last_access_date]["active"]["total"] += 1
                            if language in ["RU", "EN"]:
                                daily_stats[last_access_date]["active"][language] += 1
                        
                        # Группировка по периодам
                        if last_access >= day_ago:  # За последние 24 часа
                            active_today["total"] += 1
                            if language in ["RU", "EN"]:
                                active_today[language] += 1
                        elif last_access >= week_ago:  # За последнюю неделю
                            active_week["total"] += 1
                            if language in ["RU", "EN"]:
                                active_week[language] += 1
                        elif last_access >= month_ago:  # За последний месяц
                            active_month["total"] += 1
                            if language in ["RU", "EN"]:
                                active_month[language] += 1
                        else:  # Более месяца
                            active_more["total"] += 1
                            if language in ["RU", "EN"]:
                                active_more[language] += 1
                    except (ValueError, TypeError):
                        active_more["total"] += 1
                        if language in ["RU", "EN"]:
                            active_more[language] += 1

                # Анализ времени регистрации
                if 'registration_time' in user:
                    try:
                        registration_time = datetime.strptime(user['registration_time'], "%Y-%m-%d %H:%M:%S")
                        
                        # Учет в ежедневной статистике
                        reg_date = registration_time.strftime("%Y-%m-%d")
                        if reg_date in daily_stats:
                            daily_stats[reg_date]["joined"]["total"] += 1
                            if language in ["RU", "EN"]:
                                daily_stats[reg_date]["joined"][language] += 1
                        
                        # Группировка по периодам
                        if registration_time >= day_ago:  # За последние 24 часа
                            joined_today["total"] += 1
                            if language in ["RU", "EN"]:
                                joined_today[language] += 1
                        elif registration_time >= week_ago:  # За последнюю неделю
                            joined_week["total"] += 1
                            if language in ["RU", "EN"]:
                                joined_week[language] += 1
                        elif registration_time >= month_ago:  # За последний месяц
                            joined_month["total"] += 1
                            if language in ["RU", "EN"]:
                                joined_month[language] += 1
                        else:  # Более месяца
                            joined_more["total"] += 1
                            if language in ["RU", "EN"]:
                                joined_more[language] += 1
                    except (ValueError, TypeError):
                        joined_more["total"] += 1
                        if language in ["RU", "EN"]:
                            joined_more[language] += 1

        return {
            "active": {
                "24h": active_today,
                "week": active_week,
                "month": active_month,
                "more": active_more
            },
            "joined": {
                "24h": joined_today,
                "week": joined_week,
                "month": joined_month,
                "more": joined_more
            },
            "daily": daily_stats,
            "total_users": len(users)
        }
    except Exception as e:
        logger.error(f"Ошибка при получении статистики активности: {e}")
        return None

def save_daily_stats_to_file(stats):
    """Сохраняет ежедневную статистику в файл"""
    try:
        current_date = datetime.now().strftime("%Y-%m-%d")
        filename = f"{current_date}.txt"
        
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(f"Daily Statistics Report - {current_date}\n")
            f.write("=" * 50 + "\n\n")
            
            f.write("Daily Active Users:\n")
            f.write("-" * 30 + "\n")
            for date, data in sorted(stats['daily'].items()):
                f.write(f"{date}: Total: {data['active']['total']}, RU: {data['active']['RU']}, EN: {data['active']['EN']}\n")
            
            f.write("\nDaily New Registrations:\n")
            f.write("-" * 30 + "\n")
            for date, data in sorted(stats['daily'].items()):
                f.write(f"{date}: Total: {data['joined']['total']}, RU: {data['joined']['RU']}, EN: {data['joined']['EN']}\n")
            
            f.write("\nSummary:\n")
            f.write("-" * 30 + "\n")
            f.write(f"Total users: {stats['total_users']}\n")
            f.write(f"Active in last 24h: Total: {stats['active']['24h']['total']}, RU: {stats['active']['24h']['RU']}, EN: {stats['active']['24h']['EN']}\n")
            f.write(f"Active in last week: Total: {stats['active']['week']['total']}, RU: {stats['active']['week']['RU']}, EN: {stats['active']['week']['EN']}\n")
            f.write(f"Active in last month: Total: {stats['active']['month']['total']}, RU: {stats['active']['month']['RU']}, EN: {stats['active']['month']['EN']}\n")
            f.write(f"Joined in last 24h: Total: {stats['joined']['24h']['total']}, RU: {stats['joined']['24h']['RU']}, EN: {stats['joined']['24h']['EN']}\n")
            f.write(f"Joined in last week: Total: {stats['joined']['week']['total']}, RU: {stats['joined']['week']['RU']}, EN: {stats['joined']['week']['EN']}\n")
            f.write(f"Joined in last month: Total: {stats['joined']['month']['total']}, RU: {stats['joined']['month']['RU']}, EN: {stats['joined']['month']['EN']}\n")
        
        return filename
    except Exception as e:
        logger.error(f"Ошибка при сохранении статистики в файл: {e}")
        return None

def generate_stats_message(user_id, JSONBIN_BIN_ID, JSONBIN_API_KEY):
    """Генерирует сообщение со статистикой для пользователя"""
    language = get_user_language(user_id, JSONBIN_BIN_ID, JSONBIN_API_KEY)
    user_stats = get_user_stats(user_id, JSONBIN_BIN_ID, JSONBIN_API_KEY)
    global_stats = get_global_stats(JSONBIN_BIN_ID, JSONBIN_API_KEY)
    activity_stats = get_activity_stats(JSONBIN_BIN_ID, JSONBIN_API_KEY)
    
    if not user_stats or not global_stats or not activity_stats:
        return (language == 'RU' and "Не удалось получить статистику." or 
                "Failed to retrieve statistics.")
    
    if language == 'RU':
        message = (
            f"📊 *Ваша статистика:*\n"
            f"🆔 ID: `{user_id}`\n"
            f"🌐 Язык: {user_stats.get('language')}\n"
            f"📅 Дата регистрации: {user_stats.get('registration_time')}\n"
            f"⏱ Последний вход: {user_stats.get('last_access')}\n\n"
            f"📈 *Общая статистика:*\n"
            f"👥 Всего пользователей: {global_stats['total_users']}\n"
            f"🇷🇺 Пользователей RU: {global_stats['ru_users']}\n"
            f"🇬🇧 Пользователей EN: {global_stats['en_users']}\n\n"
            f"📊 *Активность пользователей:*\n\n"
            f"*За 24 часа:*\n"
            f"• Активных: {activity_stats['active']['24h']['total']} (RU: {activity_stats['active']['24h']['RU']}, EN: {activity_stats['active']['24h']['EN']})\n"
            f"• Новых: {activity_stats['joined']['24h']['total']} (RU: {activity_stats['joined']['24h']['RU']}, EN: {activity_stats['joined']['24h']['EN']})\n\n"
            f"*За неделю:*\n"
            f"• Активных: {activity_stats['active']['week']['total']} (RU: {activity_stats['active']['week']['RU']}, EN: {activity_stats['active']['week']['EN']})\n"
            f"• Новых: {activity_stats['joined']['week']['total']} (RU: {activity_stats['joined']['week']['RU']}, EN: {activity_stats['joined']['week']['EN']})\n\n"
            f"*За месяц:*\n"
            f"• Активных: {activity_stats['active']['month']['total']} (RU: {activity_stats['active']['month']['RU']}, EN: {activity_stats['active']['month']['EN']})\n"
            f"• Новых: {activity_stats['joined']['month']['total']} (RU: {activity_stats['joined']['month']['RU']}, EN: {activity_stats['joined']['month']['EN']})\n"
        )
    else:  # EN
        message = (
            f"📊 *Your statistics:*\n"
            f"🆔 ID: `{user_id}`\n"
            f"🌐 Language: {user_stats.get('language')}\n"
            f"📅 Registration date: {user_stats.get('registration_time')}\n"
            f"⏱ Last access: {user_stats.get('last_access')}\n\n"
            f"📈 *Global statistics:*\n"
            f"👥 Total users: {global_stats['total_users']}\n"
            f"🇷🇺 RU users: {global_stats['ru_users']}\n"
            f"🇬🇧 EN users: {global_stats['en_users']}\n\n"
            f"📊 *User activity:*\n\n"
            f"*Last 24 hours:*\n"
            f"• Active: {activity_stats['active']['24h']['total']} (RU: {activity_stats['active']['24h']['RU']}, EN: {activity_stats['active']['24h']['EN']})\n"
            f"• New: {activity_stats['joined']['24h']['total']} (RU: {activity_stats['joined']['24h']['RU']}, EN: {activity_stats['joined']['24h']['EN']})\n\n"
            f"*Last week:*\n"
            f"• Active: {activity_stats['active']['week']['total']} (RU: {activity_stats['active']['week']['RU']}, EN: {activity_stats['active']['week']['EN']})\n"
            f"• New: {activity_stats['joined']['week']['total']} (RU: {activity_stats['joined']['week']['RU']}, EN: {activity_stats['joined']['week']['EN']})\n\n"
            f"*Last month:*\n"
            f"• Active: {activity_stats['active']['month']['total']} (RU: {activity_stats['active']['month']['RU']}, EN: {activity_stats['active']['month']['EN']})\n"
            f"• New: {activity_stats['joined']['month']['total']} (RU: {activity_stats['joined']['month']['RU']}, EN: {activity_stats['joined']['month']['EN']})\n"
        )
    
    return message

def get_detailed_stats_button(language):
    """Возвращает инлайн кнопку для просмотра детальной статистики"""
    markup = types.InlineKeyboardMarkup()
    if language == 'RU':
        button = types.InlineKeyboardButton("📊 Детальная статистика", callback_data='detailed_stats')
    else:  # EN
        button = types.InlineKeyboardButton("📊 Detailed Statistics", callback_data='detailed_stats')
    markup.add(button)
    return markup
