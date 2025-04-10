import logging
import time
import requests
import json
from datetime import datetime, timedelta
from telebot import types
import os

# Настройка логирования
logger = logging.getLogger(__name__)

# Константы для JSONBin
JSONBIN_API_KEY = "$2a$10$hT79uCEaJENfQBZ7576aL.upUOtnPqJZX53sWcln0HZib/bgs.8.u"
JSONBIN_BIN_ID = "67f532028a456b796684e974"

# Константы для кэширования и времени активности
users_cache = None
last_cache_update = 0
CACHE_TTL = 300  # Время жизни кэша в секундах (5 минут)
ACTIVITY_UPDATE_COOLDOWN = 6 * 3600  # 6 часов в секундах

def initialize_jsonbin():
    """Проверяет и инициализирует структуру в JSONBin, если она отсутствует"""
    try:
        users = get_users_data(force_update=True)
        if users is None:
            # Создаем начальную структуру
            initial_data = {"users": []}
            url = f"https://api.jsonbin.io/v3/b/{JSONBIN_BIN_ID}"
            headers = {
                "X-Master-Key": JSONBIN_API_KEY,
                "Content-Type": "application/json"
            }
            response = requests.put(url, json=initial_data, headers=headers)
            logger.info(f"Инициализация JSONBin: {response.status_code}, {response.text}")
            return response.status_code == 200
        return True
    except Exception as e:
        logger.error(f"Ошибка при инициализации JSONBin: {e}")
        return False

def get_users_data(force_update=False):
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
                    # Если структура неправильная, инициализируем ее
                    logger.warning("Неверная структура данных, инициализируем...")
                    users_data = []
                    initialize_jsonbin()

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
        return users_cache or []  # Возвращаем старый кэш, если он есть

def update_users_data(users_data):
    """Обновляет данные пользователей в JSONBin.io и кэш"""
    global users_cache, last_cache_update

    try:
        logger.debug(f"Обновляем данные пользователей: {len(users_data)} записей")
        url = f"https://api.jsonbin.io/v3/b/{JSONBIN_BIN_ID}"
        headers = {
            "X-Master-Key": JSONBIN_API_KEY,
            "Content-Type": "application/json"
        }
        data = {"users": users_data}
        response = requests.put(url, json=data, headers=headers)
        logger.debug(f"Ответ на обновление данных: {response.status_code}")

        if response.status_code == 200:
            # Обновляем кэш после успешного обновления в JSONBin
            users_cache = users_data
            last_cache_update = time.time()
            return True
        else:
            logger.error(f"Ошибка обновления данных в JSONBin: {response.status_code}, {response.text}")
            return False
    except Exception as e:
        logger.error(f"Ошибка при обновлении данных в JSONBin: {e}")
        return False

def is_user_authorized(user_id):
    """Проверяет, авторизован ли пользователь, ищет его ID в кэше данных JSONBin"""
    try:
        users = get_users_data()
        if users is None:
            logger.warning("Не удалось получить данные пользователей")
            return False

        for user in users:
            if isinstance(user, dict) and user.get('user_id') == user_id:
                # Проверяем пользователя без обновления времени последнего захода
                # Это будет делать check_and_update_last_access если прошло более 6 часов
                check_and_update_last_access(user_id)
                logger.debug(f"Пользователь {user_id} найден в базе")
                return True

        logger.debug(f"Пользователь {user_id} не найден в базе")
        return False
    except Exception as e:
        logger.error(f"Ошибка при проверке авторизации: {e}")
        return False

def check_and_update_last_access(user_id):
    """
    Проверяет и обновляет время последнего захода пользователя,
    только если прошло больше 6 часов с момента последнего обновления
    """
    try:
        users = get_users_data()
        if users is None:
            logger.error("Не удалось получить данные пользователей для обновления времени последнего захода")
            return False

        current_time = datetime.now()
        update_needed = False

        for user in users:
            if isinstance(user, dict) and user.get('user_id') == user_id:
                # Проверяем, прошло ли 6 часов с последнего обновления
                if 'last_access' in user:
                    try:
                        last_access_time = datetime.strptime(user['last_access'], "%Y-%m-%d %H:%M:%S")
                        time_diff = current_time - last_access_time
                        # Обновляем только если прошло более 6 часов
                        if time_diff.total_seconds() >= ACTIVITY_UPDATE_COOLDOWN:
                            update_needed = True
                    except (ValueError, TypeError):
                        # Если не удалось распарсить дату или другая ошибка, обновляем время
                        update_needed = True
                else:
                    # Если поле last_access отсутствует, обновляем
                    update_needed = True

                # Обновляем время, если необходимо
                if update_needed:
                    user['last_access'] = current_time.strftime("%Y-%m-%d %H:%M:%S")
                    update_users_data(users)
                    logger.debug(f"Обновлено время последнего захода для пользователя {user_id}")
                else:
                    logger.debug(f"Пропущено обновление времени для пользователя {user_id}: прошло менее 6 часов")

                return True

        return False
    except Exception as e:
        logger.error(f"Ошибка при проверке и обновлении времени последнего захода: {e}")
        return False

def register_user(user_id, language):
    """Регистрирует нового пользователя в JSONBin"""
    try:
        users = get_users_data()
        if users is None:
            logger.error("Не удалось получить данные пользователей для регистрации")
            return False

        # Проверяем, существует ли пользователь
        for user in users:
            if isinstance(user, dict) and user.get('user_id') == user_id:
                logger.debug(f"Пользователь {user_id} уже зарегистрирован")
                # Проверяем и обновляем время последнего захода, если прошло более 6 часов
                check_and_update_last_access(user_id)
                return True  # Пользователь уже зарегистрирован

        # Добавляем нового пользователя
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        new_user = {
            "user_id": user_id,
            "language": language,
            "registration_time": now,
            "last_access": now
        }

        logger.debug(f"Регистрируем нового пользователя: {new_user}")
        users.append(new_user)
        result = update_users_data(users)
        logger.debug(f"Результат регистрации: {result}")
        return result
    except Exception as e:
        logger.error(f"Ошибка при регистрации пользователя: {e}")
        return False

def get_user_stats(user_id):
    """Получает статистику для конкретного пользователя"""
    try:
        users = get_users_data()
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

def get_global_stats():
    """Получает общую статистику всех пользователей"""
    try:
        users = get_users_data()
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

def get_language_stats_history():
    """Получает историческую статистику по языкам за разные периоды времени"""
    try:
        users = get_users_data()
        if users is None:
            logger.error("Не удалось получить данные пользователей для исторической статистики")
            return None

        current_time = datetime.now()
        
        # Подсчет для разных периодов
        stats = {
            "today": {"RU": 0, "EN": 0},
            "week": {"RU": 0, "EN": 0},
            "month": {"RU": 0, "EN": 0}
        }

        # История по дням за последний месяц
        daily_stats = {}
        
        # Инициализация статистики по дням
        for i in range(30):
            day = (current_time - timedelta(days=i)).strftime("%Y-%m-%d")
            daily_stats[day] = {"RU": 0, "EN": 0}

        for user in users:
            if isinstance(user, dict) and 'registration_time' in user and 'language' in user:
                lang = user.get('language')
                if lang not in ('RU', 'EN'):
                    continue
                
                try:
                    reg_time = datetime.strptime(user['registration_time'], "%Y-%m-%d %H:%M:%S")
                    days_diff = (current_time - reg_time).days
                    
                    # Добавляем в статистику по периодам
                    if days_diff < 1:  # Сегодня
                        stats["today"][lang] += 1
                    if days_diff < 7:  # Неделя
                        stats["week"][lang] += 1
                    if days_diff < 30:  # Месяц
                        stats["month"][lang] += 1
                    
                    # Добавляем в ежедневную статистику
                    day_key = reg_time.strftime("%Y-%m-%d")
                    if day_key in daily_stats:
                        daily_stats[day_key][lang] += 1
                        
                except (ValueError, TypeError):
                    continue

        return {
            "periods": stats,
            "daily": daily_stats
        }
    except Exception as e:
        logger.error(f"Ошибка при получении исторической статистики: {e}")
        return None

def get_activity_stats():
    """Получает статистику пользователей по времени активности и регистрации с добавлением языковой статистики"""
    try:
        users = get_users_data()
        if users is None:
            logger.error("Не удалось получить данные пользователей для статистики активности")
            return None

        current_time = datetime.now()

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

        # Словари для хранения количества пользователей по месяцам их активности/регистрации
        months_activity = {}
        months_registration = {}

        for user in users:
            if isinstance(user, dict):
                language = user.get('language', 'unknown')
                
                # Анализ времени последнего захода
                if 'last_access' in user:
                    try:
                        last_access = datetime.strptime(user['last_access'], "%Y-%m-%d %H:%M:%S")
                        days_diff = (current_time - last_access).days

                        # Группировка по месяцам
                        month_key = last_access.strftime("%Y-%m")
                        if month_key not in months_activity:
                            months_activity[month_key] = {"total": 0, "RU": 0, "EN": 0}
                        months_activity[month_key]["total"] += 1
                        if language in ["RU", "EN"]:
                            months_activity[month_key][language] += 1

                        # Группировка по периодам
                        if days_diff < 1:  # Сегодня
                            active_today["total"] += 1
                            if language in ["RU", "EN"]:
                                active_today[language] += 1
                        elif days_diff < 7:  # Неделя
                            active_week["total"] += 1
                            if language in ["RU", "EN"]:
                                active_week[language] += 1
                        elif days_diff < 30:  # Месяц
                            active_month["total"] += 1
                            if language in ["RU", "EN"]:
                                active_month[language] += 1
                        else:  # Более месяца
                            active_more["total"] += 1
                            if language in ["RU", "EN"]:
                                active_more[language] += 1
                    except (ValueError, TypeError):
                        active_more["total"] += 1  # В случае ошибки, считаем как неактивного
                        if language in ["RU", "EN"]:
                            active_more[language] += 1

                # Анализ времени регистрации
                if 'registration_time' in user:
                    try:
                        registration_time = datetime.strptime(user['registration_time'], "%Y-%m-%d %H:%M:%S")
                        days_diff = (current_time - registration_time).days

                        # Группировка по месяцам
                        month_key = registration_time.strftime("%Y-%m")
                        if month_key not in months_registration:
                            months_registration[month_key] = {"total": 0, "RU": 0, "EN": 0}
                        months_registration[month_key]["total"] += 1
                        if language in ["RU", "EN"]:
                            months_registration[month_key][language] += 1

                        # Группировка по периодам
                        if days_diff < 1:  # Сегодня
                            joined_today["total"] += 1
                            if language in ["RU", "EN"]:
                                joined_today[language] += 1
                        elif days_diff < 7:  # Неделя
                            joined_week["total"] += 1
                            if language in ["RU", "EN"]:
                                joined_week[language] += 1
                        elif days_diff < 30:  # Месяц
                            joined_month["total"] += 1
                            if language in ["RU", "EN"]:
                                joined_month[language] += 1
                        else:  # Более месяца
                            joined_more["total"] += 1
                            if language in ["RU", "EN"]:
                                joined_more[language] += 1
                    except (ValueError, TypeError):
                        joined_more["total"] += 1  # В случае ошибки, считаем как старого пользователя
                        if language in ["RU", "EN"]:
                            joined_more[language] += 1

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
            "total_users": len(users)
        }
    except Exception as e:
        logger.error(f"Ошибка при получении статистики активности: {e}")
        return None

def create_statistics_menu(language):
    """Создает инлайн-клавиатуру с меню статистики"""
    markup = types.InlineKeyboardMarkup(row_width=1)
    
    if language == 'RU':
        user_button = types.InlineKeyboardButton("📊 Моя статистика", callback_data='stats_user')
        activity_button = types.InlineKeyboardButton("📈 Активность пользователей", callback_data='stats_activity')
        detailed_button = types.InlineKeyboardButton("📋 Детальная статистика (файл)", callback_data='stats_detailed')
    else:  # 'EN'
        user_button = types.InlineKeyboardButton("📊 My statistics", callback_data='stats_user')
        activity_button = types.InlineKeyboardButton("📈 User activity", callback_data='stats_activity')
        detailed_button = types.InlineKeyboardButton("📋 Detailed statistics (file)", callback_data='stats_detailed')
    
    markup.add(user_button, activity_button, detailed_button)
    return markup

def show_user_statistics(user_id, language):
    """Формирует сообщение и клавиатуру со статистикой пользователя"""
    # Получаем статистику пользователя
    user_stats = get_user_stats(user_id)
    if not user_stats:
        message_text = "Не удалось получить вашу статистику." if language == 'RU' else "Failed to get your statistics."
        markup = create_back_button(language)
        return message_text, markup

    # Получаем общую статистику
    global_stats = get_global_stats()
    if not global_stats:
        message_text = "Не удалось получить общую статистику." if language == 'RU' else "Failed to get global statistics."
        markup = create_back_button(language)
        return message_text, markup

    # Формируем сообщение в зависимости от языка пользователя
    if language == 'RU':
        message_text = (
            f"📊 *Ваша статистика:*\n"
            f"🆔 ID: `{user_id}`\n"
            f"🌐 Язык: {user_stats.get('language')}\n"
            f"📅 Дата регистрации: {user_stats.get('registration_time')}\n"
            f"⏱ Последний вход: {user_stats.get('last_access')}\n\n"
            f"📈 *Общая статистика:*\n"
            f"👥 Всего пользователей: {global_stats['total_users']}\n"
            f"🇷🇺 Пользователей RU: {global_stats['ru_users']}\n"
            f"🇬🇧 Пользователей EN: {global_stats['en_users']}"
        )
    else:  # EN
        message_text = (
            f"📊 *Your statistics:*\n"
            f"🆔 ID: `{user_id}`\n"
            f"🌐 Language: {user_stats.get('language')}\n"
            f"📅 Registration date: {user_stats.get('registration_time')}\n"
            f"⏱ Last access: {user_stats.get('last_access')}\n\n"
            f"📈 *Global statistics:*\n"
            f"👥 Total users: {global_stats['total_users']}\n"
            f"🇷🇺 RU users: {global_stats['ru_users']}\n"
            f"🇬🇧 EN users: {global_stats['en_users']}"
        )

    # Создаем кнопку "назад"
    markup = create_back_button(language)
    
    return message_text, markup

def show_activity_statistics(user_id, language):
    """Формирует сообщение и клавиатуру со статистикой активности"""
    # Получаем статистику активности
    stats = get_activity_stats()
    if not stats:
        message_text = "Не удалось получить статистику активности." if language == 'RU' else "Failed to get activity statistics."
        markup = create_back_button(language)
        return message_text, markup

    # Формируем сообщение в зависимости от языка пользователя
    if language == 'RU':
        activity_stats_text = (
            f"📊 *Статистика активности пользователей:*\n\n"
            f"👥 *Всего пользователей:* {stats['total_users']}\n\n"
            f"*Активность (последний заход):*\n"
            f"🔹 Сегодня: {stats['active']['today']['total']} (RU: {stats['active']['today']['RU']}, EN: {stats['active']['today']['EN']})\n"
            f"🔹 За неделю: {stats['active']['week']['total']} (RU: {stats['active']['week']['RU']}, EN: {stats['active']['week']['EN']})\n"
            f"🔹 За месяц: {stats['active']['month']['total']} (RU: {stats['active']['month']['RU']}, EN: {stats['active']['month']['EN']})\n"
            f"🔹 Более месяца: {stats['active']['more']['total']} (RU: {stats['active']['more']['RU']}, EN: {stats['active']['more']['EN']})\n\n"
            f"*Регистрация новых пользователей:*\n"
            f"🔸 Сегодня: {stats['joined']['today']['total']} (RU: {stats['joined']['today']['RU']}, EN: {stats['joined']['today']['EN']})\n"
            f"🔸 За неделю: {stats['joined']['week']['total']} (RU: {stats['joined']['week']['RU']}, EN: {stats['joined']['week']['EN']})\n"
            f"🔸 За месяц: {stats['joined']['month']['total']} (RU: {stats['joined']['month']['RU']}, EN: {stats['joined']['month']['EN']})\n"
            f"🔸 Более месяца назад: {stats['joined']['more']['total']} (RU: {stats['joined']['more']['RU']}, EN: {stats['joined']['more']['EN']})\n\n"
        )

        # Ограничим количество месяцев в истории для избежания слишком длинного сообщения
        # Выберем только последние 3 месяца для вывода
        activity_months = list(stats['active']['by_months'].items())[-3:]
        joined_months = list(stats['joined']['by_months'].items())[-3:]
        
        if activity_months:
            activity_stats_text += "*Последние месяцы (активность):*\n"
            for month, counts in activity_months:
                activity_stats_text += f"📅 {month}: {counts['total']} (RU: {counts['RU']}, EN: {counts['EN']})\n"
            
        if joined_months:
            activity_stats_text += "\n*Последние месяцы (регистрация):*\n"
            for month, counts in joined_months:
                activity_stats_text += f"📅 {month}: {counts['total']} (RU: {counts['RU']}, EN: {counts['EN']})\n"
    else:  # EN
        activity_stats_text = (
            f"📊 *User Activity Statistics:*\n\n"
            f"👥 *Total users:* {stats['total_users']}\n\n"
            f"*Activity (last access):*\n"
            f"🔹 Today: {stats['active']['today']['total']} (RU: {stats['active']['today']['RU']}, EN: {stats['active']['today']['EN']})\n"
            f"🔹 This week: {stats['active']['week']['total']} (RU: {stats['active']['week']['RU']}, EN: {stats['active']['week']['EN']})\n"
            f"🔹 This month: {stats['active']['month']['total']} (RU: {stats['active']['month']['RU']}, EN: {stats['active']['month']['EN']})\n"
            f"🔹 More than a month: {stats['active']['more']['total']} (RU: {stats['active']['more']['RU']}, EN: {stats['active']['more']['EN']})\n\n"
            f"*New user registrations:*\n"
            f"🔸 Today: {stats['joined']['today']['total']} (RU: {stats['joined']['today']['RU']}, EN: {stats['joined']['today']['EN']})\n"
            f"🔸 This week: {stats['joined']['week']['total']} (RU: {stats['joined']['week']['RU']}, EN: {stats['joined']['week']['EN']})\n"
            f"🔸 This month: {stats['joined']['month']['total']} (RU: {stats['joined']['month']['RU']}, EN: {stats['joined']['month']['EN']})\n"
            f"🔸 More than a month ago: {stats['joined']['more']['total']} (RU: {stats['joined']['more']['RU']}, EN: {stats['joined']['more']['EN']})\n\n"
        )

        # Ограничим количество месяцев в истории для избежания слишком длинного сообщения
        # Выберем только последние 3 месяца для вывода
        activity_months = list(stats['active']['by_months'].items())[-3:]
        joined_months = list(stats['joined']['by_months'].items())[-3:]
        
        if activity_months:
            activity_stats_text += "*Recent months (activity):*\n"
            for month, counts in activity_months:
                activity_stats_text += f"📅 {month}: {counts['total']} (RU: {counts['RU']}, EN: {counts['EN']})\n"
            
        if joined_months:
            activity_stats_text += "\n*Recent months (registration):*\n"
            for month, counts in joined_months:
                activity_stats_text += f"📅 {month}: {counts['total']} (RU: {counts['RU']}, EN: {counts['EN']})\n"

    # Создаем кнопку "назад"
    markup = create_back_button(language)
    
    return activity_stats_text, markup

def create_back_button(language):
    """Создает инлайн-клавиатуру с кнопкой 'назад'"""
    markup = types.InlineKeyboardMarkup()
    back_text = "« Назад" if language == 'RU' else "« Back"
    back_button = types.InlineKeyboardButton(back_text, callback_data='stats_back')
    markup.add(back_button)
    return markup

def generate_detailed_statistics_file(language):
    """Генерирует файл с детальной статистикой по дням"""
    # Получаем текущую дату для имени файла
    current_date = datetime.now().strftime("%Y-%m-%d")
    filename = f"statistics_{current_date}.txt"
    
    # Получаем данные для статистики
    stats = get_activity_stats()
    global_stats = get_global_stats()
    language_history = get_language_stats_history()
    
    # Формируем содержимое файла
    if language == 'RU':
        content = [
            "=====================================================",
            "              ДЕТАЛЬНАЯ СТАТИСТИКА БОТА              ",
            f"                    {current_date}                   ",
            "=====================================================\n",
            "ОБЩАЯ ИНФОРМАЦИЯ:",
            "-----------------------------------------------------",
            f"Всего пользователей: {global_stats['total_users']}",
            f"Пользователей RU: {global_stats['ru_users']}",
            f"Пользователей EN: {global_stats['en_users']}\n",
            "СТАТИСТИКА АКТИВНОСТИ (последний вход):",
            "-----------------------------------------------------",
            f"Сегодня: {stats['active']['today']['total']} (RU: {stats['active']['today']['RU']}, EN: {stats['active']['today']['EN']})",
            f"За неделю: {stats['active']['week']['total']} (RU: {stats['active']['week']['RU']}, EN: {stats['active']['week']['EN']})",
            f"За месяц: {stats['active']['month']['total']} (RU: {stats['active']['month']['RU']}, EN: {stats['active']['month']['EN']})",
            f"Более месяца: {stats['active']['more']['total']} (RU: {stats['active']['more']['RU']}, EN: {stats['active']['more']['EN']})\n",
            "СТАТИСТИКА РЕГИСТРАЦИЙ:",
            "-----------------------------------------------------",
            f"Сегодня: {stats['joined']['today']['total']} (RU: {stats['joined']['today']['RU']}, EN: {stats['joined']['today']['EN']})",
            f"За неделю: {stats['joined']['week']['total']} (RU: {stats['joined']['week']['RU']}, EN: {stats['joined']['week']['EN']})",
            f"За месяц: {stats['joined']['month']['total']} (RU: {stats['joined']['month']['RU']}, EN: {stats['joined']['month']['EN']})",
            f"Более месяца назад: {stats['joined']['more']['total']} (RU: {stats['joined']['more']['RU']}, EN: {stats['joined']['more']['EN']})\n",
            "АКТИВНОСТЬ ПО МЕСЯЦАМ:",
            "-----------------------------------------------------"
        ]
        
        # Добавляем статистику по месяцам
        for month, counts in stats['active']['by_months'].items():
            content.append(f"{month}: {counts['total']} (RU: {counts['RU']}, EN: {counts['EN']})")
            
        content.extend([
            "\nРЕГИСТРАЦИИ ПО МЕСЯЦАМ:",
            "-----------------------------------------------------"
        ])
        
        for month, counts in stats['joined']['by_months'].items():
            content.append(f"{month}: {counts['total']} (RU: {counts['RU']}, EN: {counts['EN']})")
            
        # Добавляем детальную статистику по дням, если доступна
        if language_history and 'daily' in language_history:
            content.extend([
                "\nДЕТАЛЬНАЯ СТАТИСТИКА ПО ДНЯМ (последние 30 дней):",
                "-----------------------------------------------------"
            ])
            
            for day, counts in sorted(language_history['daily'].items()):
                content.append(f"{day}: RU: {counts['RU']}, EN: {counts['EN']}")
    else:  # EN
        content = [
            "=====================================================",
            "              DETAILED BOT STATISTICS              ",
            f"                    {current_date}                   ",
            "=====================================================\n",
            "GENERAL INFORMATION:",
            "-----------------------------------------------------",
            f"Total users: {global_stats['total_users']}",
            f"RU users: {global_stats['ru_users']}",
            f"EN users: {global_stats['en_users']}\n",
            "ACTIVITY STATISTICS (last access):",
            "-----------------------------------------------------",
            f"Today: {stats['active']['today']['total']} (RU: {stats['active']['today']['RU']}, EN: {stats['active']['today']['EN']})",
            f"This week: {stats['active']['week']['total']} (RU: {stats['active']['week']['RU']}, EN: {stats['active']['week']['EN']})",
            f"This month: {stats['active']['month']['total']} (RU: {stats['active']['month']['RU']}, EN: {stats['active']['month']['EN']})",
            f"More than a month: {stats['active']['more']['total']} (RU: {stats['active']['more']['RU']}, EN: {stats['active']['more']['EN']})\n",
            "REGISTRATION STATISTICS:",
            "-----------------------------------------------------",
            f"Today: {stats['joined']['today']['total']} (RU: {stats['joined']['today']['RU']}, EN: {stats['joined']['today']['EN']})",
            f"This week: {stats['joined']['week']['total']} (RU: {stats['joined']['week']['RU']}, EN: {stats['joined']['week']['EN']})",
            f"This month: {stats['joined']['month']['total']} (RU: {stats['joined']['month']['RU']}, EN: {stats['joined']['month']['EN']})",
            f"More than a month ago: {stats['joined']['more']['total']} (RU: {stats['joined']['more']['RU']}, EN: {stats['joined']['more']['EN']})\n",
            "ACTIVITY BY MONTHS:",
            "-----------------------------------------------------"
        ]
        
        # Добавляем статистику по месяцам
        for month, counts in stats['active']['by_months'].items():
            content.append(f"{month}: {counts['total']} (RU: {counts['RU']}, EN: {counts['EN']})")
            
        content.extend([
            "\nREGISTRATIONS BY MONTHS:",
            "-----------------------------------------------------"
        ])
        
        for month, counts in stats['joined']['by_months'].items():
            content.append(f"{month}: {counts['total']} (RU: {counts['RU']}, EN: {counts['EN']})")
            
        # Добавляем детальную статистику по дням, если доступна
        if language_history and 'daily' in language_history:
            content.extend([
                "\nDETAILED STATISTICS BY DAY (last 30 days):",
                "-----------------------------------------------------"
            ])
            
            for day, counts in sorted(language_history['daily'].items()):
                content.append(f"{day}: RU: {counts['RU']}, EN: {counts['EN']}")
    
    # Объединяем все строки в один текст
    file_content = "\n".join(content)
    
    return filename, file_content
