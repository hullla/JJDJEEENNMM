import logging
import time
import requests
from datetime import datetime, timedelta
import json
import telebot
from telebot import types

# Настройка более подробного логирования
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Константы для JSONBin.io
JSONBIN_API_KEY = "$2a$10$hT79uCEaJENfQBZ7576aL.upUOtnPqJZX53sWcln0HZib/bgs.8.u"
JSONBIN_BIN_ID = "67f532028a456b796684e974"

# Локальный кэш данных пользователей для минимизации API-запросов
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

def get_user_language(user_id):
    """Получает язык пользователя"""
    user_stats = get_user_stats(user_id)
    return user_stats.get('language', 'RU') if user_stats else 'RU'

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

def get_activity_stats():
    """Получает статистику пользователей по времени активности и регистрации"""
    try:
        users = get_users_data()
        if users is None:
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
        
        # Словари для хранения статистики по языкам
        language_stats = {
            "today": {"RU": 0, "EN": 0},
            "week": {"RU": 0, "EN": 0},
            "month": {"RU": 0, "EN": 0}
        }

        # Статистика по активности по дням за последний месяц
        daily_stats = {}
        month_ago = current_time - timedelta(days=30)
        
        for user in users:
            if isinstance(user, dict):
                language = user.get('language', 'UNKNOWN')
                
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
                        
                        # Статистика по дням за последний месяц
                        if last_access >= month_ago:
                            day_key = last_access.strftime("%Y-%m-%d")
                            if day_key not in daily_stats:
                                daily_stats[day_key] = {"total": 0, "RU": 0, "EN": 0}
                            daily_stats[day_key]["total"] += 1
                            if language in ["RU", "EN"]:
                                daily_stats[day_key][language] += 1

                        # Группировка по периодам
                        if days_diff < 1:  # Сегодня
                            active_today += 1
                            if language in ["RU", "EN"]:
                                language_stats["today"][language] += 1
                        elif days_diff < 7:  # Неделя
                            active_week += 1
                            if language in ["RU", "EN"]:
                                language_stats["week"][language] += 1
                        elif days_diff < 30:  # Месяц
                            active_month += 1
                            if language in ["RU", "EN"]:
                                language_stats["month"][language] += 1
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
        
        # Сортируем статистику по дням
        daily_stats = dict(sorted(daily_stats.items(), reverse=True))  # Сортировка от новых к старым

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
            "daily_stats": daily_stats,
            "total_users": len(users)
        }
    except Exception as e:
        logger.error(f"Ошибка при получении статистики активности: {e}")
        return None

def generate_detailed_stats_file():
    """Генерирует файл с детальной статистикой по дням за последний месяц"""
    try:
        stats = get_activity_stats()
        if not stats or 'daily_stats' not in stats:
            return None
        
        daily_stats = stats['daily_stats']
        current_date = datetime.now().strftime("%Y-%m-%d")
        filename = f"{current_date}.txt"
        
        content = []
        
        # Заголовок
        content.append(f"========== DETAILED STATISTICS ==========")
        content.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        content.append(f"Total Users: {stats['total_users']}")
        content.append("")
        
        # Статистика по дням
        content.append("========== DAILY ACTIVITY STATISTICS ==========")
        for day, day_stats in daily_stats.items():
            content.append(f"Date: {day}")
            content.append(f"  Total Active: {day_stats['total']}")
            content.append(f"  RU Users: {day_stats['RU']}")
            content.append(f"  EN Users: {day_stats['EN']}")
            content.append("")
        
        # Статистика по активности
        content.append("========== ACTIVITY PERIODS ==========")
        content.append(f"Active Today: {stats['active']['today']}")
        content.append(f"Active This Week: {stats['active']['week']}")
        content.append(f"Active This Month: {stats['active']['month']}")
        content.append(f"Inactive (>30 days): {stats['active']['more']}")
        content.append("")
        
        # Статистика по языкам
        content.append("========== LANGUAGE STATISTICS ==========")
        content.append("Today:")
        content.append(f"  RU: {stats['language_stats']['today']['RU']}")
        content.append(f"  EN: {stats['language_stats']['today']['EN']}")
        content.append("This Week:")
        content.append(f"  RU: {stats['language_stats']['week']['RU']}")
        content.append(f"  EN: {stats['language_stats']['week']['EN']}")
        content.append("This Month:")
        content.append(f"  RU: {stats['language_stats']['month']['RU']}")
        content.append(f"  EN: {stats['language_stats']['month']['EN']}")
        content.append("")
        
        # Статистика по регистрациям
        content.append("========== REGISTRATION STATISTICS ==========")
        content.append(f"Registered Today: {stats['joined']['today']}")
        content.append(f"Registered This Week: {stats['joined']['week']}")
        content.append(f"Registered This Month: {stats['joined']['month']}")
        content.append(f"Registered Earlier: {stats['joined']['more']}")
        
        # Объединяем всё в один файл
        return {
            "filename": filename,
            "content": "\n".join(content)
        }
    except Exception as e:
        logger.error(f"Ошибка при генерации файла статистики: {e}")
        return None

# Функции обработки сообщений бота для статистики
def register_statistics_handlers(bot):
    @bot.callback_query_handler(func=lambda call: call.data == 'statistics_menu')
    def statistics_menu_callback(call):
        try:
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            
            # Проверка авторизации
            if not is_user_authorized(user_id):
                bot.answer_callback_query(call.id, "Вы не авторизованы. Используйте /start для регистрации.")
                return
            
            # Получаем язык пользователя
            language = get_user_language(user_id)
            
            # Создаем клавиатуру в зависимости от языка
            markup = types.InlineKeyboardMarkup(row_width=1)
            
            if language == 'RU':
                my_stats_btn = types.InlineKeyboardButton("📊 Моя статистика", callback_data='my_stats')
                activity_stats_btn = types.InlineKeyboardButton("📈 Статистика активности", callback_data='activity_stats')
                detailed_stats_btn = types.InlineKeyboardButton("📋 Детальная статистика", callback_data='detailed_stats')
                back_btn = types.InlineKeyboardButton("🔙 Назад", callback_data='back_to_start')
                title = "📊 *Меню статистики*\n\nВыберите тип статистики для просмотра:"
            else:  # EN
                my_stats_btn = types.InlineKeyboardButton("📊 My Statistics", callback_data='my_stats')
                activity_stats_btn = types.InlineKeyboardButton("📈 Activity Statistics", callback_data='activity_stats')
                detailed_stats_btn = types.InlineKeyboardButton("📋 Detailed Statistics", callback_data='detailed_stats')
                back_btn = types.InlineKeyboardButton("🔙 Back", callback_data='back_to_start')
                title = "📊 *Statistics Menu*\n\nSelect statistics type to view:"
            
            markup.add(my_stats_btn, activity_stats_btn, detailed_stats_btn, back_btn)
            
            bot.edit_message_text(
                chat_id=chat_id,
                message_id=call.message.message_id,
                text=title,
                reply_markup=markup,
                parse_mode="Markdown"
            )
            
        except Exception as e:
            logger.error(f"Ошибка при обработке меню статистики: {e}")
            try:
                bot.answer_callback_query(call.id, "Произошла ошибка. Попробуйте позже.")
            except:
                pass
    
    @bot.callback_query_handler(func=lambda call: call.data == 'my_stats')
    def my_stats_callback(call):
        try:
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            
            # Проверка авторизации
            if not is_user_authorized(user_id):
                bot.answer_callback_query(call.id, "Вы не авторизованы. Используйте /start для регистрации.")
                return
            
            # Получаем статистику пользователя
            user_stats = get_user_stats(user_id)
            if not user_stats:
                bot.answer_callback_query(call.id, "Не удалось получить вашу статистику.")
                return
            
            # Получаем общую статистику
            global_stats = get_global_stats()
            if not global_stats:
                bot.answer_callback_query(call.id, "Не удалось получить общую статистику.")
                return
            
            # Создаем клавиатуру для возврата
            markup = types.InlineKeyboardMarkup(row_width=1)
            if user_stats.get('language') == 'RU':
                back_btn = types.InlineKeyboardButton("🔙 Вернуться в меню статистики", callback_data='statistics_menu')
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
                back_btn = types.InlineKeyboardButton("🔙 Back to Statistics Menu", callback_data='statistics_menu')
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
            
            markup.add(back_btn)
            
            bot.edit_message_text(
                chat_id=chat_id,
                message_id=call.message.message_id,
                text=message_text,
                reply_markup=markup,
                parse_mode="Markdown"
            )
            
        except Exception as e:
            logger.error(f"Ошибка при получении статистики пользователя: {e}")
            try:
                bot.answer_callback_query(call.id, "Произошла ошибка. Попробуйте позже.")
            except:
                pass
    
    @bot.callback_query_handler(func=lambda call: call.data == 'activity_stats')
    def activity_stats_callback(call):
        try:
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            
            # Проверка авторизации
            if not is_user_authorized(user_id):
                bot.answer_callback_query(call.id, "Вы не авторизованы. Используйте /start для регистрации.")
                return
            
            # Получаем статистику активности
            stats = get_activity_stats()
            if not stats:
                bot.answer_callback_query(call.id, "Не удалось получить статистику активности.")
                return
            
            # Получаем предпочтительный язык пользователя
            language = get_user_language(user_id)
            
            # Создаем клавиатуру для возврата
            markup = types.InlineKeyboardMarkup(row_width=1)
            
            if language == 'RU':
                back_btn = types.InlineKeyboardButton("🔙 Вернуться в меню статистики", callback_data='statistics_menu')
                
                activity_stats_text = (
                    f"📊 *Статистика активности пользователей:*\n\n"
                    f"👥 *Всего пользователей:* {stats['total_users']}\n\n"
                    f"*Активность (последний заход):*\n"
                    f"🔹 Сегодня: {stats['active']['today']}\n"
                    f"🔹 За неделю: {stats['active']['week']}\n"
                    f"🔹 За месяц: {stats['active']['month']}\n"
                    f"🔹 Более месяца: {stats['active']['more']}\n\n"
                    f"*Регистрация новых пользователей:*\n"
                    f"🔸 Сегодня: {stats['joined']['today']}\n"
                    f"🔸 За неделю: {stats['joined']['week']}\n"
                    f"🔸 За месяц: {stats['joined']['month']}\n"
                    f"🔸 Более месяца назад: {stats['joined']['more']}\n\n"
                    f"*Статистика по странам:*\n"
                    f"🇷🇺 RU сегодня: {stats['language_stats']['today']['RU']}\n"
                    f"🇬🇧 EN сегодня: {stats['language_stats']['today']['EN']}\n"
                    f"🇷🇺 RU за неделю: {stats['language_stats']['week']['RU']}\n"
                    f"🇬🇧 EN за неделю: {stats['language_stats']['week']['EN']}\n"
                    f"🇷🇺 RU за месяц: {stats['language_stats']['month']['RU']}\n"
                    f"🇬🇧 EN за месяц: {stats['language_stats']['month']['EN']}\n\n"
                )

                # Добавляем статистику по месяцам для последнего захода
                activity_stats_text += "*Распределение по месяцам (последний заход):*\n"
                for month, count in stats['active']['by_months'].items():
                    activity_stats_text += f"📅 {month}: {count}\n"

                activity_stats_text += "\n*Распределение по месяцам (регистрация):*\n"
                for month, count in stats['joined']['by_months'].items():
                    activity_stats_text += f"📅 {month}: {count}\n"
            else:  # EN
                back_btn = types.InlineKeyboardButton("🔙 Back to Statistics Menu", callback_data='statistics_menu')
                
                activity_stats_text = (
                    f"📊 *User Activity Statistics:*\n\n"
                    f"👥 *Total users:* {stats['total_users']}\n\n"
                    f"*Activity (last access):*\n"
                    f"🔹 Today: {stats['active']['today']}\n"
                    f"🔹 This week: {stats['active']['week']}\n"
                    f"🔹 This month: {stats['active']['month']}\n"
                    f"🔹 More than a month: {stats['active']['more']}\n\n"
                    f"*New user registrations:*\n"
                    f"🔸 Today: {stats['joined']['today']}\n"
                    f"🔸 This week: {stats['joined']['week']}\n"
                    f"🔸 This month: {stats['joined']['month']}\n"
                    f"🔸 More than a month ago: {stats['joined']['more']}\n\n"
                    f"*Country Statistics:*\n"
                    f"🇷🇺 RU today: {stats['language_stats']['today']['RU']}\n"
                    f"🇬🇧 EN today: {stats['language_stats']['today']['EN']}\n"
                    f"🇷🇺 RU this week: {stats['language_stats']['week']['RU']}\n"
                    f"🇬🇧 EN this week: {stats['language_stats']['week']['EN']}\n"
                    f"🇷🇺 RU this month: {stats['language_stats']['month']['RU']}\n"
                    f"🇬🇧 EN this month: {stats['language_stats']['month']['EN']}\n\n"
                )

                # Добавляем статистику по месяцам для последнего захода
                activity_stats_text += "*Distribution by months (last access):*\n"
                for month, count in stats['active']['by_months'].items():
                    activity_stats_text += f"📅 {month}: {count}\n"

                activity_stats_text += "\n*Distribution by months (registration):*\n"
                for month, count in stats['joined']['by_months'].items():
                    activity_stats_text += f"📅 {month}: {count}\n"
            
            markup.add(back_btn)
            
            bot.edit_message_text(
                chat_id=chat_id,
                message_id=call.message.message_id,
                text=activity_stats_text,
                reply_markup=markup,
                parse_mode="Markdown"
            )
            
        except Exception as e:
            logger.error(f"Ошибка при получении статистики активности: {e}")
            try:
                bot.answer_callback_query(call.id, "Произошла ошибка. Попробуйте позже.")
            except:
                pass
    
    @bot.callback_query_handler(func=lambda call: call.data == 'detailed_stats')
    def detailed_stats_callback(call):
        try:
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            
            # Проверка авторизации
            if not is_user_authorized(user_id):
                bot.answer_callback_query(call.id, "Вы не авторизованы. Используйте /start для регистрации.")
                return
            
            # Получаем язык пользователя
            language = get_user_language(user_id)
            
            # Генерируем файл со статистикой
            stats_file = generate_detailed_stats_file()
            if not stats_file:
                bot.answer_callback_query(call.id, "Не удалось сгенерировать детальную статистику.")
                return
            
            # Создаем клавиатуру для возврата
            markup = types.InlineKeyboardMarkup(row_width=1)
            
            if language == 'RU':
                back_btn = types.InlineKeyboardButton("🔙 Вернуться в меню статистики", callback_data='statistics_menu')
                message_text = (
                    f"📋 *Детальная статистика*\n\n"
                    f"Файл: `{stats_file['filename']}`\n\n"
                    f"Статистика по дням за последний месяц:"
                )
            else:  # EN
                back_btn = types.InlineKeyboardButton("🔙 Back to Statistics Menu", callback_data='statistics_menu')
                message_text = (
                    f"📋 *Detailed Statistics*\n\n"
                    f"File: `{stats_file['filename']}`\n\n"
                    f"Daily statistics for the last month:"
                )
            
            markup.add(back_btn)
            
            bot.edit_message_text(
                chat_id=chat_id,
                message_id=call.message.message_id,
                text=message_text,
                reply_markup=markup,
                parse_mode="Markdown"
            )
            
            # Отправляем файл отдельным сообщением
            from io import BytesIO
            stats_io = BytesIO(stats_file['content'].encode('utf-8'))
            stats_io.name = stats_file['filename']
            bot.send_document(chat_id, stats_io)
            
        except Exception as e:
            logger.error(f"Ошибка при получении детальной статистики: {e}")
            try:
                bot.answer_callback_query(call.id, "Произошла ошибка. Попробуйте позже.")
            except:
                pass
    
    @bot.callback_query_handler(func=lambda call: call.data == 'back_to_start')
    def back_to_start_callback(call):
        try:
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            
            # Проверка авторизации
            if not is_user_authorized(user_id):
                bot.answer_callback_query(call.id, "Вы не авторизованы. Используйте /start для регистрации.")
                return
            
            # Получаем язык пользователя
            language = get_user_language(user_id)
            
            # Создаем клавиатуру для статистики
            markup = types.InlineKeyboardMarkup(row_width=1)
            if language == 'RU':
                stats_btn = types.InlineKeyboardButton("📊 Статистика", callback_data='statistics_menu')
                message_text = "✅ Вы авторизованы!"
            else:  # EN
                stats_btn = types.InlineKeyboardButton("📊 Statistics", callback_data='statistics_menu')
                message_text = "✅ You are authorized!"
            
            markup.add(stats_btn)
            
            bot.edit_message_text(
                chat_id=chat_id,
                message_id=call.message.message_id,
                text=message_text,
                reply_markup=markup
            )
            
        except Exception as e:
            logger.error(f"Ошибка при возврате на начальный экран: {e}")
            try:
                bot.answer_callback_query(call.id, "Произошла ошибка. Попробуйте позже.")
            except:
                pass
