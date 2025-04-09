import telebot
from telebot import types
import logging
import time
import requests
from datetime import datetime, timedelta
import json

# Настройка более подробного логирования
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Инициализация бота
BOT_TOKEN = "7671924788:AAHSVGQ6pK3guB97tLeVEXH2s16YPzJfFP4"
CHANNEL_ID = "-1001948875251"  # Оставляем для обратной совместимости
JSONBIN_API_KEY = "$2a$10$hT79uCEaJENfQBZ7576aL.upUOtnPqJZX53sWcln0HZib/bgs.8.u"
JSONBIN_BIN_ID = "67f532028a456b796684e974"

bot = telebot.TeleBot(BOT_TOKEN)

# Локальный кэш данных пользователей для минимизации API-запросов
users_cache = None
last_cache_update = 0
CACHE_TTL = 7200  # Время жизни кэша в секундах (2 часа)
LAST_ACCESS_COOLDOWN = 21600  # 6 часов в секундах

def initialize_jsonbin():
    """Проверяет и инициализирует структуру в JSONBin, если она отсутствует"""
    try:
        users = get_users_data(force_update=True)
        if users is None:
            # Создаем начальную структуру
            initial_data = {
                "users": [],
                "usage_history": []  # Добавляем историю использования
            }
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
        return users_cache.get('users', [])

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
                    users_data = data
                else:
                    # Если структура неправильная, инициализируем ее
                    logger.warning("Неверная структура данных, инициализируем...")
                    users_data = {"users": [], "usage_history": []}
                    initialize_jsonbin()

                # Обновляем кэш
                users_cache = users_data
                last_cache_update = current_time
                logger.debug(f"Данные пользователей обновлены: {len(users_data.get('users', []))} записей")
                return users_data.get('users', [])
            except json.JSONDecodeError:
                logger.error(f"Ошибка декодирования JSON: {response.text}")
                return users_cache.get('users', []) if users_cache else []
        else:
            logger.error(f"Ошибка получения данных из JSONBin: {response.status_code}, {response.text}")
            return users_cache.get('users', []) if users_cache else []
    except Exception as e:
        logger.error(f"Ошибка при получении данных из JSONBin: {e}")
        return users_cache.get('users', []) if users_cache else []

def get_full_data():
    """Получает все данные из JSONBin.io с кэшированием"""
    global users_cache, last_cache_update

    current_time = time.time()

    # Используем кэш, если он актуален
    if users_cache is not None and (current_time - last_cache_update) < CACHE_TTL:
        return users_cache

    try:
        logger.debug("Запрашиваем полные данные из JSONBin...")
        url = f"https://api.jsonbin.io/v3/b/{JSONBIN_BIN_ID}"
        headers = {
            "X-Master-Key": JSONBIN_API_KEY,
            "X-Bin-Meta": "false"
        }
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            try:
                data = response.json()
                if isinstance(data, dict):
                    if 'users' not in data:
                        data['users'] = []
                    if 'usage_history' not in data:
                        data['usage_history'] = []
                else:
                    data = {"users": [], "usage_history": []}
                
                # Обновляем кэш
                users_cache = data
                last_cache_update = current_time
                return data
            except json.JSONDecodeError:
                logger.error(f"Ошибка декодирования JSON: {response.text}")
                return users_cache or {"users": [], "usage_history": []}
        else:
            logger.error(f"Ошибка получения данных из JSONBin: {response.status_code}")
            return users_cache or {"users": [], "usage_history": []}
    except Exception as e:
        logger.error(f"Ошибка при получении данных из JSONBin: {e}")
        return users_cache or {"users": [], "usage_history": []}

def update_full_data(data):
    """Обновляет все данные в JSONBin.io и кэш"""
    global users_cache, last_cache_update

    try:
        logger.debug(f"Обновляем полные данные: {len(data.get('users', []))} пользователей, {len(data.get('usage_history', []))} записей использования")
        url = f"https://api.jsonbin.io/v3/b/{JSONBIN_BIN_ID}"
        headers = {
            "X-Master-Key": JSONBIN_API_KEY,
            "Content-Type": "application/json"
        }
        response = requests.put(url, json=data, headers=headers)
        logger.debug(f"Ответ на обновление данных: {response.status_code}")

        if response.status_code == 200:
            # Обновляем кэш после успешного обновления в JSONBin
            users_cache = data
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
                # Обновляем время использования бота
                record_bot_usage(user_id)
                
                logger.debug(f"Пользователь {user_id} найден в базе")
                return True

        logger.debug(f"Пользователь {user_id} не найден в базе")
        return False
    except Exception as e:
        logger.error(f"Ошибка при проверке авторизации: {e}")
        return False

def record_bot_usage(user_id):
    """Записывает использование бота пользователем"""
    try:
        data = get_full_data()
        now = datetime.now()
        
        # Проверяем, нужно ли обновлять last_access пользователя
        update_needed = True
        for user in data.get('users', []):
            if user.get('user_id') == user_id:
                # Проверяем, прошло ли 6 часов с последнего обновления
                if 'last_access' in user:
                    try:
                        last_access_time = datetime.strptime(user['last_access'], "%Y-%m-%d %H:%M:%S")
                        if now - last_access_time < timedelta(seconds=LAST_ACCESS_COOLDOWN):
                            update_needed = False
                    except ValueError:
                        # Если формат даты некорректный, все равно обновляем
                        pass
                
                if update_needed:
                    user['last_access'] = now.strftime("%Y-%m-%d %H:%M:%S")
                break
        
        # Записываем использование в историю
        usage_entry = {
            "user_id": user_id,
            "timestamp": now.strftime("%Y-%m-%d %H:%M:%S")
        }
        
        if 'usage_history' not in data:
            data['usage_history'] = []
        
        data['usage_history'].append(usage_entry)
        
        # Обновляем данные, только если изменились
        if update_needed or len(data.get('usage_history', [])) > 0:
            update_full_data(data)
            
        return True
    except Exception as e:
        logger.error(f"Ошибка при записи использования бота: {e}")
        return False

def register_user(user_id, language):
    """Регистрирует нового пользователя в JSONBin"""
    try:
        data = get_full_data()
        users = data.get('users', [])
        
        # Проверяем, существует ли пользователь
        for user in users:
            if isinstance(user, dict) and user.get('user_id') == user_id:
                logger.debug(f"Пользователь {user_id} уже зарегистрирован")
                # Записываем использование бота
                record_bot_usage(user_id)
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
        data['users'] = users
        
        # Также добавляем запись в историю использования
        usage_entry = {
            "user_id": user_id,
            "timestamp": now
        }
        
        if 'usage_history' not in data:
            data['usage_history'] = []
        
        data['usage_history'].append(usage_entry)
        
        result = update_full_data(data)
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

def get_registration_stats():
    """Получает статистику регистраций за последние день, неделю, месяц"""
    try:
        users = get_users_data()
        now = datetime.now()
        
        day_count = 0
        week_count = 0
        month_count = 0
        
        for user in users:
            if 'registration_time' in user:
                try:
                    reg_time = datetime.strptime(user['registration_time'], "%Y-%m-%d %H:%M:%S")
                    
                    # Проверяем, попадает ли в интервалы
                    if now - reg_time <= timedelta(days=1):  # 24 часа
                        day_count += 1
                    
                    if now - reg_time <= timedelta(days=7):  # неделя
                        week_count += 1
                    
                    if now - reg_time <= timedelta(days=30):  # месяц
                        month_count += 1
                        
                except ValueError:
                    # Пропускаем, если формат даты некорректный
                    continue
                    
        return {
            "day": day_count,
            "week": week_count,
            "month": month_count
        }
    except Exception as e:
        logger.error(f"Ошибка при получении статистики регистраций: {e}")
        return {"day": 0, "week": 0, "month": 0}

def get_usage_stats():
    """Получает статистику использования за последние 24 часа, 3 дня, неделю, месяц"""
    try:
        data = get_full_data()
        usage_history = data.get('usage_history', [])
        now = datetime.now()
        
        # Множество для хранения уникальных пользователей за каждый период
        day_users = set()
        three_day_users = set()
        week_users = set()
        month_users = set()
        
        for entry in usage_history:
            if 'timestamp' in entry and 'user_id' in entry:
                try:
                    use_time = datetime.strptime(entry['timestamp'], "%Y-%m-%d %H:%M:%S")
                    user_id = entry['user_id']
                    
                    # Проверяем, попадает ли в интервалы
                    if now - use_time <= timedelta(days=1):  # 24 часа
                        day_users.add(user_id)
                    
                    if now - use_time <= timedelta(days=3):  # 3 дня
                        three_day_users.add(user_id)
                    
                    if now - use_time <= timedelta(days=7):  # неделя
                        week_users.add(user_id)
                    
                    if now - use_time <= timedelta(days=30):  # месяц
                        month_users.add(user_id)
                        
                except (ValueError, TypeError):
                    # Пропускаем, если формат даты некорректный или user_id не верен
                    continue
                    
        return {
            "day": len(day_users),
            "three_days": len(three_day_users),
            "week": len(week_users),
            "month": len(month_users)
        }
    except Exception as e:
        logger.error(f"Ошибка при получении статистики использования: {e}")
        return {"day": 0, "three_days": 0, "week": 0, "month": 0}

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

        # Получаем статистику регистраций
        reg_stats = get_registration_stats()
        
        # Получаем статистику использования
        usage_stats = get_usage_stats()
        
        return {
            "total_users": total_users,
            "ru_users": ru_users,
            "en_users": en_users,
            "registrations": reg_stats,
            "usage": usage_stats
        }
    except Exception as e:
        logger.error(f"Ошибка при получении общей статистики: {e}")
        return None

@bot.message_handler(commands=['start'])
def start_command(message):
    try:
        user_id = message.from_user.id
        chat_id = message.chat.id
        logger.info(f"Команда /start от пользователя {user_id}")

        # Анимация загрузки
        load_emojis = [
            "~(˘▾˘~)",
            "~(˘▾˘~)~(˘▾˘~)",
            "~(˘▾˘~)~(˘▾˘~)~(˘▾˘~)",
            "(◡‿◡✿)(◡‿◡✿)(◡‿◡✿)(◡‿◡✿)",
            "(◕‿↼)(◕‿↼)(◕‿↼)(◕‿↼)(◕‿↼)"
        ]

        msg = bot.send_message(chat_id, load_emojis[0])

        # Проигрываем анимацию
        for emoji in load_emojis[1:]:
            time.sleep(0.07)
            bot.edit_message_text(emoji, chat_id, msg.message_id)

        # Проверка авторизации через кэш данных JSONBin
        authorized = is_user_authorized(user_id)
        logger.debug(f"Результат проверки авторизации: {authorized}")

        if authorized:
            bot.edit_message_text("✅ Вы авторизованы!", chat_id, msg.message_id)
        else:
            markup = types.InlineKeyboardMarkup(row_width=2)
            ru_button = types.InlineKeyboardButton("RU 🇷🇺", callback_data='lang_ru')
            en_button = types.InlineKeyboardButton("EN 🇬🇧", callback_data='lang_en')
            markup.add(ru_button, en_button)
            bot.edit_message_text("Выберите язык / Choose language:", chat_id, msg.message_id, reply_markup=markup)
    except Exception as e:
        logger.error(f"Ошибка при обработке команды /start: {e}")
        try:
            bot.send_message(chat_id, "Произошла ошибка. Попробуйте позже.")
        except:
            pass

@bot.message_handler(commands=['stats'])
def stats_command(message):
    try:
        user_id = message.from_user.id
        chat_id = message.chat.id
        logger.info(f"Команда /stats от пользователя {user_id}")

        # Проверка авторизации
        if not is_user_authorized(user_id):
            bot.send_message(chat_id, "Вы не авторизованы. Используйте /start для регистрации.")
            return

        # Получаем статистику пользователя
        user_stats = get_user_stats(user_id)
        if not user_stats:
            bot.send_message(chat_id, "Не удалось получить вашу статистику.")
            return

        # Получаем общую статистику
        global_stats = get_global_stats()
        if not global_stats:
            bot.send_message(chat_id, "Не удалось получить общую статистику.")
            return

        # Формируем сообщение в зависимости от языка пользователя
        if user_stats.get('language') == 'RU':
            message_text = (
                f"📊 *Ваша статистика:*\n"
                f"🆔 ID: `{user_id}`\n"
                f"🌐 Язык: {user_stats.get('language')}\n"
                f"📅 Дата регистрации: {user_stats.get('registration_time')}\n"
                f"⏱ Последний вход: {user_stats.get('last_access')}\n\n"
                f"📈 *Общая статистика:*\n"
                f"👥 Всего пользователей: {global_stats['total_users']}\n"
                f"🇷🇺 Пользователей RU: {global_stats['ru_users']}\n"
                f"🇬🇧 Пользователей EN: {global_stats['en_users']}\n\n"
                f"📊 *Регистрации:*\n"
                f"📆 За 24 часа: {global_stats['registrations']['day']}\n"
                f"📆 За неделю: {global_stats['registrations']['week']}\n"
                f"📆 За месяц: {global_stats['registrations']['month']}\n\n"
                f"🔄 *Активность пользователей:*\n"
                f"📊 За 24 часа: {global_stats['usage']['day']}\n"
                f"📊 За 3 дня: {global_stats['usage']['three_days']}\n"
                f"📊 За неделю: {global_stats['usage']['week']}\n"
                f"📊 За месяц: {global_stats['usage']['month']}"
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
                f"🇬🇧 EN users: {global_stats['en_users']}\n\n"
                f"📊 *Registrations:*\n"
                f"📆 Last 24 hours: {global_stats['registrations']['day']}\n"
                f"📆 Last week: {global_stats['registrations']['week']}\n"
                f"📆 Last month: {global_stats['registrations']['month']}\n\n"
                f"🔄 *User activity:*\n"
                f"📊 Last 24 hours: {global_stats['usage']['day']}\n"
                f"📊 Last 3 days: {global_stats['usage']['three_days']}\n"
                f"📊 Last week: {global_stats['usage']['week']}\n"
                f"📊 Last month: {global_stats['usage']['month']}"
            )

        bot.send_message(chat_id, message_text, parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Ошибка при обработке команды /stats: {e}")
        try:
            bot.send_message(chat_id, "Произошла ошибка при получении статистики. Попробуйте позже.")
        except:
            pass

@bot.callback_query_handler(func=lambda call: call.data.startswith('lang_'))
def language_callback(call):
    try:
        user_id = call.from_user.id
        language = call.data.split('_')[1].upper()
        logger.info(f"Выбор языка от пользователя {user_id}: {language}")

        # Финальная проверка перед регистрацией
        if is_user_authorized(user_id):
            response = "🔐 Вы уже в системе!" if language == 'RU' else "🔐 Already registered!"
            bot.edit_message_text(response, call.message.chat.id, call.message.message_id)
            return

        # Регистрация пользователя в JSONBin
        try:
            if register_user(user_id, language):
                response = "📬 Запрос отправлен!" if language == 'RU' else "📬 Request submitted!"
            else:
                response = "⚠️ Ошибка регистрации" if language == 'RU' else "⚠️ Registration error"
        except Exception as e:
            logger.error(f"Ошибка регистрации: {e}")
            response = "⚠️ Ошибка связи" if language == 'RU' else "⚠️ Connection error"

        bot.edit_message_text(response, call.message.chat.id, call.message.message_id)
    except Exception as e:
        logger.error(f"Ошибка при обработке выбора языка: {e}")
        try:
            bot.answer_callback_query(call.id, "Произошла ошибка. Попробуйте позже.")
        except:
            pass

def main():
    try:
        # Проверяем доступность JSONBin и инициализируем структуру при необходимости
        logger.info("Инициализация JSONBin структуры...")
        success = initialize_jsonbin()
        if not success:
            logger.error("Не удалось инициализировать JSONBin структуру!")

        # При запуске бота, сразу загружаем данные пользователей в кэш
        logger.info("Загрузка данных пользователей в кэш...")
        data = get_full_data()
        users = data.get('users', [])
        logger.info(f"Загружено {len(users)} записей пользователей и {len(data.get('usage_history', []))} записей использования")

        logger.info("Бот запущен. Кэш пользователей инициализирован.")

        # Добавляем тестовый запрос к API Telegram для проверки токена
        test_response = requests.get(f"https://api.telegram.org/bot{BOT_TOKEN}/getMe")
        if test_response.status_code == 200:
            bot_info = test_response.json()
            logger.info(f"Бот подключен: @{bot_info['result']['username']}")
        else:
            logger.error(f"Ошибка подключения к Telegram API: {test_response.status_code}")

        bot.infinity_polling()
    except Exception as e:
        logger.critical(f"Критическая ошибка при запуске бота: {e}")

if __name__ == "__main__":
    main()