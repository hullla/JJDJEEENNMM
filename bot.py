import telebot
from telebot import types
import logging
import time
import requests
from datetime import datetime, timedelta
import json
import os
# Импортируем функции из statistics.py
from statistics import (
    get_user_stats,
    get_global_stats,
    get_activity_stats,
    generate_detailed_stats_file
)

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
            # Получаем язык пользователя
            user_data = get_user_stats(user_id, get_users_data())
            language = user_data.get('language', 'RU')
            
            # Создаем инлайн-кнопку для статистики
            markup = types.InlineKeyboardMarkup(row_width=1)
            if language == 'RU':
                stats_button = types.InlineKeyboardButton("📊 Статистика", callback_data='statistics')
            else:  # EN
                stats_button = types.InlineKeyboardButton("📊 Statistics", callback_data='statistics')
            markup.add(stats_button)
            
            # Сообщение в зависимости от языка
            if language == 'RU':
                welcome_text = "✅ Вы авторизованы! Используйте кнопку ниже для просмотра статистики."
            else:  # EN
                welcome_text = "✅ You are authorized! Use the button below to view statistics."
            
            bot.edit_message_text(welcome_text, chat_id, msg.message_id, reply_markup=markup)
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

@bot.callback_query_handler(func=lambda call: call.data == 'statistics')
def statistics_callback(call):
    try:
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        logger.info(f"Запрос статистики от пользователя {user_id}")

        # Проверка авторизации
        if not is_user_authorized(user_id):
            bot.answer_callback_query(call.id, "Вы не авторизованы. Используйте /start для регистрации.")
            return

        # Получаем данные пользователя и язык
        users_data = get_users_data()
        user_data = get_user_stats(user_id, users_data)
        language = user_data.get('language', 'RU')

        # Получаем общую статистику
        global_stats = get_global_stats(users_data)
        if not global_stats:
            response_text = "Не удалось получить статистику." if language == 'RU' else "Failed to get statistics."
            bot.send_message(chat_id, response_text)
            return

        # Получаем статистику активности
        activity_stats = get_activity_stats(users_data)
        if not activity_stats:
            response_text = "Не удалось получить статистику активности." if language == 'RU' else "Failed to get activity statistics."
            bot.send_message(chat_id, response_text)
            return

        # Формируем сообщение с основной статистикой
        if language == 'RU':
            message_text = (
                f"📊 *Ваша статистика:*\n"
                f"🆔 ID: `{user_id}`\n"
                f"🌐 Язык: {user_data.get('language')}\n"
                f"📅 Дата регистрации: {user_data.get('registration_time')}\n"
                f"⏱ Последний вход: {user_data.get('last_access')}\n\n"
                f"📈 *Общая статистика:*\n"
                f"👥 Всего пользователей: {global_stats['total_users']}\n"
                f"🇷🇺 Пользователей RU: {global_stats['ru_users']}\n"
                f"🇬🇧 Пользователей EN: {global_stats['en_users']}\n\n"
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
                f"*Статистика стран:*\n"
                f"📱 За 24 часа: RU {activity_stats['countries']['24h']['RU']}, EN {activity_stats['countries']['24h']['EN']}\n"
                f"📆 За неделю: RU {activity_stats['countries']['week']['RU']}, EN {activity_stats['countries']['week']['EN']}\n"
                f"🗓 За месяц: RU {activity_stats['countries']['month']['RU']}, EN {activity_stats['countries']['month']['EN']}"
            )
        else:  # EN
            message_text = (
                f"📊 *Your statistics:*\n"
                f"🆔 ID: `{user_id}`\n"
                f"🌐 Language: {user_data.get('language')}\n"
                f"📅 Registration date: {user_data.get('registration_time')}\n"
                f"⏱ Last access: {user_data.get('last_access')}\n\n"
                f"📈 *Global statistics:*\n"
                f"👥 Total users: {global_stats['total_users']}\n"
                f"🇷🇺 RU users: {global_stats['ru_users']}\n"
                f"🇬🇧 EN users: {global_stats['en_users']}\n\n"
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
                f"*Country statistics:*\n"
                f"📱 Last 24 hours: RU {activity_stats['countries']['24h']['RU']}, EN {activity_stats['countries']['24h']['EN']}\n"
                f"📆 Last week: RU {activity_stats['countries']['week']['RU']}, EN {activity_stats['countries']['week']['EN']}\n"
                f"🗓 Last month: RU {activity_stats['countries']['month']['RU']}, EN {activity_stats['countries']['month']['EN']}"
            )

        # Создаем кнопку для детальной статистики
        markup = types.InlineKeyboardMarkup()
        if language == 'RU':
            detailed_button = types.InlineKeyboardButton("📋 Детальная статистика за месяц", callback_data='detailed_stats')
        else:
            detailed_button = types.InlineKeyboardButton("📋 Detailed monthly statistics", callback_data='detailed_stats')
        markup.add(detailed_button)

        bot.send_message(chat_id, message_text, parse_mode="Markdown", reply_markup=markup)
        bot.answer_callback_query(call.id)
    except Exception as e:
        logger.error(f"Ошибка при обработке запроса статистики: {e}")
        try:
            bot.answer_callback_query(call.id, "Произошла ошибка. Попробуйте позже.")
        except:
            pass

@bot.callback_query_handler(func=lambda call: call.data == 'detailed_stats')
def detailed_stats_callback(call):
    try:
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        logger.info(f"Запрос детальной статистики от пользователя {user_id}")

        # Проверка авторизации
        if not is_user_authorized(user_id):
            bot.answer_callback_query(call.id, "Вы не авторизованы. Используйте /start для регистрации.")
            return

        # Получаем данные пользователя и язык
        user_data = get_user_stats(user_id, get_users_data())
        language = user_data.get('language', 'RU')

        # Генерируем файл со статистикой
        filename = generate_detailed_stats_file()
        if not filename:
            response_text = "Не удалось создать файл статистики." if language == 'RU' else "Failed to create statistics file."
            bot.answer_callback_query(call.id, response_text)
            return

        # Отправляем файл
        with open(filename, 'rb') as file:
            caption = "Детальная статистика по дням за последний месяц" if language == 'RU' else "Detailed statistics by day for the last month"
            bot.send_document(chat_id, file, caption=caption)
        
        # Удаляем временный файл
        try:
            os.remove(filename)
        except:
            pass
            
        bot.answer_callback_query(call.id)
    except Exception as e:
        logger.error(f"Ошибка при обработке запроса детальной статистики: {e}")
        try:
            bot.answer_callback_query(call.id, "Произошла ошибка. Попробуйте позже.")
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
            
            # Добавляем кнопку для просмотра статистики
            markup = types.InlineKeyboardMarkup(row_width=1)
            stats_button_text = "📊 Статистика" if language == 'RU' else "📊 Statistics"
            stats_button = types.InlineKeyboardButton(stats_button_text, callback_data='statistics')
            markup.add(stats_button)
            
            bot.edit_message_text(response, call.message.chat.id, call.message.message_id, reply_markup=markup)
            return

        # Регистрация пользователя в JSONBin
        try:
            if register_user(user_id, language):
                response = "📬 Запрос отправлен!" if language == 'RU' else "📬 Request submitted!"
                
                # Добавляем кнопку для просмотра статистики
                markup = types.InlineKeyboardMarkup(row_width=1)
                stats_button_text = "📊 Статистика" if language == 'RU' else "📊 Statistics"
                stats_button = types.InlineKeyboardButton(stats_button_text, callback_data='statistics')
                markup.add(stats_button)
                
                bot.edit_message_text(response, call.message.chat.id, call.message.message_id, reply_markup=markup)
            else:
                response = "⚠️ Ошибка регистрации" if language == 'RU' else "⚠️ Registration error"
                bot.edit_message_text(response, call.message.chat.id, call.message.message_id)
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
        users = get_users_data(force_update=True)
        logger.info(f"Загружено {len(users) if users else 0} записей")

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
