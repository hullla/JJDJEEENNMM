import telebot
from telebot import types
import logging
import time
import requests
from datetime import datetime, timedelta
import json
import os
import statistics

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
        users = statistics.get_users_data(JSONBIN_BIN_ID, JSONBIN_API_KEY, force_update=True)
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
        users = statistics.get_users_data(JSONBIN_BIN_ID, JSONBIN_API_KEY)
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
        users = statistics.get_users_data(JSONBIN_BIN_ID, JSONBIN_API_KEY)
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
        users = statistics.get_users_data(JSONBIN_BIN_ID, JSONBIN_API_KEY)
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
            language = statistics.get_user_language(user_id, JSONBIN_BIN_ID, JSONBIN_API_KEY)
            
            # Инлайн-клавиатура с кнопкой статистики
            markup = types.InlineKeyboardMarkup()
            if language == 'RU':
                stats_button = types.InlineKeyboardButton("📊 Статистика", callback_data='show_stats')
                welcome_text = "Добро пожаловать! Выберите действие:"
            else:  # EN
                stats_button = types.InlineKeyboardButton("📊 Statistics", callback_data='show_stats')
                welcome_text = "Welcome! Choose an action:"
            
            markup.add(stats_button)
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

@bot.message_handler(commands=['stats'])
def stats_command(message):
    """Перенаправляет на общую функцию показа статистики"""
    try:
        user_id = message.from_user.id
        chat_id = message.chat.id
        logger.info(f"Команда /stats от пользователя {user_id}")

        # Проверка авторизации
        if not is_user_authorized(user_id):
            language = 'RU'  # По умолчанию
            response = "Вы не авторизованы. Используйте /start для регистрации." if language == 'RU' else "You are not authorized. Use /start to register."
            bot.send_message(chat_id, response)
            return

        # Показываем статистику
        show_statistics(user_id, chat_id)
    except Exception as e:
        logger.error(f"Ошибка при обработке команды /stats: {e}")
        try:
            bot.send_message(chat_id, "Произошла ошибка при получении статистики. Попробуйте позже.")
        except:
            pass

def show_statistics(user_id, chat_id):
    """Общая функция для отображения статистики"""
    try:
        # Получаем язык пользователя
        language = statistics.get_user_language(user_id, JSONBIN_BIN_ID, JSONBIN_API_KEY)
        
        # Генерируем сообщение со статистикой
        stats_message = statistics.generate_stats_message(user_id, JSONBIN_BIN_ID, JSONBIN_API_KEY)
        
        # Получаем инлайн кнопку для детальной статистики
        markup = statistics.get_detailed_stats_button(language)
        
        # Отправляем сообщение с кнопкой
        bot.send_message(chat_id, stats_message, parse_mode="Markdown", reply_markup=markup)
    except Exception as e:
        logger.error(f"Ошибка при отображении статистики: {e}")
        language = statistics.get_user_language(user_id, JSONBIN_BIN_ID, JSONBIN_API_KEY)
        error_msg = "Произошла ошибка при получении статистики. Попробуйте позже." if language == 'RU' else "An error occurred while retrieving statistics. Please try again later."
        bot.send_message(chat_id, error_msg)

@bot.callback_query_handler(func=lambda call: call.data == 'show_stats')
def show_stats_callback(call):
    """Обработчик для инлайн кнопки статистики"""
    try:
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        logger.info(f"Запрос статистики от пользователя {user_id}")
        
        # Отвечаем на колбэк, чтобы убрать часы загрузки
        bot.answer_callback_query(call.id)
        
        # Показываем статистику
        show_statistics(user_id, chat_id)
    except Exception as e:
        logger.error(f"Ошибка при обработке запроса статистики: {e}")
        try:
            bot.answer_callback_query(call.id, "Произошла ошибка. Попробуйте позже.")
        except:
            pass

@bot.callback_query_handler(func=lambda call: call.data == 'detailed_stats')
def detailed_stats_callback(call):
    """Обработчик для инлайн кнопки детальной статистики"""
    try:
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        logger.info(f"Запрос детальной статистики от пользователя {user_id}")
        
        # Отвечаем на колбэк, чтобы убрать часы загрузки
        bot.answer_callback_query(call.id)
        
        # Получаем язык пользователя
        language = statistics.get_user_language(user_id, JSONBIN_BIN_ID, JSONBIN_API_KEY)
        
        # Получаем детальную статистику
        stats = statistics.get_activity_stats(JSONBIN_BIN_ID, JSONBIN_API_KEY)
        if not stats:
            error_msg = "Не удалось получить детальную статистику." if language == 'RU' else "Failed to retrieve detailed statistics."
            bot.send_message(chat_id, error_msg)
            return
        
        # Сохраняем статистику в файл
        filename = statistics.save_daily_stats_to_file(stats)
        if not filename:
            error_msg = "Не удалось сохранить статистику в файл." if language == 'RU' else "Failed to save statistics to file."
            bot.send_message(chat_id, error_msg)
            return
        
        # Отправляем файл
        with open(filename, 'rb') as f:
            caption = "Детальная статистика за последний месяц" if language == 'RU' else "Detailed statistics for the last month"
            bot.send_document(chat_id, f, caption=caption)
            
        # Удаляем файл после отправки
        try:
            os.remove(filename)
        except:
            pass
    except Exception as e:
        logger.error(f"Ошибка при обработке запроса детальной статистики: {e}")
        try:
            language = statistics.get_user_language(user_id, JSONBIN_BIN_ID, JSONBIN_API_KEY)
            error_msg = "Произошла ошибка при получении детальной статистики. Попробуйте позже." if language == 'RU' else "An error occurred while retrieving detailed statistics. Please try again later."
            bot.send_message(chat_id, error_msg)
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
            # Создаем инлайн-клавиатуру с кнопкой статистики
            markup = types.InlineKeyboardMarkup()
            if language == 'RU':
                stats_button = types.InlineKeyboardButton("📊 Статистика", callback_data='show_stats')
                welcome_text = "Вы уже в системе! Выберите действие:"
            else:  # EN
                stats_button = types.InlineKeyboardButton("📊 Statistics", callback_data='show_stats')
                welcome_text = "You are already registered! Choose an action:"
            
            markup.add(stats_button)
            bot.edit_message_text(welcome_text, call.message.chat.id, call.message.message_id, reply_markup=markup)
            return

        # Регистрация пользователя в JSONBin
        try:
            if register_user(user_id, language):
                # Создаем инлайн-клавиатуру с кнопкой статистики
                markup = types.InlineKeyboardMarkup()
                if language == 'RU':
                    stats_button = types.InlineKeyboardButton("📊 Статистика", callback_data='show_stats')
                    welcome_text = "Регистрация успешна! Выберите действие:"
                else:  # EN
                    stats_button = types.InlineKeyboardButton("📊 Statistics", callback_data='show_stats')
                    welcome_text = "Registration successful! Choose an action:"
                
                markup.add(stats_button)
                bot.edit_message_text(welcome_text, call.message.chat.id, call.message.message_id, reply_markup=markup)
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
        users = statistics.get_users_data(JSONBIN_BIN_ID, JSONBIN_API_KEY, force_update=True)
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
