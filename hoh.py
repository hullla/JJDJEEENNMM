import telebot
from telebot import types
import logging
import time
import datetime
import requests
import json

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Инициализация бота
BOT_TOKEN = "7714752663:AAGV_XL4IfAjZ_o5vqyf29IJlA5scv1BD6c"
CHANNEL_ID = "-1001948875251"
JSONBIN_API_KEY = "$2a$10$s9kk4994hSgcahu7WYiM/uEsPNVF5eCNpeiz6SkThOhKKwhc6yX0W"
JSONBIN_BIN_ID = "$2a$10$C7S.J33A66P0gXo.q0ELpeAbjACmEGCVWc9o3Wv02YMxTVuwRxTRW"

bot = telebot.TeleBot(BOT_TOKEN)

# Кэш базы данных пользователей
user_database_cache = {"users": []}
last_cache_update = 0
CACHE_TIMEOUT = 60  # Время жизни кэша в секундах

def get_user_database(force_update=False):
    """Получает базу данных пользователей из JSONBin или из кэша"""
    global user_database_cache, last_cache_update
    
    # Проверяем нужно ли обновить кэш
    current_time = time.time()
    if not force_update and (current_time - last_cache_update < CACHE_TIMEOUT):
        return user_database_cache
    
    try:
        url = f"https://api.jsonbin.io/v3/b/{JSONBIN_BIN_ID}/latest"
        headers = {
            "X-Master-Key": JSONBIN_API_KEY
        }
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            user_database_cache = response.json()['record']
            last_cache_update = current_time
            return user_database_cache
        else:
            logger.error(f"Не удалось получить данные: {response.text}")
            return user_database_cache
    except Exception as e:
        logger.error(f"Ошибка при получении данных: {e}")
        return user_database_cache

def update_user_database(user_data):
    """Обновляет базу данных пользователей в JSONBin"""
    try:
        database = get_user_database()
        user_exists = False
        
        # Проверяем, есть ли уже пользователь в базе
        for i, user in enumerate(database.get("users", [])):
            if user.get("id") == user_data.get("id"):
                database["users"][i] = user_data
                user_exists = True
                break
        
        # Если пользователя нет, добавляем его
        if not user_exists:
            if "users" not in database:
                database["users"] = []
            database["users"].append(user_data)
        
        # Обновляем локальный кэш
        global user_database_cache
        user_database_cache = database
        
        # Отправляем обновленную базу в JSONBin
        url = f"https://api.jsonbin.io/v3/b/{JSONBIN_BIN_ID}"
        headers = {
            "Content-Type": "application/json",
            "X-Master-Key": JSONBIN_API_KEY
        }
        response = requests.put(url, json=database, headers=headers)
        
        if response.status_code == 200:
            logger.info(f"Данные обновлены для пользователя {user_data.get('id')}")
            return True
        else:
            logger.error(f"Ошибка обновления данных: {response.text}")
            return False
    except Exception as e:
        logger.error(f"Ошибка при обновлении данных: {e}")
        return False

def is_user_authorized(user_id):
    """Проверяет, зарегистрирован ли пользователь в базе, используя кэш"""
    try:
        database = get_user_database()
        for user in database.get("users", []):
            if user.get("id") == user_id:
                return True
        return False
    except Exception as e:
        logger.error(f"Ошибка проверки авторизации: {e}")
        return False

@bot.message_handler(commands=['start'])
def start_command(message):
    user_id = message.from_user.id
    chat_id = message.chat.id

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

    # Проверка авторизации через кэш
    if is_user_authorized(user_id):
        bot.edit_message_text("✅ Вы авторизованы!", chat_id, msg.message_id)
    else:
        markup = types.InlineKeyboardMarkup(row_width=2)
        ru_button = types.InlineKeyboardButton("RU 🇷🇺", callback_data='lang_ru')
        en_button = types.InlineKeyboardButton("EN 🇬🇧", callback_data='lang_en')
        markup.add(ru_button, en_button)
        bot.edit_message_text("Выберите язык / Choose language:", chat_id, msg.message_id, reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data.startswith('lang_'))
def language_callback(call):
    user_id = call.from_user.id
    language = call.data.split('_')[1].upper()

    # Финальная проверка перед отправкой
    if is_user_authorized(user_id):
        response = "🔐 Вы уже в системе!" if language == 'RU' else "🔐 Already registered!"
        bot.edit_message_text(response, call.message.chat.id, call.message.message_id)
        return

    # Сохранение данных в JSONBin
    try:
        current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        user_data = {
            "id": user_id,
            "language": language,
            "registration_time": current_time
        }
        
        success = update_user_database(user_data)
        
        if success:
            # Отправляем уведомление в канал, если нужно
            bot.send_message(CHANNEL_ID, f"ID: {user_id}\nLanguage: {language}\nTime: {current_time}")
            response = "📬 Запрос отправлен!" if language == 'RU' else "📬 Request submitted!"
        else:
            response = "⚠️ Ошибка при сохранении" if language == 'RU' else "⚠️ Error saving data"
    except Exception as e:
        logger.error(f"Ошибка: {e}")
        response = "⚠️ Ошибка связи" if language == 'RU' else "⚠️ Connection error"

    bot.edit_message_text(response, call.message.chat.id, call.message.message_id)

def main():
    # Инициализируем кэш при запуске
    try:
        get_user_database(force_update=True)
        logger.info("База данных пользователей загружена в кэш")
    except Exception as e:
        logger.error(f"Ошибка загрузки базы данных: {e}")
    
    logger.info(f"Бот запущен.")
    bot.infinity_polling()

if __name__ == "__main__":
    main()
