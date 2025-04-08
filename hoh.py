import telebot
from telebot import types
import logging
import time
import requests
from datetime import datetime

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Инициализация бота
BOT_TOKEN = "7714752663:AAERF4Nj3ICaw0jbv0JyFbCvlut7ZzilTKg"
CHANNEL_ID = "-1001948875251"  # Оставляем для обратной совместимости
JSONBIN_API_KEY = "$2a$10$hT79uCEaJENfQBZ7576aL.upUOtnPqJZX53sWcln0HZib/bgs.8.u"
JSONBIN_BIN_ID = "67f532028a456b796684e974"

bot = telebot.TeleBot(BOT_TOKEN)

# Локальный кэш данных пользователей для минимизации API-запросов
users_cache = None
last_cache_update = 0
CACHE_TTL = 300  # Время жизни кэша в секундах (5 минут)

def get_users_data(force_update=False):
    """Получает данные всех пользователей из JSONBin.io с кэшированием"""
    global users_cache, last_cache_update
    
    current_time = time.time()
    
    # Используем кэш, если он актуален и не требуется принудительное обновление
    if not force_update and users_cache is not None and (current_time - last_cache_update) < CACHE_TTL:
        return users_cache
    
    try:
        url = f"https://api.jsonbin.io/v3/b/{JSONBIN_BIN_ID}"
        headers = {
            "X-Master-Key": JSONBIN_API_KEY
        }
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            users_data = response.json().get('record', {}).get('users', [])
            # Обновляем кэш
            users_cache = users_data
            last_cache_update = current_time
            return users_data
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
        url = f"https://api.jsonbin.io/v3/b/{JSONBIN_BIN_ID}"
        headers = {
            "X-Master-Key": JSONBIN_API_KEY,
            "Content-Type": "application/json"
        }
        data = {"users": users_data}
        response = requests.put(url, json=data, headers=headers)
        
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
    users = get_users_data()
    
    for user in users:
        if user.get('user_id') == user_id:
            return True
    
    return False

def register_user(user_id, language):
    """Регистрирует нового пользователя в JSONBin"""
    users = get_users_data()
    
    # Проверяем, существует ли пользователь
    for user in users:
        if user.get('user_id') == user_id:
            return True  # Пользователь уже зарегистрирован
    
    # Добавляем нового пользователя
    new_user = {
        "user_id": user_id,
        "language": language,
        "registration_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    
    users.append(new_user)
    return update_users_data(users)

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

    # Проверка авторизации через кэш данных JSONBin
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
        logger.error(f"Ошибка: {e}")
        response = "⚠️ Ошибка связи" if language == 'RU' else "⚠️ Connection error"

    bot.edit_message_text(response, call.message.chat.id, call.message.message_id)

def main():
    # При запуске бота, сразу загружаем данные пользователей в кэш
    get_users_data(force_update=True)
    logger.info(f"Бот запущен. Кэш пользователей инициализирован.")
    bot.infinity_polling()

if __name__ == "__main__":
    main()
