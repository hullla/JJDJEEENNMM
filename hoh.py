import telebot
from telebot import types
import logging
import time
import re

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Инициализация бота
BOT_TOKEN = "7714752663:AAGV_XL4IfAjZ_o5vqyf29IJlA5scv1BD6c"
CHANNEL_ID = "-1001948875251"

bot = telebot.TeleBot(BOT_TOKEN)

def is_user_authorized(user_id):
    """Проверяет наличие ID пользователя в сообщениях канала"""
    try:
        # Получаем последние 100 сообщений из канала
        messages = bot.get_chat_history(chat_id=CHANNEL_ID, limit=100)
        
        # Ищем ID пользователя в тексте сообщений
        for message in messages:
            if message.text and re.search(fr'ID:\s*{user_id}\b', message.text):
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
    
    # Проверка авторизации в реальном времени
    is_authorized = is_user_authorized(user_id)
    
    if is_authorized:
        bot.edit_message_text("✅ Доступ разрешен!", chat_id, msg.message_id)
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
    
    # Проверяем актуальный статус перед отправкой
    if is_user_authorized(user_id):
        response = "🔒 Вы уже авторизованы!" if language == 'RU' else "🔒 You're already authorized!"
        bot.edit_message_text(response, call.message.chat.id, call.message.message_id)
        return
    
    # Отправляем данные в канал
    try:
        user_info = f"ID: {user_id}\nLanguage: {language}"
        bot.send_message(CHANNEL_ID, user_info)
        response = "📩 Запрос отправлен!" if language == 'RU' else "📩 Request sent!"
    except Exception as e:
        logger.error(f"Ошибка отправки: {e}")
        response = "🚫 Ошибка отправки" if language == 'RU' else "🚫 Sending error"
    
    bot.edit_message_text(response, call.message.chat.id, call.message.message_id)

def main():
    logger.info(f"Бот запущен.")
    bot.infinity_polling()

if __name__ == "__main__":
    main()