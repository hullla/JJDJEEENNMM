import telebot
from telebot import types
import logging
import time

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Инициализация бота
BOT_TOKEN = "7671924788:AAHHnq5uD7IzAwdAFRwwzqlKnp-6VPVvCi0"
CHANNEL_ID = "-1001948875251"

bot = telebot.TeleBot(BOT_TOKEN)

def is_user_authorized(user_id):
    """Проверяет, состоит ли пользователь в канале"""
    try:
        member = bot.get_chat_member(chat_id=CHANNEL_ID, user_id=user_id)
        return member.status in ['member', 'administrator', 'creator']
    except Exception as e:
        logger.error(f"Ошибка проверки пользователя {user_id}: {e}")
        return False

@bot.message_handler(commands=['start'])
def start_command(message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    
    # Отправляем анимацию загрузки
    load_emojis = [
        "~(˘▾˘~)",
        "~(˘▾˘~)~(˘▾˘~)",
        "~(˘▾˘~)~(˘▾˘~)~(˘▾˘~)",
        "(◡‿◡✿)(◡‿◡✿)(◡‿◡✿)(◡‿◡✿)",
        "(◕‿↼)(◕‿↼)(◕‿↼)(◕‿↼)(◕‿↼)"
    ]
    
    msg = bot.send_message(chat_id, load_emojis[0])
    
    # Анимация загрузки
    for emoji in load_emojis[1:]:
        time.sleep(0.07)  # Общая длительность анимации ~0.3 сек
        bot.edit_message_text(emoji, chat_id, msg.message_id)
    
    # Проверяем авторизацию
    is_authorized = is_user_authorized(user_id)
    
    if is_authorized:
        bot.edit_message_text("✅ Вы уже авторизованы!", chat_id, msg.message_id)
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
    
    if is_user_authorized(user_id):
        response = "Вы уже авторизованы!" if language == 'RU' else "You're already authorized!"
        bot.edit_message_text(response, call.message.chat.id, call.message.message_id)
        return
    
    user_info = f"ID: {user_id}\nLanguage: {language}"
    try:
        bot.send_message(CHANNEL_ID, user_info)
        response = "✅ Данные отправлены! Ожидайте авторизации." if language == 'RU' else "✅ Data sent! Please wait for authorization."
    except Exception as e:
        logger.error(f"Ошибка отправки: {e}")
        response = "🚫 Ошибка отправки. Попробуйте позже." if language == 'RU' else "🚫 Sending error. Please try again."
    
    bot.edit_message_text(response, call.message.chat.id, call.message.message_id)

def main():
    logger.info(f"Бот запущен.")
    bot.infinity_polling()

if __name__ == "__main__":
    main()