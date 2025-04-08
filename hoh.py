import telebot
from telebot import types
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Инициализация бота
BOT_TOKEN = "7671924788:AAFYL_iYxW99UE2sj90RpK13AjgDJSKtHFo"  # Будет заменено через GitHub Secrets
CHANNEL_ID = "-1001948875251"  # Будет заменено через GitHub Secrets

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
    is_authorized = is_user_authorized(user_id)
    logger.info(f"Пользователь {user_id} авторизован: {is_authorized}")

    if is_authorized:
        bot.send_message(message.chat.id, "Вы уже авторизованы!")
        return

    # Создаем инлайн клавиатуру
    markup = types.InlineKeyboardMarkup(row_width=2)
    ru_button = types.InlineKeyboardButton("RU 🇷🇺", callback_data='lang_ru')
    en_button = types.InlineKeyboardButton("EN 🇬🇧", callback_data='lang_en')
    markup.add(ru_button, en_button)
    
    bot.send_message(message.chat.id, "Выберите язык / Choose language:", reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data.startswith('lang_'))
def language_callback(call):
    user_id = call.from_user.id
    language = call.data.split('_')[1].upper()
    
    # Проверяем авторизацию еще раз
    if is_user_authorized(user_id):
        bot.edit_message_text(chat_id=call.message.chat.id,
                             message_id=call.message.message_id,
                             text="Вы уже авторизованы!" if language == 'RU' else "You're already authorized!",
                             reply_markup=None)
        return

    # Отправляем информацию в канал
    user_info = f"ID: {user_id}\nLanguage: {language}"
    try:
        bot.send_message(CHANNEL_ID, user_info)
        logger.info(f"Информация о пользователе отправлена в канал: {user_info}")
        response_ru = "Вы выбрали русский язык. Ожидайте авторизации администратором."
        response_en = "You've selected English. Please wait for admin authorization."
    except Exception as e:
        logger.error(f"Ошибка при отправке информации в канал: {e}")
        response_ru = "Ошибка отправки данных. Попробуйте позже."
        response_en = "Data sending error. Please try again later."

    # Отвечаем пользователю
    response = response_ru if language == 'RU' else response_en
    bot.edit_message_text(chat_id=call.message.chat.id,
                         message_id=call.message.message_id,
                         text=response,
                         reply_markup=None)

def main():
    logger.info(f"Бот запущен.")
    bot.infinity_polling()

if __name__ == "__main__":
    main()