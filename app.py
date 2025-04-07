import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
import os
import time

BOT_TOKEN = os.environ['7671924788:AAHCVF8B-PiyNC84gbNdn7i54Ai5eWTLm0s']
CHANNEL_ID = os.environ['-1001948875251']  # Например, '@my_hidden_channel' или '-1001234567890'

bot = telebot.TeleBot(BOT_TOKEN)

@bot.message_handler(commands=['start'])
def start_handler(message):
    user_id = message.from_user.id

    # Получаем историю канала (последние 100 сообщений)
    try:
        updates = bot.get_chat_history(CHANNEL_ID, limit=100)
        authorized = any(str(user_id) in msg.text for msg in updates if msg.text)
    except Exception as e:
        print(f"Ошибка чтения канала: {e}")
        authorized = False

    if authorized:
        bot.send_message(user_id, "Вы уже авторизованы.")
    else:
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("RU", callback_data="lang_RU"))
        markup.add(InlineKeyboardButton("EN", callback_data="lang_EN"))
        bot.send_message(user_id, "Выберите язык:", reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data.startswith("lang_"))
def handle_language_selection(call):
    lang = call.data.split("_")[1]
    user_id = call.from_user.id
    username = call.from_user.username or "NoUsername"

    text = f"ID: {user_id}\nLang: {lang}\nUser: @{username}"
    try:
        bot.send_message(CHANNEL_ID, text)
        bot.send_message(user_id, f"Вы авторизованы как {lang}.")
    except Exception as e:
        bot.send_message(user_id, "Ошибка отправки в канал. Обратитесь к администратору.")

bot.infinity_polling()
