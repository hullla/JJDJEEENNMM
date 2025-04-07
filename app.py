import os
import telebot
from telebot import types

TOKEN = os.getenv('7671924788:AAHCVF8B-PiyNC84gbNdn7i54Ai5eWTLm0s')  # Важно: имя переменной должно совпадать
CHANNEL_ID = os.getenv('-1001948875251')

if not TOKEN or not CHANNEL_ID:
    raise ValueError("Missing environment variables!")

bot = telebot.TeleBot(TOKEN)

def is_user_authorized(user_id):
    try:
        result = bot.search_messages(
            chat_id=CHANNEL_ID,
            query=str(user_id),
            limit=1
        )
        return len(result) > 0
    except Exception as e:
        print(f"Authorization error: {e}")
        return False

@bot.message_handler(commands=['start'])
def start(message):
    user_id = message.from_user.id
    if is_user_authorized(user_id):
        bot.send_message(message.chat.id, "✅ Authorized!")
    else:
        msg = bot.send_message(
            message.chat.id,
            "Choose language:",
            reply_markup=create_lang_keyboard()
        )
        bot.register_next_step_handler(msg, process_lang_step)

def create_lang_keyboard():
    markup = types.ReplyKeyboardMarkup(one_time_keyboard=True)
    markup.add('RU', 'EN')
    return markup

def process_lang_step(message):
    user_id = message.from_user.id
    lang = message.text.upper()
    
    if lang not in ['RU', 'EN']:
        lang = 'EN'
    
    bot.send_message(
        CHANNEL_ID,
        f"User ID: {user_id}\nLanguage: {lang}"
    )
    bot.send_message(
        message.chat.id,
        f"✅ Registration complete! ({lang})"
    )

if __name__ == '__main__':
    print("Bot starting...")
    bot.infinity_polling()