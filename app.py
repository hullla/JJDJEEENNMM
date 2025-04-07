import os
import telebot
from telebot import types

TOKEN = os.getenv('7671924788:AAHCVF8B-PiyNC84gbNdn7i54Ai5eWTLm0s')
CHANNEL_ID = os.getenv('-1001948875251')

bot = telebot.TeleBot(TOKEN)

def is_user_authorized(user_id):
    try:
        result = bot.search_messages(CHANNEL_ID, query=str(user_id))
        return len(result) > 0
    except Exception as e:
        print(f"Error checking authorization: {e}")
        return False

@bot.message_handler(commands=['start'])
def start(message):
    user_id = message.from_user.id
    if is_user_authorized(user_id):
        bot.send_message(message.chat.id, "✅ Вы авторизованы!")
    else:
        msg = bot.send_message(message.chat.id, "Выберите язык / Choose language:", 
                             reply_markup=create_lang_keyboard())
        bot.register_next_step_handler(msg, process_lang)

def create_lang_keyboard():
    markup = types.ReplyKeyboardMarkup(one_time_keyboard=True)
    markup.add(types.KeyboardButton('RU'), types.KeyboardButton('EN'))
    return markup

def process_lang(message):
    user_id = message.from_user.id
    lang = message.text.strip().upper()
    if lang not in ['RU', 'EN']:
        lang = 'EN'
    
    bot.send_message(CHANNEL_ID, f"User ID: {user_id}, Language: {lang}")
    bot.send_message(message.chat.id, f"✅ Регистрация завершена! / Registration completed! ({lang})")

if __name__ == '__main__':
    bot.polling(none_stop=True)