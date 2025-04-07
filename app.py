import os
import telebot

BOT_TOKEN = os.getenv('7671924788:AAHCVF8B-PiyNC84gbNdn7i54Ai5eWTLm0s')
CHANNEL_ID = os.getenv('-1001948875251')

bot = telebot.TeleBot(BOT_TOKEN)
authorized_users = set()

def load_authorized_users():
    authorized = set()
    try:
        messages = bot.get_chat_history(chat_id=CHANNEL_ID, limit=100)
        for msg in messages:
            if msg.text and msg.text.startswith('User ID: '):
                user_id = int(msg.text.split(', ')[0].split(': ')[1])
                authorized.add(user_id)
    except Exception as e:
        print(f"Error loading authorized users: {e}")
    return authorized

authorized_users = load_authorized_users()

@bot.message_handler(commands=['start'])
def handle_start(message):
    user_id = message.from_user.id
    if user_id not in authorized_users:
        lang = (message.from_user.language_code or 'en')[:2].upper()
        lang = lang if lang in ['RU', 'EN'] else 'EN'
        bot.send_message(CHANNEL_ID, f"User ID: {user_id}, Language: {lang}")
        authorized_users.add(user_id)

@bot.message_handler(func=lambda _: True)
def handle_all_messages(message):
    if message.from_user.id not in authorized_users:
        return

if __name__ == '__main__':
    bot.infinity_polling()