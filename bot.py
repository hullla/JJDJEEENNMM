import telebot
from telebot import types
import logging
import re
import requests
import time
import threading

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Инициализация бота
BOT_TOKEN = "7671924788:AAEEDhZ7xBXUXESOHEq-hLsetlCSjK3hMY0"
CHANNEL_ID = "-1001948875251"

bot = telebot.TeleBot(BOT_TOKEN)

# Кэш авторизованных пользователей
authorized_users = set()

def load_authorized_users():
    """Загружает список авторизованных пользователей из канала"""
    global authorized_users
    temp_authorized_users = set()
    
    try:
        # Получаем последние обновления, которые могут содержать сообщения из канала
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates?limit=100&allowed_updates=[\"channel_post\"]"
        response = requests.get(url)
        data = response.json()
        
        if data.get('ok'):
            # Обрабатываем посты из канала
            for update in data.get('result', []):
                if 'channel_post' in update:
                    post = update['channel_post']
                    if str(post.get('chat', {}).get('id')) == CHANNEL_ID and 'text' in post:
                        # Ищем ID пользователей в формате "ID: НОМЕР"
                        matches = re.findall(r'ID:\s*(\d+)', post['text'])
                        for match in matches:
                            temp_authorized_users.add(int(match))
        
        # Если нашли ID, обновляем основной набор
        if temp_authorized_users:
            authorized_users.update(temp_authorized_users)
            logger.info(f"Загружено {len(temp_authorized_users)} авторизованных пользователей")
        
    except Exception as e:
        logger.error(f"Ошибка при загрузке авторизованных пользователей: {e}")

def check_user_authorized(user_id):
    """Проверяет, авторизован ли пользователь"""
    # Если список пуст, пробуем загрузить
    if not authorized_users:
        load_authorized_users()
    
    return user_id in authorized_users

def update_authorized_users_periodically():
    """Периодически обновляет список авторизованных пользователей"""
    while True:
        try:
            load_authorized_users()
            logger.info(f"Обновлен список авторизованных пользователей. Всего: {len(authorized_users)}")
        except Exception as e:
            logger.error(f"Ошибка при обновлении списка авторизованных пользователей: {e}")
        
        # Обновляем каждую минуту
        time.sleep(60)

@bot.message_handler(commands=['start'])
def start_command(message):
    user_id = message.from_user.id

    # Проверяем, авторизован ли пользователь
    is_authorized = check_user_authorized(user_id)
    logger.info(f"Пользователь {user_id} авторизован: {is_authorized}")

    if is_authorized:
        # Если пользователь уже авторизован, показываем приветствие
        bot.send_message(message.chat.id, "Вы уже авторизованы. Добро пожаловать! / You are already authorized. Welcome!")
    else:
        # Если пользователь не авторизован, предлагаем выбрать язык
        markup = types.InlineKeyboardMarkup(row_width=2)
        ru_button = types.InlineKeyboardButton("RU 🇷🇺", callback_data='lang_ru')
        en_button = types.InlineKeyboardButton("EN 🇬🇧", callback_data='lang_en')
        markup.add(ru_button, en_button)

        bot.send_message(message.chat.id, "Выберите язык / Choose language:", reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data.startswith('lang_'))
def language_callback(call):
    user_id = call.from_user.id
    username = call.from_user.username or "No username"
    first_name = call.from_user.first_name or "No first name"
    last_name = call.from_user.last_name or "No last name"

    # Получаем выбранный язык
    language = call.data.split('_')[1].upper()

    # Проверяем, авторизован ли пользователь
    is_authorized = check_user_authorized(user_id)

    # Только если пользователь НЕ авторизован, отправляем информацию в канал
    if not is_authorized:
        user_info = f"New user:\nID: {user_id}\nUsername: @{username}\nName: {first_name} {last_name}\nLanguage: {language}"
        try:
            bot.send_message(CHANNEL_ID, user_info)
            logger.info(f"Информация о новом пользователе отправлена в канал: {user_info}")
        except Exception as e:
            logger.error(f"Ошибка при отправке информации в канал: {e}")

    # Отвечаем пользователю в зависимости от выбора языка и статуса авторизации
    if language == 'RU':
        if is_authorized:
            response = "Вы выбрали русский язык. Добро пожаловать!"
        else:
            response = "Вы выбрали русский язык. Ожидайте авторизации администратором."
    else:  # 'EN'
        if is_authorized:
            response = "You've selected English. Welcome!"
        else:
            response = "You've selected English. Please wait for admin authorization."

    # Отвечаем пользователю и убираем инлайн кнопки
    bot.edit_message_text(chat_id=call.message.chat.id, 
                         message_id=call.message.message_id,
                         text=response,
                         reply_markup=None)

@bot.message_handler(func=lambda message: message.text and message.text.lower() == 'hi')
def handle_hi(message):
    user_id = message.from_user.id
    
    # Проверяем, авторизован ли пользователь
    is_authorized = check_user_authorized(user_id)
    
    if is_authorized:
        bot.send_message(message.chat.id, "Hello! How can I help you today?")
    else:
        # Если пользователь не авторизован, предлагаем ему авторизоваться
        bot.send_message(message.chat.id, "You need to be authorized to use this bot. Please use /start to begin the authorization process.")

@bot.message_handler(func=lambda message: True)
def handle_all_messages(message):
    user_id = message.from_user.id
    
    # Проверяем, авторизован ли пользователь
    is_authorized = check_user_authorized(user_id)
    
    if not is_authorized:
        # Если пользователь не авторизован, предлагаем ему авторизоваться
        bot.send_message(message.chat.id, "Вам необходимо авторизоваться. Используйте /start для начала процесса авторизации.")
        return
    
    # Для авторизованных пользователей
    bot.send_message(message.chat.id, "Ваше сообщение получено. Напишите 'hi' для приветствия.")

def main():
    """Основная функция запуска бота"""
    # Загружаем авторизованных пользователей при запуске
    load_authorized_users()

    logger.info(f"Бот запущен. Загружено {len(authorized_users)} авторизованных пользователей.")

    # Запускаем отдельный поток для обновления списка авторизованных пользователей
    update_thread = threading.Thread(target=update_authorized_users_periodically)
    update_thread.daemon = True
    update_thread.start()

    # Запускаем бота
    while True:
        try:
            bot.polling(none_stop=True, interval=0, timeout=20)
        except Exception as e:
            logger.error(f"Ошибка в цикле бота: {e}")
            time.sleep(5)

if __name__ == "__main__":
    main()
