import telebot
from telebot import types
import logging
import re
import requests
import time

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Инициализация бота
BOT_TOKEN = "7671924788:AAGxFv3Mtkr1xrfDCfA9JfmlaFSkWid9ckg"  # Будет заменено через GitHub Secrets
CHANNEL_ID = "-1001948875251"  # Будет заменено через GitHub Secrets

bot = telebot.TeleBot(BOT_TOKEN)

# Кэш авторизованных пользователей
authorized_users = set()

def get_channel_messages():
    """Получает сообщения из канала"""
    try:
        # Получаем историю сообщений через API
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/getChat?chat_id={CHANNEL_ID}"
        response = requests.get(url)
        chat_data = response.json()
        
        if not chat_data.get('ok'):
            logger.error(f"Ошибка при получении информации о канале: {chat_data}")
            return []
        
        # Получаем сообщения из канала
        messages = []
        offset = 0
        limit = 100  # Максимальное количество сообщений для получения
        
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/getChatHistory?chat_id={CHANNEL_ID}&limit={limit}&offset={offset}"
        response = requests.get(url)
        data = response.json()
        
        if not data.get('ok'):
            # Если getHistory не работает, пробуем использовать другой метод
            url = f"https://api.telegram.org/bot{BOT_TOKEN}/forwardMessages?chat_id={BOT_TOKEN.split(':')[0]}&from_chat_id={CHANNEL_ID}&message_ids=1-100"
            response = requests.get(url)
            data = response.json()
            
            if not data.get('ok'):
                # Если и это не работает, попробуем использовать getUpdates
                url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates?limit=100"
                response = requests.get(url)
                data = response.json()
                
                if data.get('ok'):
                    for update in data.get('result', []):
                        if 'channel_post' in update and str(update['channel_post'].get('chat', {}).get('id')) == CHANNEL_ID:
                            messages.append(update['channel_post'])
            
        return messages
        
    except Exception as e:
        logger.error(f"Ошибка при получении сообщений из канала: {e}")
        return []

def load_authorized_users():
    """Загружает список авторизованных пользователей из канала"""
    global authorized_users
    authorized_users.clear()
    
    try:
        # Альтернативный подход - использовать getMessage
        for i in range(1, 101):  # Получаем 100 последних сообщений
            try:
                url = f"https://api.telegram.org/bot{BOT_TOKEN}/getMessage?chat_id={CHANNEL_ID}&message_id={i}"
                response = requests.get(url)
                data = response.json()
                
                if data.get('ok'):
                    message = data.get('result', {})
                    if 'text' in message:
                        # Ищем ID пользователей в формате "ID: НОМЕР"
                        matches = re.findall(r'ID:\s*(\d+)', message['text'])
                        for match in matches:
                            authorized_users.add(int(match))
            except:
                continue
        
        # Если не получилось, используем другой подход
        if not authorized_users:
            # Получаем сообщения из канала через getUpdates
            url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates?limit=100"
            response = requests.get(url)
            data = response.json()
            
            if data.get('ok'):
                for update in data.get('result', []):
                    if 'channel_post' in update:
                        post = update['channel_post']
                        if str(post.get('chat', {}).get('id')) == CHANNEL_ID and 'text' in post:
                            # Ищем ID пользователей в формате "ID: НОМЕР"
                            matches = re.findall(r'ID:\s*(\d+)', post['text'])
                            for match in matches:
                                authorized_users.add(int(match))
        
        logger.info(f"Загружено {len(authorized_users)} авторизованных пользователей: {authorized_users}")
    except Exception as e:
        logger.error(f"Ошибка при загрузке авторизованных пользователей: {e}")

def check_user_authorized(user_id):
    """Проверяет, авторизован ли пользователь"""
    # Обновляем список авторизованных пользователей
    if not authorized_users:
        load_authorized_users()
    
    return user_id in authorized_users

@bot.message_handler(commands=['start'])
def start_command(message):
    user_id = message.from_user.id
    
    # Проверяем, авторизован ли пользователь
    is_authorized = check_user_authorized(user_id)
    logger.info(f"Пользователь {user_id} авторизован: {is_authorized}")
    
    if is_authorized:
        # Если пользователь уже авторизован, показываем соответствующее сообщение
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
    
    # Если пользователь не авторизован, отправляем информацию в канал
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

def main():
    """Основная функция запуска бота"""
    # Загружаем авторизованных пользователей при запуске
    load_authorized_users()
    
    logger.info(f"Бот запущен. Загружено {len(authorized_users)} авторизованных пользователей.")
    
    # Добавляем таймер для периодического обновления списка авторизованных пользователей
    while True:
        try:
            bot.polling(none_stop=True, interval=0, timeout=60)
        except Exception as e:
            logger.error(f"Ошибка в цикле бота: {e}")
            time.sleep(10)

if __name__ == "__main__":
    main()
