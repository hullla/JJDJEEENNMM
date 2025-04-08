import telebot
from telebot import types
import logging
import re
import requests

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Инициализация бота
BOT_TOKEN = "7671924788:AAG_v2uSvLqg1IXhAZAvdR0wJEONhzp0fbU"  # Будет заменено через GitHub Secrets
CHANNEL_ID = "-1001948875251"  # Будет заменено через GitHub Secrets

bot = telebot.TeleBot(BOT_TOKEN)

# Кэш авторизованных пользователей
authorized_users = set()

def load_authorized_users():
    """Загружает список авторизованных пользователей из канала"""
    try:
        # Получаем сообщения через API
        offset = 0
        limit = 100
        all_messages = []
        
        while True:
            url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates?limit={limit}&offset={offset}"
            response = requests.get(url)
            data = response.json()
            
            if not data.get('ok'):
                logger.error(f"Ошибка при получении обновлений: {data}")
                break
                
            result = data.get('result', [])
            if not result:
                break
                
            all_messages.extend(result)
            offset = result[-1]['update_id'] + 1
            
            if len(result) < limit:
                break
        
        # Извлекаем сообщения из канала
        channel_messages = []
        for update in all_messages:
            if 'channel_post' in update:
                channel_post = update['channel_post']
                if str(channel_post.get('chat', {}).get('id')) == CHANNEL_ID and 'text' in channel_post:
                    channel_messages.append(channel_post['text'])
        
        # Ищем ID пользователей
        for message in channel_messages:
            matches = re.findall(r'ID:\s*(\d+)', message)
            for match in matches:
                authorized_users.add(int(match))
                
        logger.info(f"Загружено {len(authorized_users)} авторизованных пользователей.")
    
    except Exception as e:
        logger.error(f"Ошибка при загрузке авторизованных пользователей: {e}")

@bot.message_handler(commands=['start'])
def start_command(message):
    user_id = message.from_user.id
    
    # Если кэш авторизованных пользователей пуст, заполняем его
    if not authorized_users:
        load_authorized_users()
    
    # Создаем инлайн клавиатуру
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
    
    # Проверяем есть ли пользователь уже в списке авторизованных
    if not authorized_users:
        load_authorized_users()
        
    # Отправляем информацию в канал только если пользователь еще не зарегистрирован
    user_exists = user_id in authorized_users
    
    if not user_exists:
        user_info = f"New user:\nID: {user_id}\nUsername: @{username}\nName: {first_name} {last_name}\nLanguage: {language}"
        try:
            bot.send_message(CHANNEL_ID, user_info)
            logger.info(f"Информация о пользователе отправлена в канал: {user_info}")
        except Exception as e:
            logger.error(f"Ошибка при отправке информации в канал: {e}")
    
    # Проверяем авторизацию
    is_authorized = user_id in authorized_users
    
    # Отвечаем пользователю в зависимости от выбора языка
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
    bot.infinity_polling()

if __name__ == "__main__":
    main()
