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

def is_user_in_channel(user_id):
    """Проверяет, есть ли ID пользователя в канале"""
    try:
        # Получаем содержимое канала
        messages = []
        offset = 0
        limit = 100
        
        while True:
            url = f"https://api.telegram.org/bot{BOT_TOKEN}/getChatHistory?chat_id={CHANNEL_ID}&limit={limit}&offset={offset}"
            response = requests.get(url)
            data = response.json()
            
            if not data.get('ok'):
                # Если метод getChatHistory не поддерживается, пробуем getUpdates
                url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates?limit={limit}&offset={offset}"
                response = requests.get(url)
                data = response.json()
                
                if not data.get('ok'):
                    logger.error(f"Ошибка при получении сообщений из канала: {data}")
                    return False
                    
                result = data.get('result', [])
                for update in result:
                    if 'channel_post' in update and str(update['channel_post'].get('chat', {}).get('id')) == CHANNEL_ID:
                        messages.append(update['channel_post'])
            else:
                result = data.get('result', [])
                messages.extend(result)
            
            if len(result) < limit:
                break
                
            offset += limit
        
        # Ищем ID пользователя в сообщениях канала
        user_id_str = str(user_id)
        for message in messages:
            if 'text' in message:
                if re.search(rf'ID:\s*{user_id_str}', message['text']):
                    logger.info(f"ID пользователя {user_id} найден в канале")
                    return True
        
        logger.info(f"ID пользователя {user_id} не найден в канале")
        return False
    
    except Exception as e:
        logger.error(f"Ошибка при проверке пользователя в канале: {e}")
        return False

def is_user_authorized(user_id):
    """Проверяет, авторизован ли пользователь"""
    # Проверяем, есть ли пользователь в канале
    return is_user_in_channel(user_id)

@bot.message_handler(commands=['start'])
def start_command(message):
    user_id = message.from_user.id
    
    # Проверяем авторизацию пользователя
    is_authorized = is_user_authorized(user_id)
    logger.info(f"Пользователь {user_id} авторизован: {is_authorized}")
    
    if is_authorized:
        # Если пользователь уже авторизован, приветствуем его
        bot.send_message(message.chat.id, "Вы уже авторизованы в системе! / You are already authorized in the system!")
    else:
        # Если пользователь не авторизован, просим выбрать язык
        markup = types.InlineKeyboardMarkup(row_width=2)
        ru_button = types.InlineKeyboardButton("RU 🇷🇺", callback_data='lang_ru')
        en_button = types.InlineKeyboardButton("EN 🇬🇧", callback_data='lang_en')
        markup.add(ru_button, en_button)
        
        bot.send_message(message.chat.id, "Выберите язык / Choose language:", reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data.startswith('lang_'))
def language_callback(call):
    user_id = call.from_user.id
    
    # Получаем выбранный язык
    language = call.data.split('_')[1].upper()
    
    # Проверяем авторизацию
    is_authorized = is_user_authorized(user_id)
    
    # Отправляем информацию в канал только если пользователь еще не авторизован
    if not is_authorized:
        # Отправляем только ID и язык
        user_info = f"ID: {user_id}\nLanguage: {language}"
        try:
            bot.send_message(CHANNEL_ID, user_info)
            logger.info(f"Информация о пользователе отправлена в канал: {user_info}")
        except Exception as e:
            logger.error(f"Ошибка при отправке информации в канал: {e}")
    
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
    logger.info(f"Бот запущен.")
    bot.infinity_polling()

if __name__ == "__main__":
    main()