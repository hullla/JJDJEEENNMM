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
BOT_TOKEN = "7671924788:AAFygetCrBwqFGtgHPsxRUGi8pDLlmDszKo"
CHANNEL_ID = "-1001948875251"

bot = telebot.TeleBot(BOT_TOKEN)

# Кэш авторизованных пользователей (для временного хранения)
authorized_users = set()

def check_user_in_channel(user_id):
    """Проверяет, есть ли ID пользователя в сообщениях канала"""
    try:
        # Используем getUpdates для получения истории сообщений канала
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates?limit=100&allowed_updates=[\"channel_post\"]"
        response = requests.get(url, timeout=10)
        data = response.json()

        if not data.get('ok'):
            logger.error(f"Ошибка при получении обновлений: {data}")
            return False

        # Ищем ID пользователя в сообщениях канала
        for update in data.get('result', []):
            if 'channel_post' in update:
                post = update['channel_post']
                if str(post.get('chat', {}).get('id')) == CHANNEL_ID and 'text' in post:
                    if f"ID: {user_id}" in post['text']:
                        # Добавляем ID в кэш для ускорения будущих проверок
                        authorized_users.add(user_id)
                        return True

        return False
    except Exception as e:
        logger.error(f"Ошибка при проверке пользователя в канале: {e}")
        return False

@bot.message_handler(commands=['start'])
def start_command(message):
    user_id = message.from_user.id

    # Сначала проверяем кэш для быстрого ответа
    if user_id in authorized_users:
        bot.send_message(message.chat.id, "Вы уже авторизованы. Добро пожаловать! / You are already authorized. Welcome!")
        return

    # Проверяем наличие ID в канале
    is_authorized = check_user_in_channel(user_id)
    logger.info(f"Пользователь {user_id} авторизован: {is_authorized}")

    if is_authorized:
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
    try:
        user_id = call.from_user.id
        username = call.from_user.username or "No username"
        first_name = call.from_user.first_name or "No first name"
        last_name = call.from_user.last_name or "No last name"

        # Получаем выбранный язык
        language = call.data.split('_')[1].upper()
        
        # Отвечаем пользователю и убираем инлайн кнопки, делаем это сразу
        if language == 'RU':
            response = "Вы выбрали русский язык. Ожидайте авторизации администратором."
        else:  # 'EN'
            response = "You've selected English. Please wait for admin authorization."
            
        bot.edit_message_text(chat_id=call.message.chat.id, 
                            message_id=call.message.message_id,
                            text=response,
                            reply_markup=None)
        
        # Проверяем наличие ID в канале ПОСЛЕ ответа пользователю
        is_authorized = check_user_in_channel(user_id)

        # Если пользователя уже авторизован, обновляем сообщение
        if is_authorized:
            authorized_users.add(user_id)
            if language == 'RU':
                bot.send_message(call.message.chat.id, "Вы авторизованы. Добро пожаловать!")
            else:  # 'EN'
                bot.send_message(call.message.chat.id, "You are authorized. Welcome!")
        else:
            # Если пользователя нет в канале, отправляем информацию
            user_info = f"New user:\nID: {user_id}\nUsername: @{username}\nName: {first_name} {last_name}\nLanguage: {language}"
            try:
                bot.send_message(CHANNEL_ID, user_info)
                logger.info(f"Информация о новом пользователе отправлена в канал: {user_info}")
            except Exception as e:
                logger.error(f"Ошибка при отправке информации в канал: {e}")
                # Сообщаем пользователю о проблеме
                if language == 'RU':
                    bot.send_message(call.message.chat.id, "Произошла ошибка. Пожалуйста, попробуйте позже.")
                else:  # 'EN'
                    bot.send_message(call.message.chat.id, "An error occurred. Please try again later.")
                
        # Отвечаем на callback чтобы убрать загрузку с кнопки
        bot.answer_callback_query(call.id)
                
    except Exception as e:
        logger.error(f"Ошибка в обработке callback: {e}")
        bot.answer_callback_query(call.id, text="Error occurred")

@bot.message_handler(func=lambda message: True)
def handle_messages(message):
    """Обрабатывает все входящие сообщения"""
    user_id = message.from_user.id

    # Проверяем авторизацию пользователя
    if user_id in authorized_users:
        is_authorized = True
    else:
        is_authorized = check_user_in_channel(user_id)
        if is_authorized:
            authorized_users.add(user_id)

    if not is_authorized:
        bot.send_message(message.chat.id, "Вы не авторизованы. Используйте /start для начала.")
        return

    # Обработка сообщения "hi"
    if message.text.lower() == "hi":
        bot.send_message(message.chat.id, "Привет! Чем могу помочь?")

def main():
    """Основная функция запуска бота"""
    logger.info("Бот запущен.")

    # Запускаем бота
    while True:
        try:
            bot.polling(none_stop=True, interval=1, timeout=60)
        except Exception as e:
            logger.error(f"Ошибка в цикле бота: {e}")
            time.sleep(10)

if __name__ == "__main__":
    main()