import telebot
from telebot import types
import logging
import re
import requests
import time

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Инициализация бота
BOT_TOKEN = "7671924788:AAFygetCrBwqFGtgHPsxRUGi8pDLlmDszKo"
CHANNEL_ID = "-1001948875251"

bot = telebot.TeleBot(BOT_TOKEN)

# Кэш авторизованных пользователей (для временного хранения)
authorized_users = set()

def check_user_in_channel(user_id):
    """Проверяет, есть ли ID пользователя в сообщениях канала"""
    # Сначала проверяем кэш
    if user_id in authorized_users:
        return True
        
    try:
        # Лучше использовать getChatMember вместо getUpdates
        try:
            # Попробуем сначала проверить является ли пользователь участником канала
            url = f"https://api.telegram.org/bot{BOT_TOKEN}/getChatMember?chat_id={CHANNEL_ID}&user_id={user_id}"
            response = requests.get(url, timeout=10)
            data = response.json()
            
            # Если пользователь найден в канале
            if data.get('ok') and data.get('result', {}).get('status') not in ['left', 'kicked']:
                authorized_users.add(user_id)
                return True
        except Exception as e:
            logger.warning(f"Ошибка при проверке пользователя через getChatMember: {e}")
            
        # Если нужно проверять ID в сообщениях канала
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
    
    try:
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
    except Exception as e:
        logger.error(f"Ошибка в функции start_command: {e}")
        try:
            bot.send_message(message.chat.id, "Произошла ошибка. Пожалуйста, попробуйте еще раз позже.")
        except:
            pass

@bot.callback_query_handler(func=lambda call: call.data.startswith('lang_'))
def language_callback(call):
    try:
        user_id = call.from_user.id
        username = call.from_user.username or "No username"
        first_name = call.from_user.first_name or "No first name"
        last_name = call.from_user.last_name or "No last name"

        # Получаем выбранный язык
        language = call.data.split('_')[1].upper()

        # Проверяем наличие ID в канале
        is_authorized = check_user_in_channel(user_id)

        # Если пользователя нет в канале, отправляем информацию
        if not is_authorized:
            user_info = f"New user:\nID: {user_id}\nUsername: @{username}\nName: {first_name} {last_name}\nLanguage: {language}"
            try:
                bot.send_message(CHANNEL_ID, user_info)
                # Добавляем в кэш после отправки в канал
                authorized_users.add(user_id)
                logger.info(f"Информация о новом пользователе отправлена в канал: {user_info}")
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
    except Exception as e:
        logger.error(f"Ошибка при обработке выбора языка: {e}")
        # Если не удалось отредактировать сообщение, отправляем новое
        try:
            bot.send_message(call.message.chat.id, "Произошла ошибка. Пожалуйста, используйте /start снова.")
        except:
            pass
    finally:
        # Отправляем завершающий ответ на callback запрос
        try:
            bot.answer_callback_query(call.id)
        except Exception as e:
            logger.error(f"Ошибка при завершении callback запроса: {e}")

@bot.message_handler(func=lambda message: True)
def handle_messages(message):
    """Обрабатывает все входящие сообщения"""
    try:
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
    except Exception as e:
        logger.error(f"Ошибка при обработке сообщения: {e}")

def main():
    """Основная функция запуска бота"""
    logger.info("Бот запущен.")

    # Запускаем бота
    while True:
        try:
            bot.polling(none_stop=True, interval=1, timeout=60)
        except Exception as e:
            logger.error(f"Ошибка в цикле бота: {e}")
            # Пересоздаем объект бота при критической ошибке
            try:
                global bot
                bot = telebot.TeleBot(BOT_TOKEN)
            except Exception as re_init_error:
                logger.error(f"Ошибка при пересоздании бота: {re_init_error}")
            time.sleep(10)

if __name__ == "__main__":
    main()
