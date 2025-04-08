import telebot
from telebot import types
import logging
import re
import requests
import time
import threading

# Настройка более детального логирования
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Инициализация бота
BOT_TOKEN = "7671924788:AAEEDhZ7xBXUXESOHEq-hLsetlCSjK3hMY0"
CHANNEL_ID = "-1001948875251"

bot = telebot.TeleBot(BOT_TOKEN)

# Кэш авторизованных пользователей
authorized_users = set()
# Блокировка для безопасного доступа к списку
auth_lock = threading.Lock()

def load_authorized_users():
    """Загружает список авторизованных пользователей из канала"""
    global authorized_users
    temp_authorized_users = set()
    
    try:
        # Получаем сообщения напрямую из канала
        try:
            messages = bot.get_chat_history(CHANNEL_ID, limit=100)
            logger.debug(f"Получено {len(messages)} сообщений из истории канала")
            
            for message in messages:
                if message.text:
                    matches = re.findall(r'ID:\s*(\d+)', message.text)
                    for match in matches:
                        temp_authorized_users.add(int(match))
        except Exception as e:
            logger.error(f"Ошибка при получении истории канала: {e}")
        
        # Если прямой метод не сработал, пробуем через API
        if not temp_authorized_users:
            url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates?limit=100&allowed_updates=[\"channel_post\"]"
            response = requests.get(url, timeout=10)
            data = response.json()
            
            if data.get('ok'):
                # Обрабатываем посты из канала
                for update in data.get('result', []):
                    if 'channel_post' in update:
                        post = update['channel_post']
                        if str(post.get('chat', {}).get('id')) == CHANNEL_ID and 'text' in post:
                            matches = re.findall(r'ID:\s*(\d+)', post['text'])
                            for match in matches:
                                temp_authorized_users.add(int(match))
        
        # Безопасно обновляем глобальный набор с использованием блокировки
        with auth_lock:
            if temp_authorized_users:
                authorized_users.update(temp_authorized_users)
                logger.info(f"Загружено {len(temp_authorized_users)} авторизованных пользователей, всего в базе: {len(authorized_users)}")
            else:
                logger.warning("Не удалось найти авторизованных пользователей в канале")
        
    except Exception as e:
        logger.error(f"Ошибка при загрузке авторизованных пользователей: {e}")

def check_user_authorized(user_id):
    """Проверяет, авторизован ли пользователь"""
    # Используем блокировку для безопасной проверки
    with auth_lock:
        is_authorized = user_id in authorized_users
    
    # Если список пуст или пользователь не найден, пробуем загрузить снова
    if not is_authorized and not authorized_users:
        load_authorized_users()
        # Проверяем еще раз после загрузки
        with auth_lock:
            is_authorized = user_id in authorized_users
    
    logger.debug(f"Проверка авторизации для пользователя {user_id}: {is_authorized}")
    return is_authorized

def update_authorized_users_periodically():
    """Периодически обновляет список авторизованных пользователей"""
    while True:
        try:
            load_authorized_users()
            # Используем блокировку для безопасного доступа
            with auth_lock:
                users_count = len(authorized_users)
            logger.info(f"Периодическое обновление: авторизованных пользователей: {users_count}")
        except Exception as e:
            logger.error(f"Ошибка при периодическом обновлении: {e}")
        
        # Обновляем каждые 30 секунд
        time.sleep(30)

@bot.message_handler(commands=['start'])
def start_command(message):
    try:
        user_id = message.from_user.id
        logger.info(f"Получена команда /start от пользователя {user_id}")

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
    except Exception as e:
        logger.error(f"Ошибка в обработке команды start: {e}")
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
        
        logger.info(f"Получен callback {call.data} от пользователя {user_id}")

        # Получаем выбранный язык
        language = call.data.split('_')[1].upper()

        # Проверяем, авторизован ли пользователь
        is_authorized = check_user_authorized(user_id)
        logger.info(f"Статус авторизации при выборе языка для {user_id}: {is_authorized}")

        # Только если пользователь НЕ авторизован, отправляем информацию в канал
        if not is_authorized:
            user_info = f"New user:\nID: {user_id}\nUsername: @{username}\nName: {first_name} {last_name}\nLanguage: {language}"
            try:
                send_result = bot.send_message(CHANNEL_ID, user_info)
                logger.info(f"Отправлена информация в канал. Результат: {send_result}")
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
        edit_result = bot.edit_message_text(
            chat_id=call.message.chat.id, 
            message_id=call.message.message_id,
            text=response,
            reply_markup=None
        )
        logger.info(f"Отредактировано сообщение. Результат: {edit_result}")
        
        # Отправляем дополнительное сообщение для надежности
        bot.send_message(call.message.chat.id, "Спасибо за выбор языка! / Thank you for selecting language!")
        
        # Отвечаем на callback, чтобы убрать "часики" у кнопки
        bot.answer_callback_query(call.id)
        
    except Exception as e:
        logger.error(f"Ошибка в обработке выбора языка: {e}")
        try:
            bot.answer_callback_query(call.id, "Произошла ошибка, попробуйте снова")
            bot.send_message(call.message.chat.id, "Произошла ошибка при обработке выбора языка. Пожалуйста, используйте /start и попробуйте снова.")
        except:
            pass

@bot.message_handler(func=lambda message: message.text and message.text.lower() == 'hi')
def handle_hi(message):
    try:
        user_id = message.from_user.id
        logger.info(f"Получено сообщение 'hi' от пользователя {user_id}")
        
        # Проверяем, авторизован ли пользователь
        is_authorized = check_user_authorized(user_id)
        logger.info(f"Проверка авторизации для hi: {user_id} - {is_authorized}")
        
        if is_authorized:
            bot.send_message(message.chat.id, "Hello! How can I help you today?")
        else:
            # Если пользователь не авторизован, предлагаем ему авторизоваться
            bot.send_message(message.chat.id, "You need to be authorized to use this bot. Please use /start to begin the authorization process.")
    except Exception as e:
        logger.error(f"Ошибка при обработке сообщения 'hi': {e}")

@bot.message_handler(func=lambda message: True)
def handle_all_messages(message):
    try:
        user_id = message.from_user.id
        logger.debug(f"Получено сообщение от пользователя {user_id}: {message.text[:20]}...")
        
        # Проверяем, авторизован ли пользователь
        is_authorized = check_user_authorized(user_id)
        
        if not is_authorized:
            # Если пользователь не авторизован, предлагаем ему авторизоваться
            bot.send_message(message.chat.id, "Вам необходимо авторизоваться. Используйте /start для начала процесса авторизации.")
            return
        
        # Для авторизованных пользователей
        bot.send_message(message.chat.id, "Ваше сообщение получено. Напишите 'hi' для приветствия.")
    except Exception as e:
        logger.error(f"Ошибка при обработке сообщения: {e}")

def main():
    """Основная функция запуска бота"""
    try:
        # Загружаем авторизованных пользователей при запуске
        load_authorized_users()
        
        with auth_lock:
            users_count = len(authorized_users)
        
        logger.info(f"Бот запущен. Загружено {users_count} авторизованных пользователей.")

        # Запускаем отдельный поток для обновления списка авторизованных пользователей
        update_thread = threading.Thread(target=update_authorized_users_periodically)
        update_thread.daemon = True
        update_thread.start()

        # Запускаем бота с надежной обработкой ошибок
        while True:
            try:
                logger.info("Запуск цикла polling...")
                bot.polling(none_stop=True, interval=1, timeout=15)
            except Exception as e:
                logger.error(f"Ошибка в цикле бота: {e}")
                time.sleep(3)
    except Exception as e:
        logger.critical(f"Критическая ошибка при запуске бота: {e}")

if __name__ == "__main__":
    main()
