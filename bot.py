import telebot
from telebot import types
import logging
import re
import requests
import time
import threading

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Инициализация бота
BOT_TOKEN = "7671924788:AAFqich6NTHpOA22I6GysTqYXTFM-d0Pulo"  # Будет заменено через GitHub Secrets
CHANNEL_ID = "-1001948875251"  # Будет заменено через GitHub Secrets

bot = telebot.TeleBot(BOT_TOKEN)

# Кэш авторизованных пользователей
authorized_users = set()
last_update_time = 0
UPDATE_INTERVAL = 300  # Обновлять список пользователей каждые 5 минут

def get_channel_messages():
    """Получает сообщения из канала"""
    try:
        messages = []
        
        # Используем getChat для проверки доступности канала
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/getChat?chat_id={CHANNEL_ID}"
        response = requests.get(url, timeout=10)
        chat_data = response.json()
        
        if not chat_data.get('ok'):
            logger.error(f"Ошибка при получении информации о канале: {chat_data}")
            return []
            
        # Используем getChatHistory (если доступно в API)
        try:
            offset = 0
            limit = 100
            url = f"https://api.telegram.org/bot{BOT_TOKEN}/getChatHistory?chat_id={CHANNEL_ID}&limit={limit}&offset={offset}"
            response = requests.get(url, timeout=10)
            data = response.json()
            
            if data.get('ok'):
                messages.extend(data.get('result', []))
                return messages
            else:
                logger.warning("getChatHistory не доступен, пробуем альтернативные методы")
        except Exception as e:
            logger.warning(f"Ошибка при использовании getChatHistory: {e}")
        
        # Если getChatHistory не работает, пробуем getUpdates
        try:
            url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates?limit=100"
            response = requests.get(url, timeout=10)
            data = response.json()
            
            if data.get('ok'):
                for update in data.get('result', []):
                    if 'channel_post' in update and str(update['channel_post'].get('chat', {}).get('id')) == CHANNEL_ID:
                        messages.append(update['channel_post'])
                        
                return messages
        except Exception as e:
            logger.warning(f"Ошибка при использовании getUpdates: {e}")
        
        # Если и это не работает, пробуем получить отдельные сообщения
        try:
            for i in range(1, 101):  # Последние 100 сообщений
                url = f"https://api.telegram.org/bot{BOT_TOKEN}/getMessage?chat_id={CHANNEL_ID}&message_id={i}"
                response = requests.get(url, timeout=5)
                data = response.json()
                
                if data.get('ok'):
                    messages.append(data.get('result'))
        except Exception as e:
            logger.warning(f"Ошибка при получении отдельных сообщений: {e}")
        
        return messages

    except Exception as e:
        logger.error(f"Ошибка при получении сообщений из канала: {e}")
        return []

def load_authorized_users():
    """Загружает список авторизованных пользователей из канала"""
    global authorized_users, last_update_time
    
    # Проверяем, не слишком ли часто обновляем
    current_time = time.time()
    if current_time - last_update_time < UPDATE_INTERVAL:
        logger.debug("Пропускаем обновление списка пользователей, так как прошло меньше UPDATE_INTERVAL")
        return
    
    last_update_time = current_time
    temp_authorized_users = set()

    try:
        # Получаем сообщения из канала
        messages = get_channel_messages()
        
        # Обрабатываем полученные сообщения
        for message in messages:
            if isinstance(message, dict) and 'text' in message:
                # Ищем ID пользователей в формате "ID: НОМЕР"
                matches = re.findall(r'ID:\s*(\d+)', message['text'])
                for match in matches:
                    temp_authorized_users.add(int(match))
        
        # Если нашли хотя бы одного пользователя, обновляем список
        if temp_authorized_users:
            authorized_users = temp_authorized_users
            logger.info(f"Загружено {len(authorized_users)} авторизованных пользователей: {authorized_users}")
        else:
            logger.warning("Не удалось найти авторизованных пользователей в сообщениях канала")
            
    except Exception as e:
        logger.error(f"Ошибка при загрузке авторизованных пользователей: {e}")

def periodic_update():
    """Периодически обновляет список авторизованных пользователей"""
    while True:
        try:
            load_authorized_users()
        except Exception as e:
            logger.error(f"Ошибка в периодическом обновлении: {e}")
        time.sleep(UPDATE_INTERVAL)

def check_user_authorized(user_id):
    """Проверяет, авторизован ли пользователь"""
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

@bot.message_handler(commands=['checkid'])
def check_id_command(message):
    """Команда для проверки своего ID"""
    user_id = message.from_user.id
    is_authorized = check_user_authorized(user_id)
    
    if is_authorized:
        bot.send_message(message.chat.id, f"Ваш ID: {user_id}\nСтатус: Авторизован ✅")
    else:
        bot.send_message(message.chat.id, f"Ваш ID: {user_id}\nСтатус: Не авторизован ❌\nПожалуйста, дождитесь авторизации администратором.")

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
            # Пытаемся отправить сообщение иначе
            try:
                url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
                payload = {
                    "chat_id": CHANNEL_ID,
                    "text": user_info
                }
                requests.post(url, json=payload, timeout=10)
            except Exception as e2:
                logger.error(f"Вторая попытка отправки также не удалась: {e2}")

    # Отвечаем пользователю в зависимости от выбора языка и статуса авторизации
    if language == 'RU':
        if is_authorized:
            response = "Вы выбрали русский язык. Добро пожаловать!"
        else:
            response = f"Вы выбрали русский язык. Ожидайте авторизации администратором.\n\nВаш ID: {user_id} (сохраните его)"
    else:  # 'EN'
        if is_authorized:
            response = "You've selected English. Welcome!"
        else:
            response = f"You've selected English. Please wait for admin authorization.\n\nYour ID: {user_id} (please save it)"

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

    # Запускаем поток для периодического обновления списка пользователей
    update_thread = threading.Thread(target=periodic_update, daemon=True)
    update_thread.start()

    # Запускаем бота с обработкой исключений
    while True:
        try:
            logger.info("Запуск polling...")
            bot.polling(none_stop=True, interval=0, timeout=60)
        except requests.exceptions.ReadTimeout:
            logger.warning("Таймаут при чтении запроса, перезапуск polling")
            time.sleep(1)
        except requests.exceptions.ConnectionError:
            logger.error("Ошибка соединения, ожидание перед повторной попыткой")
            time.sleep(10)
        except Exception as e:
            logger.error(f"Критическая ошибка в цикле бота: {e}")
            time.sleep(30)  # Ждем подольше при критических ошибках

if __name__ == "__main__":
    main()