import telebot
from telebot import types
import logging
import time
import requests
from datetime import datetime
from statistics import (
    initialize_jsonbin, get_users_data, is_user_authorized, check_and_update_last_access, 
    register_user, get_user_stats, get_global_stats, get_activity_stats,
    create_statistics_menu, show_user_statistics, show_activity_statistics,
    generate_detailed_statistics_file
)

# Настройка более подробного логирования
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Инициализация бота
BOT_TOKEN = "7671924788:AAHSVGQ6pK3guB97tLeVEXH2s16YPzJfFP4"
CHANNEL_ID = "-1001948875251"  # Оставляем для обратной совместимости

bot = telebot.TeleBot(BOT_TOKEN)

@bot.message_handler(commands=['start'])
def start_command(message):
    try:
        user_id = message.from_user.id
        chat_id = message.chat.id
        logger.info(f"Команда /start от пользователя {user_id}")

        # Анимация загрузки
        load_emojis = [
            "~(˘▾˘~)",
            "~(˘▾˘~)~(˘▾˘~)",
            "~(˘▾˘~)~(˘▾˘~)~(˘▾˘~)",
            "(◡‿◡✿)(◡‿◡✿)(◡‿◡✿)(◡‿◡✿)",
            "(◕‿↼)(◕‿↼)(◕‿↼)(◕‿↼)(◕‿↼)"
        ]

        msg = bot.send_message(chat_id, load_emojis[0])

        # Проигрываем анимацию
        for emoji in load_emojis[1:]:
            time.sleep(0.07)
            bot.edit_message_text(emoji, chat_id, msg.message_id)

        # Проверка авторизации через кэш данных JSONBin
        authorized = is_user_authorized(user_id)
        logger.debug(f"Результат проверки авторизации: {authorized}")

        if authorized:
            # Показываем меню статистики вместо "Вы авторизованы"
            user_stats = get_user_stats(user_id)
            language = user_stats.get('language', 'RU') if user_stats else 'RU'
            markup = create_statistics_menu(language)
            
            message_text = "✅ Вы авторизованы!" if language == 'RU' else "✅ You are authorized!"
            bot.edit_message_text(message_text, chat_id, msg.message_id, reply_markup=markup)
        else:
            markup = types.InlineKeyboardMarkup(row_width=2)
            ru_button = types.InlineKeyboardButton("RU 🇷🇺", callback_data='lang_ru')
            en_button = types.InlineKeyboardButton("EN 🇬🇧", callback_data='lang_en')
            markup.add(ru_button, en_button)
            bot.edit_message_text("Выберите язык / Choose language:", chat_id, msg.message_id, reply_markup=markup)
    except Exception as e:
        logger.error(f"Ошибка при обработке команды /start: {e}")
        try:
            bot.send_message(chat_id, "Произошла ошибка. Попробуйте позже.")
        except:
            pass

@bot.message_handler(commands=['stats', 'activity_stats'])
def stats_commands(message):
    try:
        user_id = message.from_user.id
        chat_id = message.chat.id
        logger.info(f"Команда статистики от пользователя {user_id}")

        # Проверка авторизации
        if not is_user_authorized(user_id):
            bot.send_message(chat_id, "Вы не авторизованы. Используйте /start для регистрации.")
            return

        # Получаем данные пользователя
        user_stats = get_user_stats(user_id)
        language = user_stats.get('language', 'RU') if user_stats else 'RU'
        
        # Показываем меню статистики
        markup = create_statistics_menu(language)
        message_text = "📊 Статистика:" if language == 'RU' else "📊 Statistics:"
        bot.send_message(chat_id, message_text, reply_markup=markup)
    except Exception as e:
        logger.error(f"Ошибка при обработке команды статистики: {e}")
        try:
            bot.send_message(chat_id, "Произошла ошибка. Попробуйте позже.")
        except:
            pass

@bot.callback_query_handler(func=lambda call: call.data.startswith('lang_'))
def language_callback(call):
    try:
        user_id = call.from_user.id
        language = call.data.split('_')[1].upper()
        logger.info(f"Выбор языка от пользователя {user_id}: {language}")

        # Финальная проверка перед регистрацией
        if is_user_authorized(user_id):
            response = "🔐 Вы уже в системе!" if language == 'RU' else "🔐 Already registered!"
            
            # Показываем меню статистики
            markup = create_statistics_menu(language)
            bot.edit_message_text(response, call.message.chat.id, call.message.message_id, reply_markup=markup)
            return

        # Регистрация пользователя в JSONBin
        try:
            if register_user(user_id, language):
                response = "📬 Запрос отправлен!" if language == 'RU' else "📬 Request submitted!"
                
                # Показываем меню статистики
                markup = create_statistics_menu(language)
                bot.edit_message_text(response, call.message.chat.id, call.message.message_id, reply_markup=markup)
            else:
                response = "⚠️ Ошибка регистрации" if language == 'RU' else "⚠️ Registration error"
                bot.edit_message_text(response, call.message.chat.id, call.message.message_id)
        except Exception as e:
            logger.error(f"Ошибка регистрации: {e}")
            response = "⚠️ Ошибка связи" if language == 'RU' else "⚠️ Connection error"
            bot.edit_message_text(response, call.message.chat.id, call.message.message_id)
    except Exception as e:
        logger.error(f"Ошибка при обработке выбора языка: {e}")
        try:
            bot.answer_callback_query(call.id, "Произошла ошибка. Попробуйте позже.")
        except:
            pass

@bot.callback_query_handler(func=lambda call: call.data.startswith('stats_'))
def statistics_callback(call):
    try:
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        message_id = call.message.message_id
        action = call.data.split('_')[1]
        
        # Проверка авторизации
        if not is_user_authorized(user_id):
            bot.answer_callback_query(call.id, "Вы не авторизованы. Используйте /start для регистрации.")
            return
        
        user_stats = get_user_stats(user_id)
        language = user_stats.get('language', 'RU') if user_stats else 'RU'
        
        if action == 'user':
            # Показываем статистику пользователя
            message_text, markup = show_user_statistics(user_id, language)
            bot.edit_message_text(message_text, chat_id, message_id, reply_markup=markup, parse_mode="Markdown")
        
        elif action == 'activity':
            # Показываем статистику активности
            message_text, markup = show_activity_statistics(user_id, language)
            bot.edit_message_text(message_text, chat_id, message_id, reply_markup=markup, parse_mode="Markdown")
        
        elif action == 'detailed':
            # Генерируем и отправляем файл со статистикой
            waiting_message = "Генерация детальной статистики..." if language == 'RU' else "Generating detailed statistics..."
            bot.edit_message_text(waiting_message, chat_id, message_id)
            
            filename, file_content = generate_detailed_statistics_file(language)
            
            # Отправляем файл
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(file_content)
            
            with open(filename, 'rb') as f:
                bot.send_document(chat_id, f, caption="📊 Детальная статистика" if language == 'RU' else "📊 Detailed statistics")
            
            # Восстанавливаем меню
            markup = create_statistics_menu(language)
            message_text = "📊 Статистика:" if language == 'RU' else "📊 Statistics:"
            bot.edit_message_text(message_text, chat_id, message_id, reply_markup=markup)
        
        elif action == 'back':
            # Возвращаемся к меню статистики
            markup = create_statistics_menu(language)
            message_text = "📊 Статистика:" if language == 'RU' else "📊 Statistics:"
            bot.edit_message_text(message_text, chat_id, message_id, reply_markup=markup)
        
    except Exception as e:
        logger.error(f"Ошибка при обработке статистики: {e}")
        try:
            bot.answer_callback_query(call.id, "Произошла ошибка при обработке статистики. Попробуйте позже.")
        except:
            pass

def main():
    try:
        # Проверяем доступность JSONBin и инициализируем структуру при необходимости
        logger.info("Инициализация JSONBin структуры...")
        success = initialize_jsonbin()
        if not success:
            logger.error("Не удалось инициализировать JSONBin структуру!")

        # При запуске бота, сразу загружаем данные пользователей в кэш
        logger.info("Загрузка данных пользователей в кэш...")
        users = get_users_data(force_update=True)
        logger.info(f"Загружено {len(users) if users else 0} записей")

        logger.info("Бот запущен. Кэш пользователей инициализирован.")

        # Добавляем тестовый запрос к API Telegram для проверки токена
        test_response = requests.get(f"https://api.telegram.org/bot{BOT_TOKEN}/getMe")
        if test_response.status_code == 200:
            bot_info = test_response.json()
            logger.info(f"Бот подключен: @{bot_info['result']['username']}")
        else:
            logger.error(f"Ошибка подключения к Telegram API: {test_response.status_code}")

        bot.infinity_polling()
    except Exception as e:
        logger.critical(f"Критическая ошибка при запуске бота: {e}")

if __name__ == "__main__":
    main()
