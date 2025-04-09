import telebot
from telebot import types
import logging
import time
import requests
from datetime import datetime, timedelta
import json

# Настройка более подробного логирования
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Инициализация бота
BOT_TOKEN = "7671924788:AAHSVGQ6pK3guB97tLeVEXH2s16YPzJfFP4"
CHANNEL_ID = "-1001948875251"  # Оставляем для обратной совместимости
JSONBIN_API_KEY = "$2a$10$hT79uCEaJENfQBZ7576aL.upUOtnPqJZX53sWcln0HZib/bgs.8.u"
JSONBIN_BIN_ID = "67f532028a456b796684e974"

bot = telebot.TeleBot(BOT_TOKEN)

# Локальный кэш данных пользователей для минимизации API-запросов
users_cache = None
last_cache_update = 0
CACHE_TTL = 7200  # Время жизни кэша в секундах (2 часа)

def initialize_jsonbin():
    """Проверяет и инициализирует структуру в JSONBin, если она отсутствует"""
    try:
        users = get_users_data(force_update=True)
        if users is None:
            # Создаем начальную структуру
            initial_data = {"users": []}
            url = f"https://api.jsonbin.io/v3/b/{JSONBIN_BIN_ID}"
            headers = {
                "X-Master-Key": JSONBIN_API_KEY,
                "Content-Type": "application/json"
            }
            response = requests.put(url, json=initial_data, headers=headers)
            logger.info(f"Инициализация JSONBin: {response.status_code}, {response.text}")
            return response.status_code == 200
        return True
    except Exception as e:
        logger.error(f"Ошибка при инициализации JSONBin: {e}")
        return False

def get_users_data(force_update=False):
    """Получает данные всех пользователей из JSONBin.io с кэшированием"""
    global users_cache, last_cache_update

    current_time = time.time()

    # Используем кэш, если он актуален и не требуется принудительное обновление
    if not force_update and users_cache is not None and (current_time - last_cache_update) < CACHE_TTL:
        return users_cache

    try:
        logger.debug("Запрашиваем данные из JSONBin...")
        url = f"https://api.jsonbin.io/v3/b/{JSONBIN_BIN_ID}"
        headers = {
            "X-Master-Key": JSONBIN_API_KEY,
            "X-Bin-Meta": "false"  # Получаем только содержимое без метаданных
        }
        response = requests.get(url, headers=headers)
        logger.debug(f"Ответ от JSONBin: {response.status_code}")

        if response.status_code == 200:
            try:
                data = response.json()
                if isinstance(data, dict) and 'users' in data:
                    users_data = data['users']
                else:
                    # Если структура неправильная, инициализируем ее
                    logger.warning("Неверная структура данных, инициализируем...")
                    users_data = []
                    initialize_jsonbin()

                # Обновляем кэш
                users_cache = users_data
                last_cache_update = current_time
                logger.debug(f"Данные пользователей обновлены: {len(users_data)} записей")
                return users_data
            except json.JSONDecodeError:
                logger.error(f"Ошибка декодирования JSON: {response.text}")
                return users_cache or []
        else:
            logger.error(f"Ошибка получения данных из JSONBin: {response.status_code}, {response.text}")
            return users_cache or []  # Возвращаем старый кэш, если он есть
    except Exception as e:
        logger.error(f"Ошибка при получении данных из JSONBin: {e}")
        return users_cache or []  # Возвращаем старый кэш, если он есть

def update_users_data(users_data):
    """Обновляет данные пользователей в JSONBin.io и кэш"""
    global users_cache, last_cache_update

    try:
        logger.debug(f"Обновляем данные пользователей: {len(users_data)} записей")
        url = f"https://api.jsonbin.io/v3/b/{JSONBIN_BIN_ID}"
        headers = {
            "X-Master-Key": JSONBIN_API_KEY,
            "Content-Type": "application/json"
        }
        data = {"users": users_data}
        response = requests.put(url, json=data, headers=headers)
        logger.debug(f"Ответ на обновление данных: {response.status_code}")

        if response.status_code == 200:
            # Обновляем кэш после успешного обновления в JSONBin
            users_cache = users_data
            last_cache_update = time.time()
            return True
        else:
            logger.error(f"Ошибка обновления данных в JSONBin: {response.status_code}, {response.text}")
            return False
    except Exception as e:
        logger.error(f"Ошибка при обновлении данных в JSONBin: {e}")
        return False

def is_user_authorized(user_id):
    """Проверяет, авторизован ли пользователь, ищет его ID в кэше данных JSONBin"""
    try:
        users = get_users_data()
        if users is None:
            logger.warning("Не удалось получить данные пользователей")
            return False

        for user in users:
            if isinstance(user, dict) and user.get('user_id') == user_id:
                # Обновляем время последнего захода и записываем использование
                record_user_activity(user_id)
                logger.debug(f"Пользователь {user_id} найден в базе")
                return True

        logger.debug(f"Пользователь {user_id} не найден в базе")
        return False
    except Exception as e:
        logger.error(f"Ошибка при проверке авторизации: {e}")
        return False

def record_user_activity(user_id):
    """Записывает активность пользователя без обновления last_access, если прошло менее 6 часов"""
    try:
        users = get_users_data()
        if users is None:
            logger.error("Не удалось получить данные пользователей для записи активности")
            return False

        now = datetime.now()
        current_time = now.strftime("%Y-%m-%d %H:%M:%S")
        updated = False

        for user in users:
            if isinstance(user, dict) and user.get('user_id') == user_id:
                # Инициализируем историю использования, если её нет
                if 'usage_history' not in user:
                    user['usage_history'] = []
                
                # Добавляем текущее время в историю использования
                user['usage_history'].append(current_time)
                
                # Проверяем, нужно ли обновлять last_access
                last_access_str = user.get('last_access')
                update_last_access = True
                
                if last_access_str:
                    try:
                        last_access = datetime.strptime(last_access_str, "%Y-%m-%d %H:%M:%S")
                        time_diff = now - last_access
                        # Если прошло менее 6 часов, не обновляем last_access
                        if time_diff.total_seconds() < 6 * 3600:
                            update_last_access = False
                            logger.debug(f"Пропуск обновления времени доступа для {user_id}: прошло менее 6 часов")
                    except ValueError:
                        pass
                
                if update_last_access:
                    user['last_access'] = current_time
                
                updated = True
                break

        if updated:
            result = update_users_data(users)
            logger.debug(f"Обновление активности пользователя {user_id}: {result}")
            return result
        
        logger.debug(f"Пользователь {user_id} не найден для записи активности")
        return False
    except Exception as e:
        logger.error(f"Ошибка при записи активности пользователя: {e}")
        return False

def register_user(user_id, language):
    """Регистрирует нового пользователя в JSONBin"""
    try:
        users = get_users_data()
        if users is None:
            logger.error("Не удалось получить данные пользователей для регистрации")
            return False

        # Проверяем, существует ли пользователь
        for user in users:
            if isinstance(user, dict) and user.get('user_id') == user_id:
                logger.debug(f"Пользователь {user_id} уже зарегистрирован")
                # Записываем активность
                record_user_activity(user_id)
                return True  # Пользователь уже зарегистрирован

        # Добавляем нового пользователя
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        new_user = {
            "user_id": user_id,
            "language": language,
            "registration_time": now,
            "last_access": now,
            "usage_history": [now]  # Инициализируем историю использования
        }

        logger.debug(f"Регистрируем нового пользователя: {new_user}")
        users.append(new_user)
        result = update_users_data(users)
        logger.debug(f"Результат регистрации: {result}")
        return result
    except Exception as e:
        logger.error(f"Ошибка при регистрации пользователя: {e}")
        return False

def get_user_stats(user_id):
    """Получает статистику для конкретного пользователя"""
    try:
        users = get_users_data()
        if users is None:
            logger.error("Не удалось получить данные пользователей для статистики")
            return None

        for user in users:
            if isinstance(user, dict) and user.get('user_id') == user_id:
                return user
        
        return None
    except Exception as e:
        logger.error(f"Ошибка при получении статистики пользователя: {e}")
        return None

def get_registration_stats():
    """Возвращает статистику регистраций за разные периоды времени"""
    try:
        users = get_users_data()
        if users is None:
            logger.error("Не удалось получить данные пользователей для статистики регистраций")
            return None

        now = datetime.now()
        day_ago = now - timedelta(days=1)
        week_ago = now - timedelta(days=7)
        month_ago = now - timedelta(days=30)

        # Счетчики для разных периодов
        last_24h = 0
        last_week = 0
        last_month = 0

        for user in users:
            if not isinstance(user, dict) or 'registration_time' not in user:
                continue
            
            try:
                reg_time = datetime.strptime(user['registration_time'], "%Y-%m-%d %H:%M:%S")
                
                # Последние 24 часа
                if reg_time >= day_ago:
                    last_24h += 1
                
                # Последняя неделя
                if reg_time >= week_ago:
                    last_week += 1
                
                # Последний месяц
                if reg_time >= month_ago:
                    last_month += 1
            
            except (ValueError, TypeError) as e:
                logger.warning(f"Ошибка при парсинге даты регистрации: {e}")
                continue

        return {
            "last_24h": last_24h,
            "last_week": last_week,
            "last_month": last_month
        }
    except Exception as e:
        logger.error(f"Ошибка при получении статистики регистраций: {e}")
        return None

def get_usage_stats():
    """Возвращает статистику использования бота за разные периоды времени"""
    try:
        users = get_users_data()
        if users is None:
            logger.error("Не удалось получить данные пользователей для статистики использования")
            return None

        now = datetime.now()
        day_ago = now - timedelta(days=1)
        three_days_ago = now - timedelta(days=3)
        week_ago = now - timedelta(days=7)
        month_ago = now - timedelta(days=30)

        # Уникальные пользователи в разные периоды
        users_24h = set()
        users_3d = set()
        users_week = set()
        users_month = set()

        for user in users:
            if not isinstance(user, dict):
                continue
            
            user_id = user.get('user_id')
            if not user_id:
                continue
            
            # Проверяем историю использования
            usage_history = user.get('usage_history', [])
            for usage_time_str in usage_history:
                try:
                    usage_time = datetime.strptime(usage_time_str, "%Y-%m-%d %H:%M:%S")
                    
                    if usage_time >= day_ago:
                        users_24h.add(user_id)
                    
                    if usage_time >= three_days_ago:
                        users_3d.add(user_id)
                    
                    if usage_time >= week_ago:
                        users_week.add(user_id)
                    
                    if usage_time >= month_ago:
                        users_month.add(user_id)
                
                except ValueError:
                    continue

        return {
            "last_24h": len(users_24h),
            "last_3d": len(users_3d),
            "last_week": len(users_week),
            "last_month": len(users_month)
        }
    except Exception as e:
        logger.error(f"Ошибка при получении статистики использования: {e}")
        return None

def get_global_stats():
    """Получает общую статистику всех пользователей"""
    try:
        users = get_users_data()
        if users is None:
            logger.error("Не удалось получить данные пользователей для общей статистики")
            return None

        total_users = len(users)
        ru_users = 0
        en_users = 0

        for user in users:
            if isinstance(user, dict):
                if user.get('language') == 'RU':
                    ru_users += 1
                elif user.get('language') == 'EN':
                    en_users += 1

        # Получаем статистику регистраций
        reg_stats = get_registration_stats() or {
            "last_24h": 0,
            "last_week": 0,
            "last_month": 0
        }
        
        # Получаем статистику использования
        usage_stats = get_usage_stats() or {
            "last_24h": 0,
            "last_3d": 0,
            "last_week": 0,
            "last_month": 0
        }

        return {
            "total_users": total_users,
            "ru_users": ru_users,
            "en_users": en_users,
            "registrations": reg_stats,
            "usage": usage_stats
        }
    except Exception as e:
        logger.error(f"Ошибка при получении общей статистики: {e}")
        return None

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
            bot.edit_message_text("✅ Вы авторизованы!", chat_id, msg.message_id)
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

@bot.message_handler(commands=['stats'])
def stats_command(message):
    try:
        user_id = message.from_user.id
        chat_id = message.chat.id
        logger.info(f"Команда /stats от пользователя {user_id}")

        # Проверка авторизации
        if not is_user_authorized(user_id):
            bot.send_message(chat_id, "Вы не авторизованы. Используйте /start для регистрации.")
            return

        # Получаем статистику пользователя
        user_stats = get_user_stats(user_id)
        if not user_stats:
            bot.send_message(chat_id, "Не удалось получить вашу статистику.")
            return

        # Получаем общую статистику
        global_stats = get_global_stats()
        if not global_stats:
            bot.send_message(chat_id, "Не удалось получить общую статистику.")
            return

        # Формируем сообщение в зависимости от языка пользователя
        if user_stats.get('language') == 'RU':
            message_text = (
                f"📊 *Ваша статистика:*\n"
                f"🆔 ID: `{user_id}`\n"
                f"🌐 Язык: {user_stats.get('language')}\n"
                f"📅 Дата регистрации: {user_stats.get('registration_time')}\n"
                f"⏱ Последний вход: {user_stats.get('last_access')}\n\n"
                
                f"📈 *Общая статистика:*\n"
                f"👥 Всего пользователей: {global_stats['total_users']}\n"
                f"🇷🇺 Пользователей RU: {global_stats['ru_users']}\n"
                f"🇬🇧 Пользователей EN: {global_stats['en_users']}\n\n"
                
                f"📊 *Новые регистрации:*\n"
                f"• За 24 часа: {global_stats['registrations']['last_24h']}\n"
                f"• За неделю: {global_stats['registrations']['last_week']}\n"
                f"• За месяц: {global_stats['registrations']['last_month']}\n\n"
                
                f"📱 *Активные пользователи:*\n"
                f"• За 24 часа: {global_stats['usage']['last_24h']}\n"
                f"• За 3 дня: {global_stats['usage']['last_3d']}\n"
                f"• За неделю: {global_stats['usage']['last_week']}\n"
                f"• За месяц: {global_stats['usage']['last_month']}"
            )
        else:  # EN
            message_text = (
                f"📊 *Your statistics:*\n"
                f"🆔 ID: `{user_id}`\n"
                f"🌐 Language: {user_stats.get('language')}\n"
                f"📅 Registration date: {user_stats.get('registration_time')}\n"
                f"⏱ Last access: {user_stats.get('last_access')}\n\n"
                
                f"📈 *Global statistics:*\n"
                f"👥 Total users: {global_stats['total_users']}\n"
                f"🇷🇺 RU users: {global_stats['ru_users']}\n"
                f"🇬🇧 EN users: {global_stats['en_users']}\n\n"
                
                f"📊 *New registrations:*\n"
                f"• Last 24 hours: {global_stats['registrations']['last_24h']}\n"
                f"• Last week: {global_stats['registrations']['last_week']}\n"
                f"• Last month: {global_stats['registrations']['last_month']}\n\n"
                
                f"📱 *Active users:*\n"
                f"• Last 24 hours: {global_stats['usage']['last_24h']}\n"
                f"• Last 3 days: {global_stats['usage']['last_3d']}\n"
                f"• Last week: {global_stats['usage']['last_week']}\n"
                f"• Last month: {global_stats['usage']['last_month']}"
            )

        bot.send_message(chat_id, message_text, parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Ошибка при обработке команды /stats: {e}")
        try:
            bot.send_message(chat_id, "Произошла ошибка при получении статистики. Попробуйте позже.")
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
            bot.edit_message_text(response, call.message.chat.id, call.message.message_id)
            return

        # Регистрация пользователя в JSONBin
        try:
            if register_user(user_id, language):
                response = "📬 Запрос отправлен!" if language == 'RU' else "📬 Request submitted!"
            else:
                response = "⚠️ Ошибка регистрации" if language == 'RU' else "⚠️ Registration error"
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
