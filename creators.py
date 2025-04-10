import logging
import time
import requests
from datetime import datetime
import json
import telebot
from telebot import types

# Настройка более подробного логирования
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Константы для JSONBin.io (используем те же, что и в statistics.py)
JSONBIN_API_KEY = "$2a$10$hT79uCEaJENfQBZ7576aL.upUOtnPqJZX53sWcln0HZib/bgs.8.u"
JSONBIN_BIN_ID = "67f532028a456b796684e974"

# Локальный кэш данных пользователей для минимизации API-запросов
creators_cache = None
last_cache_update = 0
CACHE_TTL = 300  # Время жизни кэша в секундах (5 минут)

def get_creator_data(user_id):
    """Получает данные создателя для конкретного пользователя из JSONBin"""
    try:
        users = get_users_data()
        if users is None:
            logger.error("Не удалось получить данные пользователей для информации создателя")
            return None

        for user in users:
            if isinstance(user, dict) and user.get('user_id') == user_id:
                # Создаем поля для создателя по умолчанию, если они отсутствуют
                if 'balance' not in user:
                    user['balance'] = 0.0
                if 'purchased_services' not in user:
                    user['purchased_services'] = []
                
                return {
                    'user_id': user.get('user_id'),
                    'balance': user.get('balance', 0.0),
                    'language': user.get('language', 'RU'),
                    'purchased_services': user.get('purchased_services', []),
                    'subscription_status': user.get('subscription_status', 'none')
                }

        return None
    except Exception as e:
        logger.error(f"Ошибка получения данных создателя: {e}")
        return None

def get_users_data(force_update=False):
    """Получает данные всех пользователей из JSONBin.io с кэшированием"""
    global creators_cache, last_cache_update

    current_time = time.time()

    # Используем кэш, если он актуален и не требуется принудительное обновление
    if not force_update and creators_cache is not None and (current_time - last_cache_update) < CACHE_TTL:
        return creators_cache

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

                # Обновляем кэш
                creators_cache = users_data
                last_cache_update = current_time
                logger.debug(f"Данные пользователей обновлены: {len(users_data)} записей")
                return users_data
            except json.JSONDecodeError:
                logger.error(f"Ошибка декодирования JSON: {response.text}")
                return creators_cache or []
        else:
            logger.error(f"Ошибка получения данных из JSONBin: {response.status_code}, {response.text}")
            return creators_cache or []  # Возвращаем старый кэш, если он есть
    except Exception as e:
        logger.error(f"Ошибка при получении данных из JSONBin: {e}")
        return creators_cache or []  # Возвращаем старый кэш, если он есть

def update_users_data(users_data):
    """Обновляет данные пользователей в JSONBin.io и кэш"""
    global creators_cache, last_cache_update

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
            creators_cache = users_data
            last_cache_update = time.time()
            return True
        else:
            logger.error(f"Ошибка обновления данных в JSONBin: {response.status_code}, {response.text}")
            return False
    except Exception as e:
        logger.error(f"Ошибка при обновлении данных в JSONBin: {e}")
        return False

def add_balance(user_id, amount):
    """Добавляет средства на счет пользователя"""
    try:
        users = get_users_data()
        if users is None:
            logger.error("Не удалось получить данные пользователей для обновления баланса")
            return False

        updated = False
        for user in users:
            if isinstance(user, dict) and user.get('user_id') == user_id:
                # Инициализируем баланс, если его нет
                if 'balance' not in user:
                    user['balance'] = 0.0
                
                # Добавляем сумму к балансу
                user['balance'] = round(user['balance'] + amount, 2)
                updated = True
                logger.info(f"Обновлен баланс пользователя {user_id}: +{amount}, новый баланс: {user['balance']}")
                break

        if updated:
            return update_users_data(users)
        else:
            logger.warning(f"Пользователь {user_id} не найден для обновления баланса")
            return False
    except Exception as e:
        logger.error(f"Ошибка при добавлении баланса: {e}")
        return False

def purchase_service(user_id, service_id, service_name, price, details=None):
    """Записывает покупку услуги пользователем"""
    try:
        users = get_users_data()
        if users is None:
            logger.error("Не удалось получить данные пользователей для покупки услуги")
            return False

        updated = False
        for user in users:
            if isinstance(user, dict) and user.get('user_id') == user_id:
                # Проверяем, достаточно ли средств у пользователя
                current_balance = user.get('balance', 0.0)
                if current_balance < price:
                    logger.warning(f"Недостаточно средств у пользователя {user_id}: {current_balance} < {price}")
                    return "insufficient_balance"
                
                # Инициализируем purchased_services, если его нет
                if 'purchased_services' not in user:
                    user['purchased_services'] = []
                
                # Добавляем запись о покупке
                purchase = {
                    'service_id': service_id,
                    'service_name': service_name,
                    'price': price,
                    'purchase_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    'details': details or {}
                }
                
                user['purchased_services'].append(purchase)
                
                # Списываем средства
                user['balance'] = round(user['balance'] - price, 2)
                
                # Обновляем статус подписки, если это услуга подписки
                if service_id == 'subscription':
                    user['subscription_status'] = 'active'
                
                updated = True
                logger.info(f"Услуга приобретена пользователем {user_id}: {service_name}, цена: {price}")
                break

        if updated:
            if update_users_data(users):
                return "success"
            else:
                return "update_failed"
        else:
            logger.warning(f"Пользователь {user_id} не найден для покупки услуги")
            return "user_not_found"
    except Exception as e:
        logger.error(f"Ошибка при покупке услуги: {e}")
        return "error"

def check_subscription(user_id):
    """Проверяет, есть ли у пользователя активная подписка"""
    try:
        users = get_users_data()
        if users is None:
            logger.error("Не удалось получить данные пользователей для проверки подписки")
            return False

        for user in users:
            if isinstance(user, dict) and user.get('user_id') == user_id:
                # Проверяем статус подписки
                subscription_status = user.get('subscription_status', 'none')
                return subscription_status == 'active'

        return False
    except Exception as e:
        logger.error(f"Ошибка при проверке подписки: {e}")
        return False

def get_service_purchasers(service_id):
    """Получает список пользователей, купивших определенную услугу"""
    try:
        users = get_users_data()
        if users is None:
            logger.error("Не удалось получить данные пользователей для покупателей услуги")
            return None

        purchasers = []
        for user in users:
            if isinstance(user, dict) and 'purchased_services' in user:
                for service in user['purchased_services']:
                    if service.get('service_id') == service_id:
                        purchasers.append({
                            'user_id': user.get('user_id'),
                            'language': user.get('language', 'RU'),
                            'purchase_date': service.get('purchase_date'),
                            'details': service.get('details', {})
                        })

        return purchasers
    except Exception as e:
        logger.error(f"Ошибка при получении покупателей услуги: {e}")
        return None

def format_creator_stats(user_id):
    """Форматирует статистику создателя для отображения"""
    try:
        creator_data = get_creator_data(user_id)
        if not creator_data:
            return None

        language = creator_data.get('language', 'RU')
        balance = creator_data.get('balance', 0.0)
        purchased_services = creator_data.get('purchased_services', [])
        subscription_status = creator_data.get('subscription_status', 'none')

        if language == 'RU':
            status_text = "Активна" if subscription_status == 'active' else "Не активна"
            message = (
                f"👤 *Информация для создателей*\n\n"
                f"🆔 ID: `{user_id}`\n"
                f"💰 Баланс: {balance:.2f} USDT\n"
                f"🔑 Подписка: {status_text}\n\n"
            )
            
            if purchased_services:
                message += "*Приобретенные услуги:*\n"
                for idx, service in enumerate(purchased_services, 1):
                    message += (
                        f"{idx}. {service.get('service_name')}\n"
                        f"   💲 Цена: {service.get('price'):.2f} USDT\n"
                        f"   📅 Дата: {service.get('purchase_date')}\n"
                    )
                    
                    # Добавляем специфичные для услуги детали, если они есть
                    details = service.get('details', {})
                    if details and details.get('subscribers'):
                        message += f"   👥 Подписчики: {details.get('subscribers')}\n"
                    if details and details.get('channel_id'):
                        message += f"   📢 Канал: {details.get('channel_id')}\n"
                    message += "\n"
            else:
                message += "*Приобретенные услуги:* Нет\n\n"
                
            message += (
                "📣 *Доступные услуги:*\n"
                "• Обязательная подписка: 0.68 USDT - 100 подписчиков\n\n"
                "💳 *Пополнение баланса:*\n"
                "Минимальная сумма: 1 USDT\n"
                "[Пополнить через Cryptobot](http://t.me/send?start=IVxHamEz6TtA)"
            )
        else:  # EN
            status_text = "Active" if subscription_status == 'active' else "Inactive"
            message = (
                f"👤 *Creator Information*\n\n"
                f"🆔 ID: `{user_id}`\n"
                f"💰 Balance: {balance:.2f} USDT\n"
                f"🔑 Subscription: {status_text}\n\n"
            )
            
            if purchased_services:
                message += "*Purchased Services:*\n"
                for idx, service in enumerate(purchased_services, 1):
                    message += (
                        f"{idx}. {service.get('service_name')}\n"
                        f"   💲 Price: {service.get('price'):.2f} USDT\n"
                        f"   📅 Date: {service.get('purchase_date')}\n"
                    )
                    
                    # Добавляем специфичные для услуги детали, если они есть
                    details = service.get('details', {})
                    if details and details.get('subscribers'):
                        message += f"   👥 Subscribers: {details.get('subscribers')}\n"
                    if details and details.get('channel_id'):
                        message += f"   📢 Channel: {details.get('channel_id')}\n"
                    message += "\n"
            else:
                message += "*Purchased Services:* None\n\n"
                
            message += (
                "📣 *Available Services:*\n"
                "• Mandatory subscription: 0.68 USDT - 100 subscribers\n\n"
                "💳 *Add Funds:*\n"
                "Minimum amount: 1 USDT\n"
                "[Top up via Cryptobot](http://t.me/send?start=IVxHamEz6TtA)"
            )

        return message
    except Exception as e:
        logger.error(f"Ошибка форматирования статистики создателя: {e}")
        return None

def register_creators_handlers(bot):
    """Регистрирует обработчики обратного вызова для функций, связанных с создателями"""
    
    @bot.callback_query_handler(func=lambda call: call.data == 'creators_menu')
    def creators_menu_callback(call):
        try:
            user_id = call.from_user.id
            chat_id = call.message.chat.id

            # Проверка авторизации
            from statistics import is_user_authorized
            if not is_user_authorized(user_id):
                bot.answer_callback_query(call.id, "Вы не авторизованы. Используйте /start для регистрации.")
                return

            creator_message = format_creator_stats(user_id)
            if not creator_message:
                bot.answer_callback_query(call.id, "Не удалось получить информацию.")
                return

            # Создаем клавиатуру для навигации
            from statistics import get_user_language
            language = get_user_language(user_id)
            
            markup = types.InlineKeyboardMarkup(row_width=1)
            if language == 'RU':
                purchase_btn = types.InlineKeyboardButton("💰 Купить подписку", callback_data='buy_subscription')
                back_btn = types.InlineKeyboardButton("🔙 Назад", callback_data='back_to_start')
            else:  # EN
                purchase_btn = types.InlineKeyboardButton("💰 Buy Subscription", callback_data='buy_subscription')
                back_btn = types.InlineKeyboardButton("🔙 Back", callback_data='back_to_start')
            
            # Показываем кнопку покупки только если у пользователя нет активной подписки
            if not check_subscription(user_id):
                markup.add(purchase_btn)
            markup.add(back_btn)

            bot.edit_message_text(
                chat_id=chat_id,
                message_id=call.message.message_id,
                text=creator_message,
                reply_markup=markup,
                parse_mode="Markdown",
                disable_web_page_preview=False
            )

        except Exception as e:
            logger.error(f"Ошибка обработки меню создателей: {e}")
            try:
                bot.answer_callback_query(call.id, "Произошла ошибка. Попробуйте позже.")
            except:
                pass

    @bot.callback_query_handler(func=lambda call: call.data == 'buy_subscription')
    def buy_subscription_callback(call):
        try:
            user_id = call.from_user.id
            chat_id = call.message.chat.id

            # Проверка авторизации
            from statistics import is_user_authorized, get_user_language
            if not is_user_authorized(user_id):
                bot.answer_callback_query(call.id, "Вы не авторизованы. Используйте /start для регистрации.")
                return

            # Получаем язык пользователя и данные создателя
            language = get_user_language(user_id)
            creator_data = get_creator_data(user_id)
            
            if not creator_data:
                bot.answer_callback_query(call.id, "Не удалось получить данные пользователя.")
                return
                
            balance = creator_data.get('balance', 0.0)
            subscription_price = 0.68  # Цена подписки
            
            # Проверяем, есть ли у пользователя уже подписка
            if check_subscription(user_id):
                message = "Вы уже имеете активную подписку!" if language == 'RU' else "You already have an active subscription!"
                bot.answer_callback_query(call.id, message)
                return
                
            # Проверяем, достаточно ли средств у пользователя
            if balance < subscription_price:
                if language == 'RU':
                    message = (
                        f"⚠️ *Недостаточно средств*\n\n"
                        f"Ваш баланс: {balance:.2f} USDT\n"
                        f"Требуется: {subscription_price:.2f} USDT\n\n"
                        f"Пожалуйста, пополните баланс через [Cryptobot](http://t.me/send?start=IVxHamEz6TtA)."
                    )
                else:  # EN
                    message = (
                        f"⚠️ *Insufficient Funds*\n\n"
                        f"Your balance: {balance:.2f} USDT\n"
                        f"Required: {subscription_price:.2f} USDT\n\n"
                        f"Please top up your balance via [Cryptobot](http://t.me/send?start=IVxHamEz6TtA)."
                    )
                
                markup = types.InlineKeyboardMarkup(row_width=1)
                back_btn = types.InlineKeyboardButton(
                    "🔙 Назад" if language == 'RU' else "🔙 Back", 
                    callback_data='creators_menu'
                )
                markup.add(back_btn)
                
                bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=call.message.message_id,
                    text=message,
                    reply_markup=markup,
                    parse_mode="Markdown",
                    disable_web_page_preview=False
                )
                return
                
            # Создаем разметку для подтверждения
            markup = types.InlineKeyboardMarkup(row_width=2)
            if language == 'RU':
                confirm_btn = types.InlineKeyboardButton("✅ Подтвердить", callback_data='confirm_subscription')
                cancel_btn = types.InlineKeyboardButton("❌ Отмена", callback_data='creators_menu')
                message = (
                    f"🔔 *Подтверждение покупки*\n\n"
                    f"Услуга: Обязательная подписка (100 подписчиков)\n"
                    f"Цена: {subscription_price:.2f} USDT\n"
                    f"Ваш баланс: {balance:.2f} USDT\n\n"
                    f"После покупки ваш баланс составит: {(balance - subscription_price):.2f} USDT\n\n"
                    f"Подтвердите покупку:"
                )
            else:  # EN
                confirm_btn = types.InlineKeyboardButton("✅ Confirm", callback_data='confirm_subscription')
                cancel_btn = types.InlineKeyboardButton("❌ Cancel", callback_data='creators_menu')
                message = (
                    f"🔔 *Purchase Confirmation*\n\n"
                    f"Service: Mandatory subscription (100 subscribers)\n"
                    f"Price: {subscription_price:.2f} USDT\n"
                    f"Your balance: {balance:.2f} USDT\n\n"
                    f"After purchase, your balance will be: {(balance - subscription_price):.2f} USDT\n\n"
                    f"Confirm purchase:"
                )
                
            markup.add(confirm_btn, cancel_btn)
            
            bot.edit_message_text(
                chat_id=chat_id,
                message_id=call.message.message_id,
                text=message,
                reply_markup=markup,
                parse_mode="Markdown"
            )

        except Exception as e:
            logger.error(f"Ошибка обработки покупки подписки: {e}")
            try:
                bot.answer_callback_query(call.id, "Произошла ошибка. Попробуйте позже.")
            except:
                pass

    @bot.callback_query_handler(func=lambda call: call.data == 'confirm_subscription')
    def confirm_subscription_callback(call):
        try:
            user_id = call.from_user.id
            chat_id = call.message.chat.id

            # Проверка авторизации
            from statistics import is_user_authorized, get_user_language
            if not is_user_authorized(user_id):
                bot.answer_callback_query(call.id, "Вы не авторизованы. Используйте /start для регистрации.")
                return

            # Получаем язык пользователя
            language = get_user_language(user_id)
            
            # Обрабатываем покупку
            subscription_price = 0.68
            purchase_details = {'subscribers': 100}
            
            purchase_result = purchase_service(
                user_id, 
                'subscription', 
                'Mandatory Subscription' if language == 'EN' else 'Обязательная подписка', 
                subscription_price, 
                purchase_details
            )
            
            markup = types.InlineKeyboardMarkup(row_width=1)
            back_btn = types.InlineKeyboardButton(
                "🔙 Назад" if language == 'RU' else "🔙 Back", 
                callback_data='creators_menu'
            )
            markup.add(back_btn)
            
            if purchase_result == "success":
                if language == 'RU':
                    message = (
                        f"✅ *Покупка успешно выполнена!*\n\n"
                        f"Услуга: Обязательная подписка (100 подписчиков)\n"
                        f"Списано: {subscription_price:.2f} USDT\n\n"
                        f"Ваша подписка активирована. Теперь вы можете полностью использовать бота."
                    )
                else:  # EN
                    message = (
                        f"✅ *Purchase Successful!*\n\n"
                        f"Service: Mandatory subscription (100 subscribers)\n"
                        f"Charged: {subscription_price:.2f} USDT\n\n"
                        f"Your subscription has been activated. You can now fully use the bot."
                    )
            elif purchase_result == "insufficient_balance":
                if language == 'RU':
                    message = (
                        f"⚠️ *Недостаточно средств*\n\n"
                        f"Пожалуйста, пополните баланс через [Cryptobot](http://t.me/send?start=IVxHamEz6TtA) "
                        f"и повторите попытку."
                    )
                else:  # EN
                    message = (
                        f"⚠️ *Insufficient Funds*\n\n"
                        f"Please top up your balance via [Cryptobot](http://t.me/send?start=IVxHamEz6TtA) "
                        f"and try again."
                    )
            else:
                if language == 'RU':
                    message = (
                        f"❌ *Ошибка при выполнении покупки*\n\n"
                        f"Пожалуйста, попробуйте позже или обратитесь в поддержку."
                    )
                else:  # EN
                    message = (
                        f"❌ *Error Processing Purchase*\n\n"
                        f"Please try again later or contact support."
                    )
            
            bot.edit_message_text(
                chat_id=chat_id,
                message_id=call.message.message_id,
                text=message,
                reply_markup=markup,
                parse_mode="Markdown",
                disable_web_page_preview=False
            )

        except Exception as e:
            logger.error(f"Ошибка подтверждения подписки: {e}")
            try:
                bot.answer_callback_query(call.id, "Произошла ошибка. Попробуйте позже.")
            except:
                pass
