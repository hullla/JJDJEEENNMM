import telebot
from telebot import types
import logging
import time
import requests
from datetime import datetime, timedelta
import json
import random
import math
from collections import defaultdict

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
stats_cache = None
last_cache_update = 0
last_stats_calculation = 0
CACHE_TTL = 7200  # Время жизни кэша в секундах (2 часа)
STATS_TTL = 3600  # Время жизни кэша статистики (1 час)

# Типы активностей пользователя
ACTIVITY_TYPES = {
    "LOGIN": "login",
    "COMMAND": "command",
    "INTERACTION": "interaction"
}

def initialize_jsonbin():
    """Проверяет и инициализирует структуру в JSONBin, если она отсутствует"""
    try:
        users = get_users_data(force_update=True)
        if users is None:
            # Создаем начальную структуру
            initial_data = {
                "users": [],
                "meta": {
                    "last_update": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "version": "2.0"
                }
            }
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
        data = {
            "users": users_data,
            "meta": {
                "last_update": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "version": "2.0"
            }
        }
        response = requests.put(url, json=data, headers=headers)
        logger.debug(f"Ответ на обновление данных: {response.status_code}")

        if response.status_code == 200:
            # Обновляем кэш после успешного обновления в JSONBin
            users_cache = users_data
            last_cache_update = time.time()
            # Сбрасываем кэш статистики
            global stats_cache, last_stats_calculation
            stats_cache = None
            last_stats_calculation = 0
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
                record_user_activity(user_id, ACTIVITY_TYPES["LOGIN"])
                logger.debug(f"Пользователь {user_id} найден в базе")
                return True

        logger.debug(f"Пользователь {user_id} не найден в базе")
        return False
    except Exception as e:
        logger.error(f"Ошибка при проверке авторизации: {e}")
        return False

def record_user_activity(user_id, activity_type, details=None):
    """Записывает активность пользователя с дополнительной информацией"""
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
                # Инициализируем историю активности, если её нет
                if 'activity_history' not in user:
                    user['activity_history'] = []
                
                # Создаем новую запись активности
                activity_record = {
                    "timestamp": current_time,
                    "type": activity_type,
                }
                
                # Добавляем детали активности, если они есть
                if details:
                    activity_record["details"] = details
                
                # Добавляем текущую активность в историю
                user['activity_history'].append(activity_record)
                
                # Ограничиваем размер истории активности (хранить последние 100 записей)
                if len(user['activity_history']) > 100:
                    user['activity_history'] = user['activity_history'][-100:]
                
                # Проверяем, нужно ли обновлять last_access
                last_access_str = user.get('last_access')
                update_last_access = True
                
                if last_access_str and activity_type == ACTIVITY_TYPES["LOGIN"]:
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

                # Обновляем счетчики активности, если их нет
                if 'activity_counts' not in user:
                    user['activity_counts'] = {}
                
                # Увеличиваем счетчик для этого типа активности
                if activity_type not in user['activity_counts']:
                    user['activity_counts'][activity_type] = 0
                    
                user['activity_counts'][activity_type] += 1
                
                # Рассчитываем уровень вовлеченности (engagement score)
                calculate_engagement_score(user)
                
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

def calculate_engagement_score(user):
    """Рассчитывает оценку вовлеченности пользователя на основе его активности"""
    try:
        # Базовая формула: учитываем частоту и недавность активности
        now = datetime.now()
        activity_history = user.get('activity_history', [])
        
        if not activity_history:
            user['engagement_score'] = 0
            return
        
        # Получаем недавнюю активность (за последние 30 дней)
        recent_activity = []
        thirty_days_ago = now - timedelta(days=30)
        
        for activity in activity_history:
            try:
                activity_time = datetime.strptime(activity.get('timestamp'), "%Y-%m-%d %H:%M:%S")
                if activity_time >= thirty_days_ago:
                    recent_activity.append(activity)
            except (ValueError, TypeError):
                continue
        
        # Если нет недавней активности
        if not recent_activity:
            user['engagement_score'] = 0
            return
        
        # Частота: количество действий за последние 30 дней
        frequency = len(recent_activity)
        
        # Недавность: дни с последней активности
        try:
            last_activity = datetime.strptime(recent_activity[-1].get('timestamp'), "%Y-%m-%d %H:%M:%S")
            days_since_last = (now - last_activity).days
        except (ValueError, TypeError, IndexError):
            days_since_last = 30
        
        # Разнообразие: разные типы действий
        activity_types = set()
        for activity in recent_activity:
            activity_types.add(activity.get('type'))
        diversity = len(activity_types)
        
        # Формула оценки: (частота * разнообразие) / (1 + недавность)
        # Чем больше активности и разных типов, тем выше оценка
        # Чем больше дней прошло с последней активности, тем ниже оценка
        engagement_score = (frequency * diversity) / (1 + days_since_last)
        
        # Нормализуем оценку от 0 до 100
        normalized_score = min(100, int(engagement_score * 10))
        
        user['engagement_score'] = normalized_score
    except Exception as e:
        logger.error(f"Ошибка при расчете engagement score: {e}")
        user['engagement_score'] = 0

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
                record_user_activity(user_id, ACTIVITY_TYPES["LOGIN"])
                return True  # Пользователь уже зарегистрирован

        # Добавляем нового пользователя
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        new_user = {
            "user_id": user_id,
            "language": language,
            "registration_time": now,
            "last_access": now,
            "activity_history": [{
                "timestamp": now,
                "type": ACTIVITY_TYPES["LOGIN"]
            }],
            "activity_counts": {
                ACTIVITY_TYPES["LOGIN"]: 1
            },
            "engagement_score": 10  # Начальная оценка вовлеченности
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

def calculate_global_stats():
    """Рассчитывает глобальную статистику на основе данных пользователей"""
    global stats_cache, last_stats_calculation
    
    current_time = time.time()
    
    # Используем кэш, если он актуален
    if stats_cache is not None and (current_time - last_stats_calculation) < STATS_TTL:
        return stats_cache
    
    try:
        users = get_users_data()
        if users is None:
            logger.error("Не удалось получить данные пользователей для глобальной статистики")
            return None

        now = datetime.now()
        day_ago = now - timedelta(days=1)
        three_days_ago = now - timedelta(days=3)
        week_ago = now - timedelta(days=7)
        month_ago = now - timedelta(days=30)
        
        # Базовые счетчики
        total_users = len(users)
        ru_users = 0
        en_users = 0
        
        # Словари для хранения идентификаторов активных пользователей
        new_registrations = {
            "last_24h": [],
            "last_week": [],
            "last_month": []
        }
        
        active_users = {
            "last_24h": set(),
            "last_3d": set(),
            "last_week": set(),
            "last_month": set()
        }
        
        # Данные для трендов и инсайтов
        hourly_activity = defaultdict(int)
        daily_activity = defaultdict(int)
        weekday_activity = defaultdict(int)
        command_usage = defaultdict(int)
        
        # Список оценок вовлеченности для расчета процентилей
        engagement_scores = []
        
        # Проходим по всем пользователям
        for user in users:
            if not isinstance(user, dict):
                continue
                
            user_id = user.get('user_id')
            if not user_id:
                continue
                
            # Подсчитываем пользователей по языкам
            if user.get('language') == 'RU':
                ru_users += 1
            elif user.get('language') == 'EN':
                en_users += 1
                
            # Добавляем оценку вовлеченности
            engagement_score = user.get('engagement_score')
            if engagement_score is not None:
                engagement_scores.append(engagement_score)
                
            # Проверяем регистрации
            try:
                reg_time = datetime.strptime(user.get('registration_time', ""), "%Y-%m-%d %H:%M:%S")
                
                if reg_time >= day_ago:
                    new_registrations["last_24h"].append(user_id)
                    
                if reg_time >= week_ago:
                    new_registrations["last_week"].append(user_id)
                    
                if reg_time >= month_ago:
                    new_registrations["last_month"].append(user_id)
            except (ValueError, TypeError):
                pass
                
            # Проверяем активность
            activity_history = user.get('activity_history', [])
            
            for activity in activity_history:
                if not isinstance(activity, dict):
                    continue
                    
                try:
                    activity_time = datetime.strptime(activity.get('timestamp', ""), "%Y-%m-%d %H:%M:%S")
                    
                    # Записываем информацию для трендов
                    hour = activity_time.hour
                    day = activity_time.day
                    weekday = activity_time.weekday()
                    
                    hourly_activity[hour] += 1
                    daily_activity[day] += 1
                    weekday_activity[weekday] += 1
                    
                    # Если это команда, подсчитываем её использование
                    if activity.get('type') == ACTIVITY_TYPES["COMMAND"] and activity.get('details'):
                        command = activity.get('details').get('command')
                        if command:
                            command_usage[command] += 1
                    
                    # Проверяем период активности
                    if activity_time >= day_ago:
                        active_users["last_24h"].add(user_id)
                        
                    if activity_time >= three_days_ago:
                        active_users["last_3d"].add(user_id)
                        
                    if activity_time >= week_ago:
                        active_users["last_week"].add(user_id)
                        
                    if activity_time >= month_ago:
                        active_users["last_month"].add(user_id)
                        
                except (ValueError, TypeError):
                    continue
        
        # Рассчитываем процентили вовлеченности
        engagement_percentiles = {}
        if engagement_scores:
            engagement_scores.sort()
            engagement_percentiles = {
                "p25": engagement_scores[int(len(engagement_scores) * 0.25)] if len(engagement_scores) > 4 else 0,
                "p50": engagement_scores[int(len(engagement_scores) * 0.5)] if len(engagement_scores) > 2 else 0,
                "p75": engagement_scores[int(len(engagement_scores) * 0.75)] if len(engagement_scores) > 4 else 0,
                "p90": engagement_scores[int(len(engagement_scores) * 0.9)] if len(engagement_scores) > 10 else 0
            }
        
        # Находим наиболее активные часы
        peak_hours = []
        if hourly_activity:
            max_activity = max(hourly_activity.values())
            for hour, count in hourly_activity.items():
                if count >= max_activity * 0.8:  # 80% от пика считаем пиковыми часами
                    peak_hours.append(hour)
        
        # Находим самые используемые команды
        top_commands = []
        if command_usage:
            sorted_commands = sorted(command_usage.items(), key=lambda x: x[1], reverse=True)
            top_commands = sorted_commands[:3]  # Топ-3 команды
        
        # Рассчитываем тренды роста
        growth_rates = {}
        
        # Темп роста новых пользователей по сравнению с предыдущим месяцем
        users_month_ago = total_users - len(new_registrations["last_month"])
        if users_month_ago > 0:
            growth_rates["monthly_growth"] = round((len(new_registrations["last_month"]) / users_month_ago) * 100, 1)
        else:
            growth_rates["monthly_growth"] = 100  # Если раньше не было пользователей
        
        # Темп роста за неделю
        users_week_ago = total_users - len(new_registrations["last_week"])
        if users_week_ago > 0:
            growth_rates["weekly_growth"] = round((len(new_registrations["last_week"]) / users_week_ago) * 100, 1)
        else:
            growth_rates["weekly_growth"] = 100
        
        # Сохраняем коэффициент удержания (retention) 
        # Процент пользователей, зарегистрированных более недели назад, которые были активны за последнюю неделю
        old_users = [u.get('user_id') for u in users if isinstance(u, dict) and 
                    u.get('registration_time') and 
                    datetime.strptime(u.get('registration_time'), "%Y-%m-%d %H:%M:%S") < week_ago]
                    
        active_old_users = [uid for uid in old_users if uid in active_users["last_week"]]
        
        retention_rate = 0
        if old_users:
            retention_rate = round((len(active_old_users) / len(old_users)) * 100, 1)
        
        # Собираем все статистики в одну структуру
        stats = {
            "total_users": total_users,
            "ru_users": ru_users,
            "en_users": en_users,
            "registrations": {
                "last_24h": len(new_registrations["last_24h"]),
                "last_week": len(new_registrations["last_week"]),
                "last_month": len(new_registrations["last_month"])
            },
            "usage": {
                "last_24h": len(active_users["last_24h"]),
                "last_3d": len(active_users["last_3d"]),
                "last_week": len(active_users["last_week"]),
                "last_month": len(active_users["last_month"])
            },
            "engagement": engagement_percentiles,
            "trends": {
                "peak_hours": peak_hours,
                "weekday_activity": dict(weekday_activity),
                "top_commands": dict(top_commands) if not top_commands else {cmd: count for cmd, count in top_commands}
            },
            "growth": growth_rates,
            "retention": retention_rate
        }
        
        # Сохраняем в кэш и возвращаем
        stats_cache = stats
        last_stats_calculation = current_time
        
        return stats
    except Exception as e:
        logger.error(f"Ошибка при расчете глобальной статистики: {e}")
        return None

def get_user_percentile(user_id):
    """Определяет, в каком процентиле находится пользователь по вовлеченности"""
    try:
        user_stats = get_user_stats(user_id)
        if not user_stats or 'engagement_score' not in user_stats:
            return None
            
        user_score = user_stats['engagement_score']
        
        global_stats = calculate_global_stats()
        if not global_stats or 'engagement' not in global_stats:
            return None
            
        engagement = global_stats['engagement']
        
        if user_score <= engagement.get('p25', 0):
            return "bottom25"
        elif user_score <= engagement.get('p50', 0):
            return "bottom50"
        elif user_score <= engagement.get('p75', 0):
            return "top50"
        elif user_score <= engagement.get('p90', 0):
            return "top25"
        else:
            return "top10"
    except Exception as e:
        logger.error(f"Ошибка при определении процентиля пользователя: {e}")
        return None

def get_personalized_insights(user_id):
    """Генерирует персонализированные инсайты для пользователя"""
    try:
        user_stats = get_user_stats(user_id)
        if not user_stats:
            return []
            
        global_stats = calculate_global_stats()
        if not global_stats:
            return []
            
        insights = []
        
        # Получаем информацию о пользователе
        engagement_score = user_stats.get('engagement_score', 0)
        percentile = get_user_percentile(user_id)
        language = user_stats.get('language', 'RU')
        
        # Определяем тренды активности пользователя
        activity_history = user_stats.get('activity_history', [])
        recent_activity = [a for a in activity_history if a.get('timestamp') 
                         and datetime.strptime(a.get('timestamp'), "%Y-%m-%d %H:%M:%S") 
                         >= (datetime.now() - timedelta(days=30))]
        
        # Если мало активности, предлагаем вернуться
        if len(recent_activity) < 3:
            if language == 'RU':
                insights.append("🔔 Мы скучаем по вам! Ваш уровень активности ниже среднего.")
            else:
                insights.append("🔔 We miss you! Your activity level is below average.")
        
        # Информация о процентиле пользователя
        if percentile:
            if language == 'RU':
                if percentile == "top10":
                    insights.append("🏆 Вы входите в топ-10% самых активных пользователей!")
                elif percentile == "top25":
                    insights.append("🥇 Вы входите в топ-25% активных пользователей.")
                elif percentile == "top50":
                    insights.append("🥈 Ваша активность выше среднего уровня.")
            else:
                if percentile == "top10":
                    insights.append("🏆 You are in the top 10% of most active users!")
                elif percentile == "top25":
                    insights.append("🥇 You are in the top 25% of active users.")
                elif percentile == "top50":
                    insights.append("🥈 Your activity is above average.")
        
        # Рекомендации на основе пиковых часов активности
        peak_hours = global_stats.get('trends', {}).get('peak_hours', [])
        if peak_hours:
            peak_hours_formatted = ", ".join([f"{h}:00" for h in sorted(peak_hours)])
            if language == 'RU':
                insights.append(f"⏰ Пиковые часы активности бота: {peak_hours_formatted}")
            else:
                insights.append(f"⏰ Peak bot activity hours: {peak_hours_formatted}")
        
        # Тренды роста сообщества
        growth = global_stats.get('growth', {}).get('weekly_growth')
        if growth and growth > 10:
            if language == 'RU':
                insights.append(f"📈 Наше сообщество растет! +{growth}% за последнюю неделю.")
            else:
                insights.append(f"📈 Our community is growing! +{growth}% over the last week.")
        
        # Если недостаточно инсайтов, добавляем общие данные
        if len(insights) < 2:
            retention = global_stats.get('retention')
            if retention:
                if language == 'RU':
                    insights.append(f"📊 Показатель удержания пользователей: {retention}%")
                else:
                    insights.append(f"📊 User retention rate: {retention}%")
        
        return insights
    except Exception as e:
        logger.error(f"Ошибка при генерации инсайтов: {e}")
        return []

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
            # Записываем использование команды
            record_user_activity(user_id, ACTIVITY_TYPES["COMMAND"], {"command": "start"})
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

        # Записываем использование команды
        record_user_activity(user_id, ACTIVITY_TYPES["COMMAND"], {"command": "stats"})

        # Анимация загрузки
        load_msg = bot.send_message(chat_id, "📊 Анализируем данные...")

        # Получаем статистику пользователя
        user_stats = get_user_stats(user_id)
        if not user_stats:
            bot.edit_message_text("Не удалось получить вашу статистику.", chat_id, load_msg.message_id)
            return

        # Получаем персонализированные инсайты
        insights = get_personalized_insights(user_id)

        # Получаем общую статистику
        global_stats = calculate_global_stats()
        if not global_stats:
            bot.edit_message_text("Не удалось получить общую статистику.", chat_id, load_msg.message_id)
            return

        # Рассчитываем дополнительные метрики
        active_ratio_24h = round((global_stats['usage']['last_24h'] / global_stats['total_users']) * 100, 1) if global_stats['total_users'] > 0 else 0
        active_ratio_week = round((global_stats['usage']['last_week'] / global_stats['total_users']) * 100, 1) if global_stats['total_users'] > 0 else 0
        
        # Получаем процентиль пользователя
        percentile = get_user_percentile(user_id)
        
        # Формируем сообщение в зависимости от языка пользователя
        if user_stats.get('language') == 'RU':
            # Базовая информация о пользователе
            message_text = (
                f"📊 *Ваша статистика:*\n"
                f"🆔 ID: `{user_id}`\n"
                f"🌐 Язык: {user_stats.get('language')}\n"
                f"📅 Дата регистрации: {user_stats.get('registration_time')}\n"
                f"⏱ Последний вход: {user_stats.get('last_access')}\n"
            )
            
            # Информация о вовлеченности
            engagement_score = user_stats.get('engagement_score', 0)
            message_text += f"🔥 Индекс вовлеченности: {engagement_score}/100\n"
            
            # Добавляем информацию о процентиле, если она есть
            if percentile:
                percentile_emoji = "🥉" if percentile in ["bottom25", "bottom50"] else "🥈" if percentile == "top50" else "🥇" if percentile == "top25" else "🏆"
                percentile_text = "нижние 25%" if percentile == "bottom25" else "нижние 50%" if percentile == "bottom50" else "верхние 50%" if percentile == "top50" else "верхние 25%" if percentile == "top25" else "топ 10%"
                message_text += f"{percentile_emoji} Вы в группе: {percentile_text}\n\n"
            else:
                message_text += "\n"
            
            # Информация о сообществе
            message_text += (
                f"📈 *Общая статистика:*\n"
                f"👥 Всего пользователей: {global_stats['total_users']}\n"
                f"🇷🇺 Пользователей RU: {global_stats['ru_users']} ({round(global_stats['ru_users']/global_stats['total_users']*100) if global_stats['total_users'] > 0 else 0}%)\n"
                f"🇬🇧 Пользователей EN: {global_stats['en_users']} ({round(global_stats['en_users']/global_stats['total_users']*100) if global_stats['total_users'] > 0 else 0}%)\n\n"
            )
            
            # Информация о росте и активности
            message_text += (
                f"📊 *Динамика сообщества:*\n"
                f"• Новых за 24ч: {global_stats['registrations']['last_24h']}\n"
                f"• Новых за неделю: {global_stats['registrations']['last_week']}\n"
                f"• Прирост за неделю: +{global_stats.get('growth', {}).get('weekly_growth', 0)}%\n"
                f"• Коэффициент удержания: {global_stats.get('retention', 0)}%\n\n"
                
                f"📱 *Активность пользователей:*\n"
                f"• Активны сегодня: {global_stats['usage']['last_24h']} ({active_ratio_24h}%)\n"
                f"• Активны за 3 дня: {global_stats['usage']['last_3d']}\n"
                f"• Активны за неделю: {global_stats['usage']['last_week']} ({active_ratio_week}%)\n"
            )
            
            # Добавляем инсайты, если они есть
            if insights:
                message_text += f"\n🔍 *Персональные инсайты:*\n"
                for insight in insights:
                    message_text += f"• {insight}\n"
        else:  # EN
            # User stats
            message_text = (
                f"📊 *Your statistics:*\n"
                f"🆔 ID: `{user_id}`\n"
                f"🌐 Language: {user_stats.get('language')}\n"
                f"📅 Registration date: {user_stats.get('registration_time')}\n"
                f"⏱ Last access: {user_stats.get('last_access')}\n"
            )
            
            # Engagement information
            engagement_score = user_stats.get('engagement_score', 0)
            message_text += f"🔥 Engagement score: {engagement_score}/100\n"
            
            # Add percentile information if available
            if percentile:
                percentile_emoji = "🥉" if percentile in ["bottom25", "bottom50"] else "🥈" if percentile == "top50" else "🥇" if percentile == "top25" else "🏆"
                percentile_text = "bottom 25%" if percentile == "bottom25" else "bottom 50%" if percentile == "bottom50" else "top 50%" if percentile == "top50" else "top 25%" if percentile == "top25" else "top 10%"
                message_text += f"{percentile_emoji} Your group: {percentile_text}\n\n"
            else:
                message_text += "\n"
            
            # Community information
            message_text += (
                f"📈 *Global statistics:*\n"
                f"👥 Total users: {global_stats['total_users']}\n"
                f"🇷🇺 RU users: {global_stats['ru_users']} ({round(global_stats['ru_users']/global_stats['total_users']*100) if global_stats['total_users'] > 0 else 0}%)\n"
                f"🇬🇧 EN users: {global_stats['en_users']} ({round(global_stats['en_users']/global_stats['total_users']*100) if global_stats['total_users'] > 0 else 0}%)\n\n"
            )
            
            # Growth and activity information
            message_text += (
                f"📊 *Community dynamics:*\n"
                f"• New in 24h: {global_stats['registrations']['last_24h']}\n"
                f"• New this week: {global_stats['registrations']['last_week']}\n"
                f"• Weekly growth: +{global_stats.get('growth', {}).get('weekly_growth', 0)}%\n"
                f"• Retention rate: {global_stats.get('retention', 0)}%\n\n"
                
                f"📱 *User activity:*\n"
                f"• Active today: {global_stats['usage']['last_24h']} ({active_ratio_24h}%)\n"
                f"• Active in 3 days: {global_stats['usage']['last_3d']}\n"
                f"• Active this week: {global_stats['usage']['last_week']} ({active_ratio_week}%)\n"
            )
            
            # Add insights if available
            if insights:
                message_text += f"\n🔍 *Personal insights:*\n"
                for insight in insights:
                    message_text += f"• {insight}\n"

        bot.edit_message_text(message_text, chat_id, load_msg.message_id, parse_mode="Markdown")
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
