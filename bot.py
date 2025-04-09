import telebot
from telebot import types
import logging
import time
import requests
from datetime import datetime, timedelta
import json
import random  # Для генерации случайного идентификатора сессии
import hashlib  # Для улучшения анонимизации данных
import statistics  # Для расчета статистических показателей

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

# Локальный кэш данных и статистики
users_cache = None
stats_cache = None
last_cache_update = 0
last_stats_update = 0
CACHE_TTL = 7200  # Время жизни кэша в секундах (2 часа)
STATS_TTL = 3600  # Время жизни кэша статистики (1 час)
SESSION_ID = hashlib.md5(str(random.randint(1, 1000000)).encode()).hexdigest()[:8]

def initialize_jsonbin():
    """Проверяет и инициализирует структуру в JSONBin, если она отсутствует"""
    try:
        users = get_users_data(force_update=True)
        if users is None:
            # Создаем начальную структуру
            initial_data = {
                "users": [],
                "meta": {
                    "last_analytics_update": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "bot_version": "2.0.0",
                    "session_id": SESSION_ID
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
        logger.debug(f"[{SESSION_ID}] Запрашиваем данные из JSONBin...")
        url = f"https://api.jsonbin.io/v3/b/{JSONBIN_BIN_ID}"
        headers = {
            "X-Master-Key": JSONBIN_API_KEY,
            "X-Bin-Meta": "false"  # Получаем только содержимое без метаданных
        }
        response = requests.get(url, headers=headers)
        logger.debug(f"[{SESSION_ID}] Ответ от JSONBin: {response.status_code}")

        if response.status_code == 200:
            try:
                data = response.json()
                if isinstance(data, dict) and 'users' in data:
                    users_data = data['users']
                else:
                    # Если структура неправильная, инициализируем ее
                    logger.warning(f"[{SESSION_ID}] Неверная структура данных, инициализируем...")
                    users_data = []
                    initialize_jsonbin()

                # Обновляем кэш
                users_cache = users_data
                last_cache_update = current_time
                logger.debug(f"[{SESSION_ID}] Данные пользователей обновлены: {len(users_data)} записей")
                return users_data
            except json.JSONDecodeError:
                logger.error(f"[{SESSION_ID}] Ошибка декодирования JSON: {response.text}")
                return users_cache or []
        else:
            logger.error(f"[{SESSION_ID}] Ошибка получения данных из JSONBin: {response.status_code}, {response.text}")
            return users_cache or []  # Возвращаем старый кэш, если он есть
    except Exception as e:
        logger.error(f"[{SESSION_ID}] Ошибка при получении данных из JSONBin: {e}")
        return users_cache or []  # Возвращаем старый кэш, если он есть

def update_users_data(users_data):
    """Обновляет данные пользователей в JSONBin.io и кэш"""
    global users_cache, last_cache_update

    try:
        logger.debug(f"[{SESSION_ID}] Обновляем данные пользователей: {len(users_data)} записей")
        url = f"https://api.jsonbin.io/v3/b/{JSONBIN_BIN_ID}"
        headers = {
            "X-Master-Key": JSONBIN_API_KEY,
            "Content-Type": "application/json"
        }
        
        # Сохраняем метаданные, если они были
        meta = {
            "last_update": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "bot_version": "2.0.0",
            "session_id": SESSION_ID,
            "update_count": get_update_count() + 1
        }
        
        data = {"users": users_data, "meta": meta}
        response = requests.put(url, json=data, headers=headers)
        logger.debug(f"[{SESSION_ID}] Ответ на обновление данных: {response.status_code}")

        if response.status_code == 200:
            # Обновляем кэш после успешного обновления в JSONBin
            users_cache = users_data
            last_cache_update = time.time()
            
            # Сбрасываем кэш статистики, так как данные изменились
            global stats_cache, last_stats_update
            stats_cache = None
            last_stats_update = 0
            
            return True
        else:
            logger.error(f"[{SESSION_ID}] Ошибка обновления данных в JSONBin: {response.status_code}, {response.text}")
            return False
    except Exception as e:
        logger.error(f"[{SESSION_ID}] Ошибка при обновлении данных в JSONBin: {e}")
        return False

def get_update_count():
    """Получает счетчик обновлений из метаданных"""
    try:
        url = f"https://api.jsonbin.io/v3/b/{JSONBIN_BIN_ID}"
        headers = {
            "X-Master-Key": JSONBIN_API_KEY,
            "X-Bin-Meta": "false"
        }
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            try:
                data = response.json()
                if isinstance(data, dict) and 'meta' in data and 'update_count' in data['meta']:
                    return data['meta']['update_count']
            except:
                pass
        return 0
    except:
        return 0

def is_user_authorized(user_id):
    """Проверяет, авторизован ли пользователь, ищет его ID в кэше данных JSONBin"""
    try:
        users = get_users_data()
        if users is None:
            logger.warning(f"[{SESSION_ID}] Не удалось получить данные пользователей")
            return False

        for user in users:
            if isinstance(user, dict) and user.get('user_id') == user_id:
                # Обновляем время последнего захода и записываем использование
                record_user_activity(user_id)
                logger.debug(f"[{SESSION_ID}] Пользователь {user_id} найден в базе")
                return True

        logger.debug(f"[{SESSION_ID}] Пользователь {user_id} не найден в базе")
        return False
    except Exception as e:
        logger.error(f"[{SESSION_ID}] Ошибка при проверке авторизации: {e}")
        return False

def record_user_activity(user_id, action_type="default"):
    """
    Записывает активность пользователя с дополнительной информацией
    action_type: тип действия (start, stats, message, и т.д.)
    """
    try:
        users = get_users_data()
        if users is None:
            logger.error(f"[{SESSION_ID}] Не удалось получить данные пользователей для записи активности")
            return False

        now = datetime.now()
        current_time = now.strftime("%Y-%m-%d %H:%M:%S")
        updated = False

        for user in users:
            if isinstance(user, dict) and user.get('user_id') == user_id:
                # Инициализируем историю использования, если её нет
                if 'usage_history' not in user:
                    user['usage_history'] = []
                
                # Добавляем подробную информацию об активности
                activity_record = {
                    "timestamp": current_time,
                    "action": action_type,
                    "session_id": SESSION_ID,
                    "time_of_day": now.hour,  # Для анализа активности по времени суток
                    "day_of_week": now.weekday()  # Для анализа активности по дням недели
                }
                
                # Ограничиваем историю максимум 100 записями
                user['usage_history'].append(activity_record)
                if len(user['usage_history']) > 100:
                    user['usage_history'] = user['usage_history'][-100:]
                
                # Обновляем время последнего доступа
                user['last_access'] = current_time
                
                # Обновляем счетчики активности
                if 'activity_metrics' not in user:
                    user['activity_metrics'] = {
                        "total_actions": 0,
                        "actions_by_type": {}
                    }
                
                user['activity_metrics']['total_actions'] += 1
                
                if action_type in user['activity_metrics']['actions_by_type']:
                    user['activity_metrics']['actions_by_type'][action_type] += 1
                else:
                    user['activity_metrics']['actions_by_type'][action_type] = 1
                
                updated = True
                break

        if updated:
            result = update_users_data(users)
            logger.debug(f"[{SESSION_ID}] Обновление активности пользователя {user_id}: {result}")
            return result
        
        logger.debug(f"[{SESSION_ID}] Пользователь {user_id} не найден для записи активности")
        return False
    except Exception as e:
        logger.error(f"[{SESSION_ID}] Ошибка при записи активности пользователя: {e}")
        return False

def register_user(user_id, language):
    """Регистрирует нового пользователя в JSONBin с расширенными данными"""
    try:
        users = get_users_data()
        if users is None:
            logger.error(f"[{SESSION_ID}] Не удалось получить данные пользователей для регистрации")
            return False

        # Проверяем, существует ли пользователь
        for user in users:
            if isinstance(user, dict) and user.get('user_id') == user_id:
                logger.debug(f"[{SESSION_ID}] Пользователь {user_id} уже зарегистрирован")
                # Записываем активность с типом "re_register"
                record_user_activity(user_id, "re_register")
                return True  # Пользователь уже зарегистрирован

        # Добавляем нового пользователя
        now = datetime.now()
        now_str = now.strftime("%Y-%m-%d %H:%M:%S")
        
        new_user = {
            "user_id": user_id,
            "language": language,
            "registration_time": now_str,
            "last_access": now_str,
            "usage_history": [{
                "timestamp": now_str,
                "action": "register",
                "session_id": SESSION_ID,
                "time_of_day": now.hour,
                "day_of_week": now.weekday()
            }],
            "activity_metrics": {
                "total_actions": 1,
                "actions_by_type": {"register": 1}
            },
            "user_segment": classify_user_segment(None),  # Начальная сегментация
            "registration_metadata": {
                "day_of_week": now.weekday(),
                "hour_of_day": now.hour,
                "month": now.month,
                "year": now.year,
                "quarter": (now.month - 1) // 3 + 1,  # Квартал года (1-4)
                "session_id": SESSION_ID
            }
        }

        logger.debug(f"[{SESSION_ID}] Регистрируем нового пользователя: {new_user}")
        users.append(new_user)
        result = update_users_data(users)
        logger.debug(f"[{SESSION_ID}] Результат регистрации: {result}")
        return result
    except Exception as e:
        logger.error(f"[{SESSION_ID}] Ошибка при регистрации пользователя: {e}")
        return False

def classify_user_segment(user_data):
    """Определяет сегмент пользователя на основе его активности"""
    if user_data is None:
        return "new_user"  # Новый пользователь
    
    try:
        # Если есть история использования
        if 'usage_history' in user_data and len(user_data['usage_history']) > 0:
            total_actions = len(user_data['usage_history'])
            
            # Получаем время последнего доступа
            last_access = datetime.strptime(user_data['last_access'], "%Y-%m-%d %H:%M:%S")
            days_since_last_access = (datetime.now() - last_access).days
            
            # Время регистрации
            reg_time = datetime.strptime(user_data['registration_time'], "%Y-%m-%d %H:%M:%S")
            account_age_days = (datetime.now() - reg_time).days
            
            # Определяем категорию активности
            if account_age_days < 7:
                return "new_user"  # Аккаунту меньше недели
            elif days_since_last_access > 30:
                return "inactive"  # Не заходил больше месяца
            elif days_since_last_access > 14:
                return "dormant"  # Не заходил 2-4 недели
            elif days_since_last_access > 7:
                return "occasional"  # Не заходил 1-2 недели
            
            # Активные пользователи
            actions_per_week = total_actions / max(1, account_age_days / 7)
            if actions_per_week > 20:
                return "super_active"  # Очень активный пользователь
            elif actions_per_week > 10:
                return "very_active"  # Активный пользователь
            elif actions_per_week > 5:
                return "active"  # Умеренно активный
            elif actions_per_week > 2:
                return "regular"  # Регулярный пользователь
            else:
                return "casual"  # Случайный пользователь
        
        return "new_user"  # По умолчанию - новый пользователь
    except Exception as e:
        logger.error(f"[{SESSION_ID}] Ошибка при определении сегмента пользователя: {e}")
        return "undefined"  # Не удалось определить

def update_user_segments():
    """Обновляет сегменты всех пользователей"""
    try:
        users = get_users_data()
        if users is None:
            logger.error(f"[{SESSION_ID}] Не удалось получить данные пользователей для обновления сегментов")
            return False

        updated = False
        for user in users:
            if isinstance(user, dict) and 'user_id' in user:
                new_segment = classify_user_segment(user)
                if user.get('user_segment') != new_segment:
                    user['user_segment'] = new_segment
                    updated = True

        if updated:
            result = update_users_data(users)
            logger.debug(f"[{SESSION_ID}] Обновление сегментов пользователей: {result}")
            return result
        return True  # Ничего не изменилось
    except Exception as e:
        logger.error(f"[{SESSION_ID}] Ошибка при обновлении сегментов пользователей: {e}")
        return False

def get_user_stats(user_id):
    """Получает расширенную статистику для конкретного пользователя"""
    try:
        users = get_users_data()
        if users is None:
            logger.error(f"[{SESSION_ID}] Не удалось получить данные пользователей для статистики")
            return None

        for user in users:
            if isinstance(user, dict) and user.get('user_id') == user_id:
                # Обновляем сегмент пользователя перед возвратом данных
                user['user_segment'] = classify_user_segment(user)
                
                # Анализируем активность пользователя
                try:
                    if 'usage_history' in user and len(user['usage_history']) > 0:
                        # Рассчитываем дополнительные метрики
                        usage_history = user['usage_history']
                        
                        # Получаем времена активности
                        activity_times = []
                        for record in usage_history:
                            if isinstance(record, dict) and 'time_of_day' in record:
                                activity_times.append(record['time_of_day'])
                            elif isinstance(record, str):  # Старый формат
                                try:
                                    dt = datetime.strptime(record, "%Y-%m-%d %H:%M:%S")
                                    activity_times.append(dt.hour)
                                except:
                                    pass
                        
                        if activity_times:
                            # Находим пиковые часы активности
                            peak_hours = {}
                            for hour in activity_times:
                                if hour in peak_hours:
                                    peak_hours[hour] += 1
                                else:
                                    peak_hours[hour] = 1
                            
                            # Сортируем по убыванию активности
                            sorted_hours = sorted(peak_hours.items(), key=lambda x: x[1], reverse=True)
                            
                            user['activity_analysis'] = {
                                "peak_hours": [h[0] for h in sorted_hours[:3]],  # Топ-3 часа активности
                                "activity_pattern": "morning" if sum(1 for h in activity_times if 5 <= h < 12) > len(activity_times) / 2 else
                                                   "afternoon" if sum(1 for h in activity_times if 12 <= h < 18) > len(activity_times) / 2 else
                                                   "evening" if sum(1 for h in activity_times if 18 <= h < 23) > len(activity_times) / 2 else
                                                   "night",
                                "average_sessions_per_day": len(set([datetime.strptime(record['timestamp'], "%Y-%m-%d %H:%M:%S").date() 
                                                               for record in usage_history if isinstance(record, dict) and 'timestamp' in record])) / 
                                                             max(1, (datetime.now() - datetime.strptime(user['registration_time'], "%Y-%m-%d %H:%M:%S")).days)
                            }
                except Exception as analysis_error:
                    logger.error(f"[{SESSION_ID}] Ошибка при анализе активности пользователя: {analysis_error}")
                    user['activity_analysis'] = {"error": "Не удалось проанализировать активность"}
                
                return user
        
        return None
    except Exception as e:
        logger.error(f"[{SESSION_ID}] Ошибка при получении статистики пользователя: {e}")
        return None

def get_global_stats():
    """Получает расширенную общую статистику всех пользователей с кэшированием"""
    global stats_cache, last_stats_update
    
    current_time = time.time()
    
    # Используем кэш статистики, если он актуален
    if stats_cache is not None and (current_time - last_stats_update) < STATS_TTL:
        return stats_cache
    
    try:
        users = get_users_data()
        if users is None:
            logger.error(f"[{SESSION_ID}] Не удалось получить данные пользователей для общей статистики")
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
        
        # Счетчики для сегментов
        segments = {
            "new_user": 0,
            "casual": 0,
            "regular": 0,
            "active": 0,
            "very_active": 0,
            "super_active": 0,
            "occasional": 0,
            "dormant": 0,
            "inactive": 0,
            "undefined": 0
        }
        
        # Счетчики для регистраций
        registrations_24h = 0
        registrations_3d = 0
        registrations_week = 0
        registrations_month = 0
        
        # Счетчики для активных пользователей
        active_users_24h = set()
        active_users_3d = set()
        active_users_week = set()
        active_users_month = set()
        
        # Дополнительные метрики
        actions_by_hour = {h: 0 for h in range(24)}  # Действия по часам суток
        actions_by_day = {d: 0 for d in range(7)}    # Действия по дням недели
        registrations_by_month = {m: 0 for m in range(1, 13)}  # Регистрации по месяцам
        registrations_by_day = {d: 0 for d in range(7)}  # Регистрации по дням недели
        
        # Для расчета среднего времени сессии
        session_durations = []
        
        # Анализ данных
        for user in users:
            if not isinstance(user, dict):
                continue
            
            # Подсчет по языкам
            if user.get('language') == 'RU':
                ru_users += 1
            elif user.get('language') == 'EN':
                en_users += 1
            
            # Сегментация пользователей
            segment = user.get('user_segment', 'undefined')
            if segment in segments:
                segments[segment] += 1
            else:
                segments['undefined'] += 1
            
            # Анализ времени регистрации
            try:
                if 'registration_time' in user:
                    reg_time = datetime.strptime(user['registration_time'], "%Y-%m-%d %H:%M:%S")
                    
                    # Регистрации за периоды
                    if reg_time >= day_ago:
                        registrations_24h += 1
                    if reg_time >= three_days_ago:
                        registrations_3d += 1
                    if reg_time >= week_ago:
                        registrations_week += 1
                    if reg_time >= month_ago:
                        registrations_month += 1
                    
                    # Регистрации по месяцам
                    registrations_by_month[reg_time.month] += 1
                    
                    # Регистрации по дням недели
                    registrations_by_day[reg_time.weekday()] += 1
            except Exception as reg_error:
                logger.warning(f"[{SESSION_ID}] Ошибка при анализе времени регистрации: {reg_error}")
            
            # Анализ активности
            try:
                usage_history = user.get('usage_history', [])
                for activity in usage_history:
                    user_id = user.get('user_id')
                    
                    if isinstance(activity, dict) and 'timestamp' in activity:
                        try:
                            activity_time = datetime.strptime(activity['timestamp'], "%Y-%m-%d %H:%M:%S")
                            
                            # Активность за периоды
                            if activity_time >= day_ago and user_id:
                                active_users_24h.add(user_id)
                            if activity_time >= three_days_ago and user_id:
                                active_users_3d.add(user_id)
                            if activity_time >= week_ago and user_id:
                                active_users_week.add(user_id)
                            if activity_time >= month_ago and user_id:
                                active_users_month.add(user_id)
                            
                            # Активность по часам
                            if 'time_of_day' in activity:
                                hour = activity['time_of_day']
                                if 0 <= hour < 24:
                                    actions_by_hour[hour] += 1
                            
                            # Активность по дням недели
                            if 'day_of_week' in activity:
                                day = activity['day_of_week']
                                if 0 <= day < 7:
                                    actions_by_day[day] += 1
                        except ValueError:
                            pass
                    elif isinstance(activity, str):
                        # Обработка старого формата записей
                        try:
                            activity_time = datetime.strptime(activity, "%Y-%m-%d %H:%M:%S")
                            
                            # Активность за периоды (только время)
                            if activity_time >= day_ago and user_id:
                                active_users_24h.add(user_id)
                            if activity_time >= three_days_ago and user_id:
                                active_users_3d.add(user_id)
                            if activity_time >= week_ago and user_id:
                                active_users_week.add(user_id)
                            if activity_time >= month_ago and user_id:
                                active_users_month.add(user_id)
                            
                            # По умолчанию для старого формата
                            actions_by_hour[activity_time.hour] += 1
                            actions_by_day[activity_time.weekday()] += 1
                        except ValueError:
                            pass
            except Exception as usage_error:
                logger.warning(f"[{SESSION_ID}] Ошибка при анализе активности: {usage_error}")
        
        # Рассчитываем дополнительные показатели
        total_actions = sum(actions_by_hour.values())
        peak_hour = max(actions_by_hour.items(), key=lambda x: x[1])[0] if actions_by_hour else None
        peak_day = max(actions_by_day.items(), key=lambda x: x[1])[0] if actions_by_day else None
        
        # Расчет роста аудитории
        prev_month_registrations = sum(1 for user in users if isinstance(user, dict) and 
                              'registration_time' in user and 
                              month_ago - timedelta(days=30) <= datetime.strptime(user['registration_time'], "%Y-%m-%d %H:%M:%S") < month_ago)
        
        growth_rate = ((registrations_month / max(1, prev_month_registrations)) - 1) * 100 if prev_month_registrations > 0 else None
        
        # Статистические показатели
        actions_per_user = total_actions / max(1, total_users)
        
        # Собираем статистику по дням недели для более удобного отображения
        day_names = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]
        activity_by_day = {day_names[day]: count for day, count in actions_by_day.items()}
        registrations_by_day_names = {day_names[day]: count for day, count in registrations_by_day.items()}
        
        # Группировка часов по периодам дня
        time_periods = {
            "Утро (5-11)": sum(actions_by_hour[h] for h in range(5, 12)),
            "День (12-17)": sum(actions_by_hour[h] for h in range(12, 18)),
            "Вечер (18-22)": sum(actions_by_hour[h] for h in range(18, 23)),
            "Ночь (23-4)": sum(actions_by_hour[h] for h in range(23, 24)) + sum(actions_by_hour[h] for h in range(0, 5))
        }
        
        # Формируем общую статистику
        stats = {
            "total_users": total_users,
            "ru_users": ru_users,
            "en_users": en_users,
            "user_segments": segments,
            "registrations": {
                "last_24h": registrations_24h,
                "last_3d": registrations_3d,
                "last_week": registrations_week,
                "last_month": registrations_month,
                "by_month": registrations_by_month,
                "by_day": registrations_by_day_names,
                "growth_rate": growth_rate
            },
            "activity": {
                "last_24h": len(active_users_24h),
                "last_3d": len(active_users_3d),
                "last_week": len(active_users_week),
                "last_month": len(active_users_month),
                "by_hour": actions_by_hour,
                "by_day": activity_by_day,
                "by_time_period": time_periods,
                "peak_hour": peak_hour,
                "peak_day": peak_day,
                "actions_per_user": actions_per_user,
                "total_actions": total_actions
            },
            "retention": {
                "1d": len(active_users_24h) / max(1, registrations_24h) * 100 if registrations_24h > 0 else None,
                "3d": len(active_users_3d) / max(1, registrations_3d) * 100 if registrations_3d > 0 else None,
                "7d": len(active_users_week) / max(1, registrations_week) * 100 if registrations_week > 0 else None,
                "30d": len(active_users_month) / max(1, registrations_month) * 100 if registrations_month > 0 else None
            },
            "meta": {
                "generated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
                "session_id": SESSION_ID,
                "cache_ttl": STATS_TTL
            }
        }
        
        # Обновляем кэш статистики
        stats_cache = stats
        last_stats_update = current_time
        
        return stats
    except Exception as e:
        logger.error(f"[{SESSION_ID}] Ошибка при получении общей статистики: {e}")
        return None

@bot.message_handler(commands=['start'])
def start_command(message):
    try:
        user_id = message.from_user.id
        chat_id = message.chat.id
        logger.info(f"[{SESSION_ID}] Команда /start от пользователя {user_id}")

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
        logger.debug(f"[{SESSION_ID}] Результат проверки авторизации: {authorized}")

        if authorized:
            # Записываем действие
            record_user_activity(user_id, "start_command")
            bot.edit_message_text("✅ Вы авторизованы!", chat_id, msg.message_id)
        else:
            markup = types.InlineKeyboardMarkup(row_width=2)
            ru_button = types.InlineKeyboardButton("RU 🇷🇺", callback_data='lang_ru')
            en_button = types.InlineKeyboardButton("EN 🇬🇧", callback_data='lang_en')
            markup.add(ru_button, en_button)
            bot.edit_message_text("Выберите язык / Choose language:", chat_id, msg.message_id, reply_markup=markup)
    except Exception as e:
        logger.error(f"[{SESSION_ID}] Ошибка при обработке команды /start: {e}")
        try:
            bot.send_message(chat_id, "Произошла ошибка. Попробуйте позже.")
        except:
            pass

@bot.message_handler(commands=['stats'])
def stats_command(message):
    try:
        user_id = message.from_user.id
        chat_id = message.chat.id
        logger.info(f"[{SESSION_ID}] Команда /stats от пользователя {user_id}")

        # Проверка авторизации
        if not is_user_authorized(user_id):
            bot.send_message(chat_id, "Вы не авторизованы. Используйте /start для регистрации.")
            return

        # Записываем действие
        record_user_activity(user_id, "stats_command")

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

        # Определяем язык пользователя для формирования ответа
        language = user_stats.get('language', 'RU')
        
        # Готовим дополнительные данные для персонализированного сообщения
        user_segment = user_stats.get('user_segment', 'undefined')
        
        # Активность пользователя
        activity_analysis = user_stats.get('activity_analysis', {})
        peak_hours = activity_analysis.get('peak_hours', [])
        peak_hours_text = ", ".join([f"{h}:00" for h in peak_hours]) if peak_hours else "данных недостаточно"
        
        # Анализ активного времени суток
        activity_pattern = activity_analysis.get('activity_pattern', 'undefined')
        
        # Сегментация пользователей на русском и английском
        segment_names_ru = {
            "new_user": "Новичок",
            "casual": "Случайный пользователь",
            "regular": "Регулярный пользователь",
            "active": "Активный пользователь",
            "very_active": "Очень активный пользователь",
            "super_active": "Супер активный пользователь",
            "occasional": "Нерегулярный пользователь",
            "dormant": "Временно неактивный",
            "inactive": "Неактивный пользователь",
            "undefined": "Не определен"
        }
        
        segment_names_en = {
            "new_user": "Newcomer",
            "casual": "Casual User",
            "regular": "Regular User",
            "active": "Active User",
            "very_active": "Very Active User",
            "super_active": "Super Active User",
            "occasional": "Occasional User",
            "dormant": "Dormant User",
            "inactive": "Inactive User",
            "undefined": "Undefined"
        }
        
        pattern_names_ru = {
            "morning": "утро",
            "afternoon": "день",
            "evening": "вечер",
            "night": "ночь",
            "undefined": "не определено"
        }
        
        pattern_names_en = {
            "morning": "morning",
            "afternoon": "afternoon",
            "evening": "evening",
            "night": "night",
            "undefined": "undefined"
        }
        
        # Находим самый популярный час и день активности
        peak_activity_hour = global_stats['activity']['peak_hour']
        peak_activity_hour_text_ru = f"{peak_activity_hour}:00" if peak_activity_hour is not None else "данные отсутствуют"
        peak_activity_hour_text_en = f"{peak_activity_hour}:00" if peak_activity_hour is not None else "data not available"
        
        peak_activity_day = global_stats['activity']['peak_day']
        day_names_ru = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]
        day_names_en = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        peak_activity_day_text_ru = day_names_ru[peak_activity_day] if peak_activity_day is not None else "данные отсутствуют"
        peak_activity_day_text_en = day_names_en[peak_activity_day] if peak_activity_day is not None else "data not available"
        
        # Вычисляем, сколько дней пользователь с нами
        reg_time = datetime.strptime(user_stats.get('registration_time'), "%Y-%m-%d %H:%M:%S")
        days_with_us = (datetime.now() - reg_time).days
        
        # Формируем сообщение в зависимости от языка пользователя
        if language == 'RU':
            message_text = (
                f"📊 *Персональная статистика*\n"
                f"🆔 ID: `{user_id}`\n"
                f"📅 Дата регистрации: {user_stats.get('registration_time').split()[0]}\n"
                f"⏱ Последний вход: {user_stats.get('last_access').split()[0]} {user_stats.get('last_access').split()[1]}\n"
                f"🏆 Ваш сегмент: {segment_names_ru.get(user_segment, 'Не определен')}\n"
                f"⌛ С нами: {days_with_us} дней\n"
                f"🕰 Ваше активное время: {pattern_names_ru.get(activity_pattern, 'не определено')}\n"
                f"⏰ Пиковые часы: {peak_hours_text}\n\n"
                
                f"📈 *Аналитика пользования*\n"
                f"• Всего пользователей: {global_stats['total_users']}\n"
                f"• Активных за 24 часа: {global_stats['activity']['last_24h']} ({round(global_stats['activity']['last_24h']/max(1, global_stats['total_users'])*100, 1)}%)\n"
                f"• Новых за неделю: {global_stats['registrations']['last_week']}\n"
                f"• Рост за месяц: {round(global_stats['registrations']['growth_rate'], 1)}% (если отрицательно, то спад)\n\n"
                
                f"🔍 *Инсайты сообщества*\n"
                f"• Самый активный час: {peak_activity_hour_text_ru}\n"
                f"• Самый активный день: {peak_activity_day_text_ru}\n"
                f"• Действий на пользователя: {round(global_stats['activity']['actions_per_user'], 1)}\n"
                f"• Удержание за 7 дней: {round(global_stats['retention']['7d'], 1)}%\n\n"
                
                f"👥 *Распределение пользователей*\n"
                f"• 🇷🇺 Русскоязычных: {global_stats['ru_users']} ({round(global_stats['ru_users']/max(1, global_stats['total_users'])*100, 1)}%)\n"
                f"• 🇬🇧 Англоязычных: {global_stats['en_users']} ({round(global_stats['en_users']/max(1, global_stats['total_users'])*100, 1)}%)\n"
                f"• 🔝 Активных: {global_stats['user_segments']['super_active'] + global_stats['user_segments']['very_active'] + global_stats['user_segments']['active']} ({round((global_stats['user_segments']['super_active'] + global_stats['user_segments']['very_active'] + global_stats['user_segments']['active'])/max(1, global_stats['total_users'])*100, 1)}%)\n"
                f"• 💤 Неактивных: {global_stats['user_segments']['inactive']} ({round(global_stats['user_segments']['inactive']/max(1, global_stats['total_users'])*100, 1)}%)"
            )
        else:  # EN
            message_text = (
                f"📊 *Personal Statistics*\n"
                f"🆔 ID: `{user_id}`\n"
                f"📅 Registration date: {user_stats.get('registration_time').split()[0]}\n"
                f"⏱ Last access: {user_stats.get('last_access').split()[0]} {user_stats.get('last_access').split()[1]}\n"
                f"🏆 Your segment: {segment_names_en.get(user_segment, 'Undefined')}\n"
                f"⌛ With us: {days_with_us} days\n"
                f"🕰 Your active time: {pattern_names_en.get(activity_pattern, 'undefined')}\n"
                f"⏰ Peak hours: {peak_hours_text}\n\n"
                
                f"📈 *Usage Analytics*\n"
                f"• Total users: {global_stats['total_users']}\n"
                f"• Active in last 24h: {global_stats['activity']['last_24h']} ({round(global_stats['activity']['last_24h']/max(1, global_stats['total_users'])*100, 1)}%)\n"
                f"• New in last week: {global_stats['registrations']['last_week']}\n"
                f"• Monthly growth: {round(global_stats['registrations']['growth_rate'], 1)}% (negative means decline)\n\n"
                
                f"🔍 *Community Insights*\n"
                f"• Most active hour: {peak_activity_hour_text_en}\n"
                f"• Most active day: {peak_activity_day_text_en}\n"
                f"• Actions per user: {round(global_stats['activity']['actions_per_user'], 1)}\n"
                f"• 7-day retention: {round(global_stats['retention']['7d'], 1)}%\n\n"
                
                f"👥 *User Distribution*\n"
                f"• 🇷🇺 Russian speakers: {global_stats['ru_users']} ({round(global_stats['ru_users']/max(1, global_stats['total_users'])*100, 1)}%)\n"
                f"• 🇬🇧 English speakers: {global_stats['en_users']} ({round(global_stats['en_users']/max(1, global_stats['total_users'])*100, 1)}%)\n"
                f"• 🔝 Active users: {global_stats['user_segments']['super_active'] + global_stats['user_segments']['very_active'] + global_stats['user_segments']['active']} ({round((global_stats['user_segments']['super_active'] + global_stats['user_segments']['very_active'] + global_stats['user_segments']['active'])/max(1, global_stats['total_users'])*100, 1)}%)\n"
                f"• 💤 Inactive users: {global_stats['user_segments']['inactive']} ({round(global_stats['user_segments']['inactive']/max(1, global_stats['total_users'])*100, 1)}%)"
            )

        bot.send_message(chat_id, message_text, parse_mode="Markdown")
    except Exception as e:
        logger.error(f"[{SESSION_ID}] Ошибка при обработке команды /stats: {e}")
        try:
            bot.send_message(chat_id, "Произошла ошибка при получении статистики. Попробуйте позже.")
        except:
            pass

@bot.callback_query_handler(func=lambda call: call.data.startswith('lang_'))
def language_callback(call):
    try:
        user_id = call.from_user.id
        language = call.data.split('_')[1].upper()
        logger.info(f"[{SESSION_ID}] Выбор языка от пользователя {user_id}: {language}")

        # Финальная проверка перед регистрацией
        if is_user_authorized(user_id):
            response = "🔐 Вы уже в системе!" if language == 'RU' else "🔐 Already registered!"
            bot.edit_message_text(response, call.message.chat.id, call.message.message_id)
            record_user_activity(user_id, "language_reselect")
            return

        # Регистрация пользователя в JSONBin
        try:
            if register_user(user_id, language):
                response = "📬 Запрос отправлен!" if language == 'RU' else "📬 Request submitted!"
            else:
                response = "⚠️ Ошибка регистрации" if language == 'RU' else "⚠️ Registration error"
        except Exception as e:
            logger.error(f"[{SESSION_ID}] Ошибка регистрации: {e}")
            response = "⚠️ Ошибка связи" if language == 'RU' else "⚠️ Connection error"

        bot.edit_message_text(response, call.message.chat.id, call.message.message_id)
    except Exception as e:
        logger.error(f"[{SESSION_ID}] Ошибка при обработке выбора языка: {e}")
        try:
            bot.answer_callback_query(call.id, "Произошла ошибка. Попробуйте позже.")
        except:
            pass

def maintenance_tasks():
    """Выполняет регулярные задачи обслуживания"""
    try:
        logger.info(f"[{SESSION_ID}] Запуск задач обслуживания...")
        
        # Обновляем сегменты пользователей
        update_user_segments()
        
        # Очищаем кэши для обновления данных
        global stats_cache, last_stats_update
        stats_cache = None
        last_stats_update = 0
        
        logger.info(f"[{SESSION_ID}] Задачи обслуживания выполнены")
    except Exception as e:
        logger.error(f"[{SESSION_ID}] Ошибка при выполнении задач обслуживания: {e}")

def main():
    try:
        # Проверяем доступность JSONBin и инициализируем структуру при необходимости
        logger.info(f"[{SESSION_ID}] Инициализация JSONBin структуры...")
        success = initialize_jsonbin()
        if not success:
            logger.error(f"[{SESSION_ID}] Не удалось инициализировать JSONBin структуру!")

        # При запуске бота, сразу загружаем данные пользователей в кэш
        logger.info(f"[{SESSION_ID}] Загрузка данных пользователей в кэш...")
        users = get_users_data(force_update=True)
        logger.info(f"[{SESSION_ID}] Загружено {len(users) if users else 0} записей")

        # Выполняем задачи обслуживания при запуске
        maintenance_tasks()

        logger.info(f"[{SESSION_ID}] Бот запущен. Кэш пользователей инициализирован.")

        # Добавляем тестовый запрос к API Telegram для проверки токена
        test_response = requests.get(f"https://api.telegram.org/bot{BOT_TOKEN}/getMe")
        if test_response.status_code == 200:
            bot_info = test_response.json()
            logger.info(f"[{SESSION_ID}] Бот подключен: @{bot_info['result']['username']}")
        else:
            logger.error(f"[{SESSION_ID}] Ошибка подключения к Telegram API: {test_response.status_code}")

        # Запускаем бота
        bot.infinity_polling()
    except Exception as e:
        logger.critical(f"[{SESSION_ID}] Критическая ошибка при запуске бота: {e}")

if __name__ == "__main__":
    main()
