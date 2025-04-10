# statistics.py
import json
import logging
import requests
from datetime import datetime, timedelta
from io import BytesIO

logger = logging.getLogger(__name__)

# Конфигурация JSONBin.io
JSONBIN_API_KEY = "$2a$10$hT79uCEaJENfQBZ7576aL.upUOtnPqJZX53sWcln0HZib/bgs.8.u"
JSONBIN_BIN_ID = "67f532028a456b796684e974"
JSONBIN_URL = f"https://api.jsonbin.io/v3/b/{JSONBIN_BIN_ID}"

# Локализация
LANGUAGES = {
    'RU': {
        'stats_title': "📊 Полная статистика",
        'user_stats': (
            "👤 Ваша статистика:\n"
            "🆔 ID: {user_id}\n"
            "🌐 Язык: {language}\n"
            "📅 Зарегистрирован: {registration_date}\n"
            "⏳ Дней в боте: {days_in_bot}\n"
            "⏱ Последняя активность: {last_access}"
        ),
        'global_stats': (
            "🌍 Глобальная статистика:\n"
            "👥 Всего пользователей: {total_users}\n"
            "🇷🇺 Русскоязычных: {ru_users}\n"
            "🇬🇧 Англоязычных: {en_users}"
        ),
        'activity_stats': (
            "📈 Статистика активности:\n\n"
            "🔥 Активные пользователи:\n"
            "• Сегодня: {active_today}\n"
            "• Неделя: {active_week}\n"
            "• Месяц: {active_month}\n\n"
            "🆕 Новые регистрации:\n"
            "• Сегодня: {joined_today}\n"
            "• Неделя: {joined_week}\n"
            "• Месяц: {joined_month}\n\n"
            "🌎 По странам (последние 30 дней):\n"
            "🇷🇺 Россия: {ru_recent}\n"
            "🇬🇧 Англия: {en_recent}"
        ),
        'back_btn': "🔙 Назад",
        'monthly_report_caption': "📅 Подробная статистика за последний месяц"
    },
    'EN': {
        'stats_title': "📊 Complete Statistics",
        'user_stats': (
            "👤 Your Statistics:\n"
            "🆔 ID: {user_id}\n"
            "🌐 Language: {language}\n"
            "📅 Registered: {registration_date}\n"
            "⏳ Days in bot: {days_in_bot}\n"
            "⏱ Last activity: {last_access}"
        ),
        'global_stats': (
            "🌍 Global Statistics:\n"
            "👥 Total users: {total_users}\n"
            "🇷🇺 Russian: {ru_users}\n"
            "🇬🇧 English: {en_users}"
        ),
        'activity_stats': (
            "📈 Activity Statistics:\n\n"
            "🔥 Active users:\n"
            "• Today: {active_today}\n"
            "• Week: {active_week}\n"
            "• Month: {active_month}\n\n"
            "🆕 New registrations:\n"
            "• Today: {joined_today}\n"
            "• Week: {joined_week}\n"
            "• Month: {joined_month}\n\n"
            "🌎 By country (last 30 days):\n"
            "🇷🇺 Russia: {ru_recent}\n"
            "🇬🇧 England: {en_recent}"
        ),
        'back_btn': "🔙 Back",
        'monthly_report_caption': "📅 Detailed statistics for last month"
    }
}

def initialize_jsonbin():
    """Инициализирует структуру данных в JSONBin"""
    try:
        response = requests.get(JSONBIN_URL, headers=get_headers())
        if response.status_code == 200:
            return True
        
        initial_data = {"users": []}
        response = requests.put(JSONBIN_URL, json=initial_data, headers=get_headers(True))
        return response.status_code == 200
    except Exception as e:
        logger.error(f"JSONBin initialization error: {e}")
        return False

def get_headers(include_content_type=False):
    headers = {"X-Master-Key": JSONBIN_API_KEY}
    if include_content_type:
        headers["Content-Type"] = "application/json"
    return headers

def get_users_data(force_update=False):
    """Получает данные пользователей с кэшированием"""
    try:
        response = requests.get(JSONBIN_URL, headers=get_headers())
        if response.status_code == 200:
            data = response.json().get('record', {}).get('users', [])
            return process_users_data(data)
        return []
    except Exception as e:
        logger.error(f"Error fetching users data: {e}")
        return []

def process_users_data(users):
    """Обработка и валидация данных пользователей"""
    valid_users = []
    for user in users:
        if isinstance(user, dict) and 'user_id' in user:
            user['days_in_bot'] = calculate_days(user.get('registration_time'))
            valid_users.append(user)
    return valid_users

def calculate_days(registration_time):
    """Вычисляет количество дней с регистрации"""
    try:
        reg_date = datetime.strptime(registration_time, "%Y-%m-%d %H:%M:%S")
        return (datetime.now() - reg_date).days
    except:
        return 0

def update_users_data(users):
    """Обновляет данные в JSONBin"""
    try:
        response = requests.put(
            JSONBIN_URL,
            json={"users": users},
            headers=get_headers(True)
        )
        return response.status_code == 200
    except Exception as e:
        logger.error(f"Error updating users data: {e}")
        return False

def get_user_stats(user_id):
    """Возвращает статистику конкретного пользователя"""
    users = get_users_data()
    for user in users:
        if user.get('user_id') == user_id:
            user['days_in_bot'] = calculate_days(user.get('registration_time'))
            return user
    return None

def get_global_stats():
    """Глобальная статистика"""
    users = get_users_data()
    ru_users = sum(1 for u in users if u.get('language') == 'RU')
    en_users = sum(1 for u in users if u.get('language') == 'EN')
    return {
        'total_users': len(users),
        'ru_users': ru_users,
        'en_users': en_users
    }

def get_activity_stats():
    """Статистика активности"""
    users = get_users_data()
    now = datetime.now()
    
    # Активность
    active = {'today': 0, 'week': 0, 'month': 0}
    # Регистрации
    joined = {'today': 0, 'week': 0, 'month': 0}
    # По странам (последние 30 дней)
    country_stats = {'RU': 0, 'EN': 0}

    for user in users:
        last_access = parse_date(user.get('last_access'))
        reg_date = parse_date(user.get('registration_time'))

        # Активность
        if last_access:
            update_period_stats(active, now - last_access)

        # Регистрации
        if reg_date:
            update_period_stats(joined, now - reg_date)
            if (now - reg_date).days <= 30:
                lang = user.get('language', 'RU')
                country_stats[lang] += 1

    return {
        'active': active,
        'joined': joined,
        'country_stats': country_stats
    }

def parse_date(date_str):
    """Парсит дату из строки"""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
    except:
        return None

def update_period_stats(stats, time_delta):
    """Обновляет счетчики периодов"""
    days = time_delta.days
    if days < 1:
        stats['today'] += 1
    if days < 7:
        stats['week'] += 1
    if days < 30:
        stats['month'] += 1

def generate_monthly_stats_file(lang='RU'):
    """Генерирует файл с месячной статистикой"""
    users = get_users_data()
    now = datetime.now()
    stats = {}

    # Инициализация 30 дней
    for i in range(30):
        date = (now - timedelta(days=i)).strftime("%Y-%m-%d")
        stats[date] = {'active': 0, 'registrations': 0, 'RU': 0, 'EN': 0}

    for user in users:
        reg_date = parse_date(user.get('registration_time'))
        last_access = parse_date(user.get('last_access'))
        user_lang = user.get('language', 'RU')

        # Регистрации
        if reg_date:
            reg_key = reg_date.strftime("%Y-%m-%d")
            if reg_key in stats:
                stats[reg_key]['registrations'] += 1
                stats[reg_key][user_lang] += 1

        # Активность
        if last_access:
            access_key = last_access.strftime("%Y-%m-%d")
            if access_key in stats:
                stats[access_key]['active'] += 1

    # Форматирование в текст
    report = []
    for date in sorted(stats.keys(), reverse=True):
        data = stats[date]
        report.append(
            f"[{date}]\n"
            f"Active: {data['active']}\n"
            f"New: {data['registrations']} "
            f"(RU: {data['RU']}, EN: {data['EN']})\n"
            f"{'-'*30}"
        )

    # Создание файла в памяти
    file_data = BytesIO("\n".join(report).encode('utf-8'))
    file_data.name = f"stats_{datetime.now().strftime('%Y-%m')}.txt"
    return file_data