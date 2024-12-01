import telebot
import requests
import logging
import time
import os
import re

API_KEY = '7246280212:AAGOvDby43WxeGbcO9eLMYZ33UtjMp9TSZo'
GEMINI_API_KEY = 'AIzaSyA8DmFWWdk7ni5gaNHL_3Vkv2nMox-WB6M'
GEMINI_API_URL = 'https://generativelanguage.googleapis.com/v1beta/models/gemini-pro:generateContent'

bot = telebot.TeleBot(API_KEY)
user_count = set()  # Множество для хранения уникальных ID пользователей

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Возможные вариации имени
name_variations = ["камилла", "камил", "камиллы", "камилле", "Camilla", "camilla", "Cam", "cam"]

# ID пользователей и их специальные сообщения
special_users = {
    1420106372: "",
    1653222949: ""
}

def detect_language(text):
    """
    Определяет преобладающий язык в тексте.
    Возвращает 'russian' или 'english'
    """
    russian_chars = len(re.findall(r'[а-яА-Я]', text))
    english_chars = len(re.findall(r'[a-zA-Z]', text))

    return 'russian' if russian_chars >= english_chars else 'english'

def load_prompt(language, is_private=True):
    """
    Загружает промт в зависимости от языка и типа чата
    """
    try:
        if language == 'russian':
            if is_private:
                with open('Russian-promt.py', 'r', encoding='utf-8') as f:
                    return f.read()
            else:
                with open('Russian-promt-group.py', 'r', encoding='utf-8') as f:
                    return f.read()
        else:
            if is_private:
                with open('American-promt.py', 'r', encoding='utf-8') as f:
                    return f.read()
            else:
                with open('American-promt-group.py', 'r', encoding='utf-8') as f:
                    return f.read()
    except FileNotFoundError:
        logging.error(f"Файл промта для языка {language} не найден!")
        return ""

@bot.message_handler(commands=['start'])
def send_welcome(message):
    user_count.add(message.from_user.id)
    bot.reply_to(message, "Hi! My name is Camilla, your assistant по текстур пакам, РП и модификациям для Minecraft. Спрашивай, что угодно!")

@bot.message_handler(commands=['stats'])
def send_stats(message):
    bot.reply_to(message, f"Количество уникальных пользователей: {len(user_count)}")

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    user_count.add(message.from_user.id)
    file_id = message.photo[-1].file_id
    file_info = bot.get_file(file_id)
    downloaded_file = bot.download_file(file_info.file_path)

    # Сохраняем изображение временно
    image_path = f"temp_{file_id}.jpg"
    with open(image_path, 'wb') as new_file:
        new_file.write(downloaded_file)

    # Обрабатываем изображение с помощью Gemini
    response = get_gemini_image_response(image_path)

    # Удаляем временное изображение
    os.remove(image_path)

    bot.reply_to(message, response)

@bot.message_handler(func=lambda message: True)
def handle_message(message):
    user_text = message.text.lower()
    user_id = message.from_user.id
    chat_type = message.chat.type

    user_count.add(user_id)

    bot.send_chat_action(message.chat.id, 'record_video_note')  # Показываем статус "Записывает Кружок"

    # Определяем язык
    detected_language = detect_language(user_text)

    if chat_type == 'private':
        additional_prompt = load_prompt(detected_language, is_private=True)

        if user_id in special_users:
            response = get_gemini_response_special(user_text, special_users[user_id], additional_prompt)
        else:
            response = get_gemini_response(user_text, additional_prompt)
    elif chat_type in ['group', 'supergroup'] and any(name in user_text for name in name_variations):
        additional_prompt = load_prompt(detected_language, is_private=False)
        response = get_gemini_response(user_text, additional_prompt)
    else:
        return  # Игнорируем сообщения, не относящиеся к боту в группах

    bot.reply_to(message, response)

def get_gemini_response(question, additional_text):
    combined_message = f"{question}\n\n{additional_text}"

    payload = {
        "contents": [{
            "parts": [{
                "text": combined_message
            }]
        }]
    }
    headers = {
        'Content-Type': 'application/json',
    }
    try:
        response = requests.post(f'{GEMINI_API_URL}?key={GEMINI_API_KEY}', json=payload, headers=headers)
        response.raise_for_status()
        data = response.json()
        result = data['candidates'][0]['content']['parts'][0]['text']

        # Удаление точки в конце текста
        if result.endswith('.'):
            result = result[:-1]

        return result
    except Exception as e:
        logging.error(f"Ошибка при обращении к Gemini API: {e}")
        return "извините, произошла ошибка при обработке запроса"

def get_gemini_response_special(question, special_message, additional_text):
    combined_message = f"{question}\n\n{special_message}\n\n{additional_text}"

    payload = {
        "contents": [{
            "parts": [{
                "text": combined_message
            }]
        }]
    }
    headers = {
        'Content-Type': 'application/json',
    }
    try:
        response = requests.post(f'{GEMINI_API_URL}?key={GEMINI_API_KEY}', json=payload, headers=headers)
        response.raise_for_status()
        data = response.json()
        result = data['candidates'][0]['content']['parts'][0]['text']

        # Удаление точки в конце текста
        if result.endswith('.'):
            result = result[:-1]

        return result
    except Exception as e:
        logging.error(f"Ошибка при обращении к Gemini API: {e}")
        return "извините, произошла ошибка при обработке запроса"

def get_gemini_image_response(image_path):
    with open(image_path, 'rb') as image_file:
        image_data = image_file.read()

    headers = {
        'Content-Type': 'application/json',
    }
    payload = {
        'requests': [
            {
                'image': {
                    'content': image_data
                },
                'features': [
                    {
                        'type': 'LABEL_DETECTION',
                    }
                ],
            }
        ]
    }
    try:
        response = requests.post(f'{GEMINI_API_URL}?key={GEMINI_API_KEY}', json=payload, headers=headers)
        response.raise_for_status()
        data = response.json()
        labels = data['responses'][0]['labelAnnotations']
        label_descriptions = [label['description'] for label in labels]
        return f"Я вижу следующие объекты на изображении: {', '.join(label_descriptions)}"
    except Exception as e:
        logging.error(f"Ошибка при обработке изображения через Gemini API: {e}")
        return "извините, произошла ошибка при обработке изображения"

if __name__ == "__main__":
    while True:
        try:
            bot.polling(none_stop=True)
        except Exception as e:
            logging.error(f"Ошибка в основном цикле: {e}")
            time.sleep(15)  # Задержка перед повторным запуском
