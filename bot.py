import telebot
from telebot import types
import os

# Получаем токен бота и ID канала из переменных окружения
BOT_TOKEN = os.getenv('7671924788:AAHCVF8B-PiyNC84gbNdn7i54Ai5eWTLm0s')
CHANNEL_ID = os.getenv('-1001948875251')

# Инициализируем бота
bot = telebot.TeleBot(BOT_TOKEN)

# Обработчик команды /start
@bot.message_handler(commands=['start'])
def start(message):
    user_id = message.from_user.id
    try:
        # Получаем информацию о канале, включая закрепленное сообщение
        chat = bot.get_chat(CHANNEL_ID)
        # Проверяем, есть ли ID пользователя в тексте закрепленного сообщения
        if chat.pinned_message and str(user_id) in chat.pinned_message.text:
            bot.send_message(message.chat.id, "Добро пожаловать обратно, авторизованный пользователь!")
        else:
            # Создаем инлайн-кнопки для выбора региона
            markup = types.InlineKeyboardMarkup()
            btn_ru = types.InlineKeyboardButton('RU', callback_data='select_RU')
            btn_en = types.InlineKeyboardButton('EN', callback_data='select_EN')
            markup.add(btn_ru, btn_en)
            bot.send_message(message.chat.id, "Пожалуйста, выберите ваш регион:", reply_markup=markup)
    except Exception as e:
        bot.send_message(message.chat.id, "Произошла ошибка. Попробуйте позже.")
        print(f"Ошибка: {e}")

# Обработчик нажатий на инлайн-кнопки
@bot.callback_query_handler(func=lambda call: True)
def callback_query(call):
    user_id = call.from_user.id
    if call.data == 'select_RU':
        selection = 'RU'
    elif call.data == 'select_EN':
        selection = 'EN'
    else:
        return
    
    try:
        # Отправляем сообщение в канал с ID пользователя и его выбором
        bot.send_message(CHANNEL_ID, f"Пользователь {user_id} выбрал {selection}")
        # Подтверждаем обработку callback-запроса
        bot.answer_callback_query(call.id)
        # Уведомляем пользователя
        bot.send_message(call.message.chat.id, "Ваш выбор записан. Ожидайте авторизации.")
    except Exception as e:
        bot.answer_callback_query(call.id, "Произошла ошибка.")
        print(f"Ошибка: {e}")

# Запускаем бота в режиме polling
if __name__ == "__main__":
    bot.polling()