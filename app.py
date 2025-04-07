import telebot
import os
import json
import logging
import requests
from telebot import types

# Configuration
BOT_TOKEN = os.environ.get('7671924788:AAHCVF8B-PiyNC84gbNdn7i54Ai5eWTLm0s')
CHANNEL_ID = os.environ.get('-1001948875251')  # Private channel ID where user data is stored
ADMIN_ID = os.environ.get('1420106372')      # Admin's telegram ID

# Setup logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize bot
bot = telebot.TeleBot(BOT_TOKEN)

# Function to get user's IP and geolocation
def get_user_location():
    try:
        ip_info = requests.get('https://ipinfo.io/json').json()
        return f"{ip_info.get('country', 'Unknown')}, {ip_info.get('city', 'Unknown')}, IP: {ip_info.get('ip', 'Unknown')}"
    except Exception as e:
        logger.error(f"Error getting location: {e}")
        return "Location unavailable"

# Function to check if user is authorized by looking in channel history
def is_user_authorized(user_id):
    try:
        # Get recent messages from the channel
        messages = bot.get_chat_history(CHANNEL_ID, limit=100)
        
        # Look for user ID in messages
        for message in messages:
            if hasattr(message, 'text') and message.text and str(user_id) in message.text:
                logger.info(f"User {user_id} found in authorized list")
                return True
        
        logger.info(f"User {user_id} not found in authorized list")
        return False
    except Exception as e:
        logger.error(f"Error checking authorization: {e}")
        # If we can't check, assume not authorized
        return False

# Start command handler
@bot.message_handler(commands=['start'])
def start_command(message):
    user_id = message.from_user.id
    
    # Check if user is authorized - this happens silently
    authorized = is_user_authorized(user_id)
    
    if authorized:
        # User is authorized, proceed normally
        bot.send_message(message.chat.id, "Welcome back! How can I help you today?")
    else:
        # Create language selection keyboard
        markup = types.InlineKeyboardMarkup(row_width=2)
        ru_button = types.InlineKeyboardButton("RU 🇷🇺", callback_data="lang_ru")
        en_button = types.InlineKeyboardButton("EN 🇬🇧", callback_data="lang_en")
        markup.add(ru_button, en_button)
        
        # Collect basic user info
        user_info = {
            'user_id': user_id,
            'username': message.from_user.username,
            'first_name': message.from_user.first_name,
            'last_name': message.from_user.last_name,
            'location': get_user_location()
        }
        
        # Store user info temporarily (this would be better with a database, but we're using globals as specified)
        global user_data
        if not 'user_data' in globals():
            user_data = {}
        user_data[user_id] = user_info
        
        # Ask user to select language
        bot.send_message(message.chat.id, "Please select your language / Пожалуйста, выберите язык:", reply_markup=markup)

# Callback handler for language selection
@bot.callback_query_handler(func=lambda call: call.data.startswith('lang_'))
def language_callback(call):
    user_id = call.from_user.id
    selected_language = call.data.split('_')[1]  # 'ru' or 'en'
    
    # Try to get user data from our temporary storage
    if not 'user_data' in globals():
        user_data = {}
    
    user_info = user_data.get(user_id, {
        'user_id': user_id,
        'username': call.from_user.username,
        'first_name': call.from_user.first_name,
        'last_name': call.from_user.last_name,
        'location': get_user_location()
    })
    
    # Add language selection to user info
    user_info['language'] = selected_language
    
    # Format message for channel
    info_text = (f"New User:\n"
                f"ID: {user_id}\n"
                f"Username: @{user_info.get('username', 'None')}\n"
                f"Name: {user_info.get('first_name', '')} {user_info.get('last_name', '')}\n"
                f"Location: {user_info.get('location', 'Unknown')}\n"
                f"Language: {selected_language.upper()}")
    
    # Send to channel
    bot.send_message(CHANNEL_ID, info_text)
    
    # Clean up temporary data
    if user_id in user_data:
        del user_data[user_id]
    
    # Send welcome message based on selected language
    if selected_language == 'ru':
        welcome_text = "Добро пожаловать! Чем я могу помочь?"
    else:  # 'en'
        welcome_text = "Welcome! How can I help you?"
    
    # Answer the callback to remove "loading" state from button
    bot.answer_callback_query(call.id)
    
    # Edit the original message to remove the inline keyboard
    bot.edit_message_text(
        chat_id=call.message.chat.id,
        message_id=call.message.message_id,
        text=welcome_text,
        reply_markup=None
    )

# Admin command to check bot status
@bot.message_handler(commands=['admin'])
def admin_command(message):
    if str(message.from_user.id) == ADMIN_ID:
        bot.send_message(message.chat.id, "Bot is running normally.")
    else:
        bot.send_message(message.chat.id, "You don't have permission to use this command.")

# Main function to start the bot
def main():
    logger.info("Starting bot...")
    bot.infinity_polling()

if __name__ == '__main__':
    main()