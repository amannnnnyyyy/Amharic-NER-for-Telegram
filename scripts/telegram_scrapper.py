import asyncio
from telethon import TelegramClient
import csv
import os
import re
from dotenv import load_dotenv
import nest_asyncio
from datetime import datetime

# Allow nested async calls in Jupyter
nest_asyncio.apply()

# Load environment variables from .env file
load_dotenv('.env')
api_id = os.getenv('TG_API_ID')
api_hash = os.getenv('TG_API_HASH')

# Function to extract entities using regex patterns
def extract_entities(message_text):
    """
    Extracts entities like price, phone numbers, and addresses from the message using regex patterns.
    """
    entities = {}

    # Debug print to show the message being processed
    print(f"Extracting entities from message: {message_text}")
    
    # Regex pattern to extract prices (e.g., '2250 ብር', '2,500.00 ብር', 'ብር 2500', etc.)
    price_pattern = r'(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)\s*ብር|\bብር\s*(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)'
    price_match = re.search(price_pattern, message_text)
    entities['price'] = price_match.group(0) if price_match else '[No Price]'

    # Regex pattern to extract phone numbers (e.g., '0928460606', '2519XXXXXXXX')
    phone_pattern = r'\b(?:09\d{8}|2519\d{8})\b'
    phones = re.findall(phone_pattern, message_text)
    entities['phones'] = ', '.join(phones) if phones else '[No Phone Numbers]'

    # Regex pattern to extract addresses (e.g., addresses starting with '📍')
    address_pattern = r'📍\s*([^\n]+)'
    addresses = re.findall(address_pattern, message_text)
    entities['address'] = '\n'.join(addresses).strip() if addresses else '[No Address]'

    # Debug print to show extracted entities
    print(f"Extracted entities: {entities}")
    
    return entities

# Function to scrape messages from Telegram channels and save to CSV
async def scrape_telegram_channels(client, channels, csv_file='telegram_data.csv', media_dir='photos', limit=100):
    """
    Scrapes messages and entities from a list of Telegram channels and saves the data to a CSV file.

    Args:
    client (TelegramClient): The initialized Telegram client.
    channels (list): List of Telegram channel usernames to scrape.
    csv_file (str): Path to the CSV file to save the scraped data. Default is 'telegram_data.csv'.
    limit (int): The number of messages to scrape per channel. Default is 100.
    """
    await client.start()
    os.makedirs(media_dir, exist_ok=True)
    # Open the CSV file and prepare the writer
    with open(csv_file, 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['Channel Title', 'Channel Username', 'Message ID', 'Message Date','Sender ID','Sender Name', 'Price', 'Phones', 'Address', 'Media Type','Media Path','Product Description'])  # CSV header

        # Iterate over the list of channels
        for channel_username in channels:
            entity = await client.get_entity(channel_username)
            channel_title = entity.title 
            print(f"Scraping data from {channel_username} ({channel_title})...")

            async for message in client.iter_messages(entity, limit=limit):
                # Extract Amharic text (if any)
                amharic_reg = re.compile(r'[\u1200-\u137F"\s]+')
                amharic_text = ' '.join(amharic_reg.findall(message.text or '[No Text]'))

                # Extract other message details
                text_content = message.text or '[No Text]'
                message_date = message.date.strftime('%Y-%m-%d %H:%M:%S') if message.date else '[No Date]'
                sender_id = message.sender_id if message.sender_id else '[No Sender ID]'
                sender_name = (await message.get_sender()).username if message.sender_id else '[No Sender Name]'

                # Extract entities from the message text
                entities = extract_entities(text_content)
                media_file = '[No Media]'
                if message.media and hasattr(message.media, 'photo'):
                     # Create a unique filename for the photo
                     filename = f"{channel_username}_{message.id}.jpg"
                     media_path = os.path.join(media_dir, filename)
                     # Download the media to the specified directory if it's a photo
                     await client.download_media(message.media, media_path)
                     media_file = media_path
                # Write the extracted data to CSV
                writer.writerow([
                    channel_title, 
                    channel_username, 
                    message.id, 
                    message_date, 
                    sender_id,
                    sender_name,
                    entities['price'], 
                    entities['phones'], 
                    entities['address'], 
                    'Photo',
                    media_file,
                    amharic_text.strip()
                ])

            print(f"Finished scraping {channel_username}")

# Wrapper function to start scraping using asyncio
def start_scraping(channels, csv_file='telegram_data.csv', limit=100):
    """
    Wrapper function to initialize the Telegram client and run the scraping process.

    Args:
    channels (list): List of Telegram channel usernames to scrape.
    csv_file (str): Path to the CSV file to save the scraped data. Default is 'telegram_data.csv'.
    limit (int): The number of messages to scrape per channel. Default is 100.
    """
    media_dir = 'photos'
    client = TelegramClient('scraping_session', api_id, api_hash)
    
    # Use asyncio.run() to run the asynchronous scraping function
    asyncio.run(scrape_telegram_channels(client, channels, csv_file,media_dir, limit))
