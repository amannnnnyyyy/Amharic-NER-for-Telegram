import nest_asyncio
import os
import csv
import re
import asyncio
from telethon import TelegramClient
from dotenv import load_dotenv

# Apply nest_asyncio to allow nested event loops
nest_asyncio.apply()

# Load environment variables from .env file
load_dotenv('.env')
api_id = os.getenv('TG_API_ID')
api_hash = os.getenv('TG_API_HASH')

# Function to scrape messages from Telegram channels and save to CSV
async def scrape_telegram_channels(client, channel, csv_file='telegram_data.csv'):
    """
    Scrapes messages and entities from a list of Telegram channels and saves the data to a CSV file.
    Args:
    client (TelegramClient): The initialized Telegram client.
    channel : A Telegram channel username to scrape.
    csv_file (str): Path to the CSV file to save the scraped data. Default is 'telegram_data.csv'.
    """
    await client.start()
    with open(csv_file, 'w', newline='', encoding='utf-8') as file:
        limit = 300
        writer = csv.writer(file)
        writer.writerow(['Message Date', 'Sender ID', 'Message ID', 'Product Description'])  # CSV header

        for channel_username in channel:
            entity = await client.get_entity(channel_username)
            channel_title = entity.title 
            print(f"Scraping data from {channel_username} ({channel_title})...")

            async for message in client.iter_messages(entity, limit=limit):
                amharic_reg = r'[\u1200-\u137F0-9\+\-_]+'
                amharic_text = ' '.join(re.findall(amharic_reg, message.message))
                print(amharic_text)
                # Only write the row if Amharic content is found
                if amharic_text.strip():
                    message_date = message.date.strftime('%Y-%m-%d %H:%M:%S') if message.date else '[No Date]'
                    sender_id = message.sender_id if message.sender_id else '[No Sender ID]'
                    writer.writerow([
                        message_date, 
                        sender_id,
                        message.id,
                        amharic_text.strip()
                    ])

            print(f"Finished scraping {channel_username}")

# Wrapper function to start scraping using asyncio
def start_scraping(channel, csv_file='telegram_data.csv'):
    """
    Wrapper function to initialize the Telegram client and run the scraping process.
    Args:
    channel : A Telegram channel username to scrape.
    csv_file (str): Path to the CSV file to save the scraped data. Default is 'telegram_data.csv'.
    """
    client = TelegramClient('scraping_session', api_id, api_hash)
    # Use asyncio.run() to run the asynchronous scraping function
    asyncio.run(scrape_telegram_channels(client, channel, csv_file))

