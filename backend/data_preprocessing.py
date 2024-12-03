import pandas as pd
import re
from langdetect import detect, LangDetectException
import contractions
from pymongo import MongoClient, UpdateOne
import logging

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# MongoDB connection URI
uri = "mongodb+srv://mohammedkaleemullah06:aeqaasGuclMt0xtO@cluster0.khzha.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = MongoClient(uri)

# Access the database
db = client['Reddit-Scraping-New']

# List of collections to process
collections_to_process = ['subreddit-data', 'statewise-data']

# Precompile regex patterns
url_pattern = re.compile(r'http\S+|www\S+')
non_alpha_pattern = re.compile(r'[^a-zA-Z\s\.]')
sentence_split_pattern = re.compile(r'(?<=\.)\s*')

# Patterns for specific links
whatsapp_group_pattern = re.compile(r'chat\.whatsapp\.com\S*')
whatsapp_channel_pattern = re.compile(r'whatsapp\.com/channel/\S*')
telegram_pattern = re.compile(r't\.me\S*')

# Function to clean and preprocess text
def clean_and_preprocess_text(selftext, comments):
    try:
        # Merge selftext and comments into a single string
        text = selftext + ' ' + ' '.join(comments) if selftext else ' '.join(comments)

        # Extract all URLs
        extracted_urls = url_pattern.findall(text) or []

        # Extract specific links
        whatsapp_group_links = whatsapp_group_pattern.findall(text) or None
        whatsapp_channel_links = whatsapp_channel_pattern.findall(text) or None
        telegram_links = telegram_pattern.findall(text) or None

        # Remove URLs and special characters
        text = url_pattern.sub('', text)
        text = non_alpha_pattern.sub('', text)

        # Remove newline characters and convert to lowercase
        text = text.replace('\n', ' ').replace('\r', ' ').lower()

        # Remove placeholders
        text = text.replace('[removed]', '').replace('[ Removed by Reddit ]', '').replace('[deleted]', '')

        # Expand contractions
        text = contractions.fix(text)

        # Split text into sentences and filter
        sentences = sentence_split_pattern.split(text)
        filtered_sentences = []
        for sentence in sentences:
            sentence = sentence.strip()
            if len(sentence) > 3:  # Filter out very short sentences
                try:
                    if detect(sentence) == 'en':  # Keep only English sentences
                        filtered_sentences.append(sentence)
                except LangDetectException:
                    logging.warning("Language detection failed for a sentence. Skipping.")

        # Join sentences back
        processed_text = '. '.join(filtered_sentences).replace('..', '.')
        return processed_text, extracted_urls, whatsapp_group_links, whatsapp_channel_links, telegram_links
    except Exception as e:
        logging.error(f"Error processing text: {e}")
        return None, None, None, None, None

def run_data_preprocessing():
    try:
        # Process each collection
        for collection_name in collections_to_process:
            logging.info(f"Processing collection: {collection_name}")
            try:
                collection = db[collection_name]
                # Retrieve data
                subreddit_data = pd.DataFrame(list(collection.find()))
                if subreddit_data.empty:
                    logging.warning(f"No data found in collection: {collection_name}")
                    continue

                # Apply preprocessing
                subreddit_data['processed_text'], \
                subreddit_data['Extracted URLs'], \
                subreddit_data['whatsapp_group_links'], \
                subreddit_data['whatsapp_channel_links'], \
                subreddit_data['telegram_links'] = zip(
                    *subreddit_data.apply(
                        lambda row: clean_and_preprocess_text(row.get('selftext', ''), row.get('comments', [])),
                        axis=1
                    )
                )

                # Prepare bulk updates
                bulk_updates = []
                for _, row in subreddit_data.iterrows():
                    if row['processed_text'] is not None:  # Only update if processing was successful
                        bulk_updates.append(UpdateOne(
                            {'_id': row['_id']},
                            {'$set': {
                                'processed_text': row['processed_text'],
                                'Extracted URLs': row['Extracted URLs'],
                                'whatsapp_group_links': row['whatsapp_group_links'],
                                'whatsapp_channel_links': row['whatsapp_channel_links'],
                                'telegram_links': row['telegram_links']
                            }}
                        ))

                # Execute bulk updates
                if bulk_updates:
                    result = collection.bulk_write(bulk_updates)
                    logging.info(f"Bulk write completed. Matched: {result.matched_count}, Modified: {result.modified_count}")
                else:
                    logging.warning(f"No updates to perform for collection: {collection_name}")
            except Exception as e:
                logging.error(f"Error processing collection {collection_name}: {e}")

        logging.info("Data preprocessing completed successfully.")
        return "Data preprocessing completed successfully."
    
    except Exception as e:
        logging.error(f"Error: {str(e)}")
        return f"Error: {str(e)}"
    
    finally:
        # Close the MongoDB client
        client.close()
