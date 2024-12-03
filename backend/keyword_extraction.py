from keybert import KeyBERT
import spacy
import pandas as pd
from pymongo import MongoClient
import logging

def run_keyword_extraction():
    try:
        # Initialize logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

        # MongoDB connection URI
        uri = "mongodb+srv://mohammedkaleemullah06:aeqaasGuclMt0xtO@cluster0.khzha.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
        client = MongoClient(uri)

        # Access the database
        db = client['Reddit-Scraping-New']

        # Load the Indian places list from the provided CSV file
        indian_places_file = 'indian_places.csv'
        indian_places_df = pd.read_csv(indian_places_file)
        indian_places_set = set(indian_places_df['Indian_locations'].str.lower())  # Use lowercase for consistency

        # Initialize spaCy and KeyBERT
        nlp = spacy.load('en_core_web_sm')
        kw_model = KeyBERT()

        # Function to filter keywords
        def filter_keywords(keywords, nlp, score_threshold=0.3):
            filtered = set()  # Use a set to ensure uniqueness
            for word, score in keywords:
                word_lower = word.lower()  # Compare in lowercase
                if (
                    score >= score_threshold and  # Filter by score threshold
                    word_lower not in indian_places_set  # Exclude Indian places
                ):
                    doc = nlp(word)
                    if all(token.pos_ != 'VERB' for token in doc):  # Exclude verbs
                        filtered.add((word, score))  # Add to set to avoid duplicates
            return list(filtered)

        # Function to process each collection
        def process_collection(collection_name, keywords_collection_name):
            logging.info(f"Processing collection: {collection_name}")
            collection = db[collection_name]
            subreddit_data = pd.DataFrame(list(collection.find({}, {"_id": 1, "processed_text": 1})))  # Only fetch necessary fields

            if subreddit_data.empty:
                logging.warning(f"No data found in collection: {collection_name}")
                return

            all_keywords = []
            seen_keywords = set()  # To track unique keywords

            for _, row in subreddit_data.iterrows():
                processed_text = row.get('processed_text')

                # Check if processed_text is a valid string
                if not isinstance(processed_text, str):
                    logging.warning(f"Skipping invalid processed_text for ID {row['_id']}: {processed_text}")
                    continue

                try:
                    # Calculate the number of words in the processed_text
                    word_count = len(processed_text.split())

                    # Calculate the number of keywords dynamically based on word count
                    no_of_keywords = max(1, word_count // 5)

                    # Extract keywords
                    keywords = kw_model.extract_keywords(processed_text, top_n=500)

                    # Filter out keywords that are verbs, below the score threshold, or duplicate
                    filtered_keywords = filter_keywords(keywords, nlp, score_threshold=0.4)

                    # Add filtered keywords to the list, ensuring uniqueness
                    for word, score in filtered_keywords:
                        if word.lower() not in seen_keywords:
                            seen_keywords.add(word.lower())  # Add to seen set (case insensitive)
                            all_keywords.append({
                                '_id': row['_id'],
                                'keyword': word,
                                'score': score,
                                'source': collection_name  # Include the source collection name
                            })
                except Exception as e:
                    logging.error(f"Error processing text for ID {row['_id']}: {e}")

            # Create a DataFrame for the keywords
            keywords_df = pd.DataFrame(all_keywords)

            if not keywords_df.empty:
                # Sort keywords by score in descending order
                keywords_df = keywords_df.sort_values(by='score', ascending=False)

                # Remove the '_id' column for MongoDB insertion
                keywords_documents = keywords_df.drop('_id', axis=1).to_dict(orient='records')

                # Insert keywords into the specified collection (with the 'source' field)
                try:
                    if keywords_documents:
                        keywords_collection = db[keywords_collection_name]
                        inserted_keywords = keywords_collection.insert_many(keywords_documents)
                        logging.info(f"Inserted {len(inserted_keywords.inserted_ids)} keywords into '{keywords_collection_name}' collection.")
                except Exception as e:
                    logging.error(f"Error inserting keywords into MongoDB: {e}")

                # Save to a CSV file for reference
                output_file = f"{collection_name}_keywords.csv"
                keywords_df.drop('_id', axis=1, inplace=True)  # Remove '_id' from the CSV file
                keywords_df.to_csv(output_file, index=False)
                logging.info(f"Keywords extracted and saved to {output_file}")
            else:
                logging.warning(f"No valid keywords extracted for collection: {collection_name}")

        # Process the specified collections with separate output collections
        collections_to_process = {
            'subreddit-data': 'subreddit-keywords',
            'statewise-data': 'statewise-keywords'
        }

        for collection_name, keywords_collection_name in collections_to_process.items():
            logging.info(f"Starting processing for collection: {collection_name}")

            # Access the collection
            collection = db[collection_name]

            # Count the total number of documents in the collection
            total_documents = collection.count_documents({})  # Counts all documents in the collection

            # Log the number of documents
            logging.info(f"Total number of documents in {collection_name}: {total_documents}")

            # Process the collection
            process_collection(collection_name, keywords_collection_name)

            logging.info(f"Finished processing for collection: {collection_name}")

        # Close the MongoDB client
        client.close()
        logging.info("Processing completed.")
        return "Keyword Extraction completed successfully."
    
    except Exception as e:
        logging.error(f"Error: {str(e)}")
        return f"Error: {str(e)}"
