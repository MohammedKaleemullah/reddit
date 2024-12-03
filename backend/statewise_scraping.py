from pymongo.mongo_client import MongoClient
from pymongo.errors import DuplicateKeyError
import pandas as pd
import praw

def run_statewise_scraping():
    try:
        # Reddit API setup
        user_agent = "Scrapper 1.0 by /u/Mindless_Grocery_947"
        reddit = praw.Reddit(
            client_id="ruNa9x3IAftZnTuYRck4gw",
            client_secret="NHzTAq1GkTq5_cwg8Ms9WF-jQH5aDQ",
            user_agent=user_agent
        )

        # MongoDB connection URI
        uri = "mongodb+srv://mohammedkaleemullah06:aeqaasGuclMt0xtO@cluster0.khzha.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
        client = MongoClient(uri)

        # Access the database and collections
        db = client['Reddit-Scraping-New']
        collection = db['statewise-data']

        all_state_names = [
            "andhra_pradesh", "ArunachalPradesh", "assam", "bihar", "Chhattisgarh", "Goa", "gujarat", "Haryana", 
            "HimachalPradesh", "Jharkhand", "karnataka", "Kerala", "MadhyaPradesh", "Maharashtra", "manipur", 
            "Meghalaya", "mizo", "NAGALAND", "Odisha", "punjab", "Rajasthan", "sikkim", "TamilNadu", "Telangana", 
            "TripuraNE", "uttarpradesh", "Uttarakhand", "westbengal", "Andaman_and_Nicobar", 
            "Chandigarh", "DNH", "Lakshadweep_Islands", "ladakh", "pondicherry"
        ]

        # Scraping data from state-specific subreddits related to the keyword 'drug'
        statewise_submission_data = []

        # Loop through each state name and scrape data
        for state in all_state_names:
            try:
                print(f"Scraping subreddit: {state}")
                for submission in reddit.subreddit(state).search('drug', limit=100):
                    submission.comments.replace_more(limit=50)  # Remove 'MoreComments'
                    comments = [comment.body for comment in submission.comments.list()]
                    statewise_submission_data.append({
                        "title": submission.title,
                        "selftext": submission.selftext,
                        "comments": comments,
                        "subreddit": state  # Include state name for identification
                    })
                print(f"Finished scraping subreddit: {state}")
            except Exception as e:
                print(f"Error scraping {state}: {e}")

        # Check if there is data to insert
        if statewise_submission_data:
            # Convert the state-specific data to a DataFrame
            statewise_data = pd.DataFrame(statewise_submission_data)

            # Convert the DataFrame to a list of dictionaries
            statewise_documents = statewise_data.to_dict(orient='records')

            try:
                # Insert data into the collection
                inserted_statewise = collection.insert_many(statewise_documents)
                print(f"Inserted {len(inserted_statewise.inserted_ids)} documents into 'statewise-data' collection.")
            except DuplicateKeyError as e:
                print("Duplicate documents were not inserted.")
            except Exception as e:
                print(f"Error inserting data into MongoDB: {e}")
        else:
            print("No data scraped. Nothing to insert into MongoDB.")

        return "Statewise scraping completed successfully."
    
    except Exception as e:
        return f"Error: {str(e)}"
    
    finally:
        # Close the MongoDB client
        client.close()
