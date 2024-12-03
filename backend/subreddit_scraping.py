import logging
from pymongo.mongo_client import MongoClient
from pymongo.errors import DuplicateKeyError
import pandas as pd
import praw


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_subreddit_scraping():
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
        collection = db['subreddit-data']

        # List of subreddits to scrape
        actual_subreddits = ["IndianEnts", "Pune_Ents", "BhangEnts"]

        # Scraping data from multiple subreddits
        submission_data = []

        # Loop through each subreddit in actual_subreddits
        # Scraping data from multiple subreddits
        submission_data = []

        # Loop through each subreddit in actual_subreddits
        for subreddit_name in actual_subreddits:
            try:
                logger.info(f"Scraping subreddit: {subreddit_name}")
                subreddit_posts = []  # Temporary list to store posts for this subreddit
                for submission in reddit.subreddit(subreddit_name).hot(limit=250):
                    submission.comments.replace_more(limit=100)  # Remove 'MoreComments'
                    comments = [comment.body for comment in submission.comments.list()]
                    subreddit_posts.append({
                        "title": submission.title,
                        "selftext": submission.selftext,
                        "comments": comments,
                        "subreddit": subreddit_name  # Include subreddit name for identification
                    })

                logger.info(f"Successfully scraped {len(subreddit_posts)} posts from {subreddit_name}")
                submission_data.extend(subreddit_posts)  # Add to the main list
            except Exception as e:
                logger.error(f"Error scraping {subreddit_name}: {e}")

        # Log total posts scraped across all subreddits
        logger.info(f"Total posts scraped across all subreddits: {len(submission_data)}")


        # Check if there is data to insert
        if submission_data:
            # Convert the data to a DataFrame
            subreddit_data = pd.DataFrame(submission_data)

            # Convert the DataFrame to a list of dictionaries
            subreddit_documents = subreddit_data.to_dict(orient='records')

            try:
                # Insert data into the collection
                inserted_subreddit = collection.insert_many(subreddit_documents, ordered=False)
                logger.info(f"Inserted {len(inserted_subreddit.inserted_ids)} documents into 'subreddit-data' collection.")
            except DuplicateKeyError:
                logger.warning("Duplicate documents were not inserted.")
            except Exception as e:
                logger.error(f"Error inserting data into MongoDB: {e}")
        else:
            logger.warning("No data scraped. Nothing to insert into MongoDB.")

    except Exception as e:
        logger.error(f"Error during subreddit scraping: {str(e)}")
        return f"Error: {str(e)}"
    finally:
        # Close the MongoDB client regardless of success or failure
        client.close()
        logger.info("MongoDB connection closed.")

    return "Subreddit scraping completed successfully."
