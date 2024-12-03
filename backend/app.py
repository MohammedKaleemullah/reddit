from flask import Flask, jsonify, request
from subreddit_scraping import run_subreddit_scraping
from statewise_scraping import run_statewise_scraping
from data_preprocessing import run_data_preprocessing
from keyword_extraction import run_keyword_extraction
import logging
from flask_cors import CORS


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

@app.route('/api/run_subreddit_scraping', methods=['POST'])
def run_subreddit_scraping_route():
    try:
        logger.info("Received request to run subreddit scraping.")
        result = run_subreddit_scraping()
        logger.info(f"Subreddit scraping completed: {result}")
        return jsonify({"message": result}), 200
    except Exception as e:
        logger.error(f"Error running subreddit scraping: {str(e)}")
        return jsonify({"error": "An error occurred during subreddit scraping."}), 500

@app.route('/api/run_statewise_scraping', methods=['POST'])
def run_statewise_scraping_route():
    try:
        logger.info("Received request to run statewise scraping.")
        result = run_statewise_scraping()
        logger.info(f"Statewise scraping completed: {result}")
        return jsonify({"message": result}), 200
    except Exception as e:
        logger.error(f"Error running statewise scraping: {str(e)}")
        return jsonify({"error": "An error occurred during statewise scraping."}), 500

@app.route('/api/run_data_preprocessing', methods=['POST'])
def run_data_preprocessing_route():
    try:
        logger.info("Received request to run data preprocessing.")
        result = run_data_preprocessing()
        logger.info(f"Data preprocessing completed: {result}")
        return jsonify({"message": result}), 200
    except Exception as e:
        logger.error(f"Error running data preprocessing: {str(e)}")
        return jsonify({"error": "An error occurred during data preprocessing."}), 500

@app.route('/api/run_keyword_extraction', methods=['POST'])
def run_keyword_extraction_route():
    try:
        logger.info("Received request to run keyword extraction.")
        result = run_keyword_extraction()
        logger.info(f"Keyword Extraction completed: {result}")
        return jsonify({"message": result}), 200
    except Exception as e:
        logger.error(f"Error running Keyword Extraction: {str(e)}")
        return jsonify({"error": "An error occurred during Keyword Extraction."}), 500

if __name__ == '__main__':
    app.run(debug=True)
