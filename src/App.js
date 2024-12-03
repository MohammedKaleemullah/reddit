import React, { useState } from 'react';
import './App.css'; // Import the CSS file for styling

function App() {
    const [message, setMessage] = useState('');
    const [loading, setLoading] = useState(false);

    const runProcess = async (endpoint) => {
        setLoading(true);
        setMessage('');
        try {
            const response = await fetch(`http://localhost:5000/api/${endpoint}`, {
                method: 'POST',
            });

            const result = await response.json();
            setMessage(result.message || result.error);
        } catch (error) {
            setMessage('An error occurred while triggering the process');
        }
        setLoading(false);
    };

    return (
        <div className="app-container">
            <h1 className="app-title">Reddit Scraping & Analysis</h1>
            <div className="button-container">
                <button
                    onClick={() => runProcess('run_subreddit_scraping')}
                    disabled={loading}
                    className="app-button"
                >
                    {loading ? 'Running Subreddit Scraping...' : 'Run Subreddit Scraping'}
                </button>
                <button
                    onClick={() => runProcess('run_statewise_scraping')}
                    disabled={loading}
                    className="app-button"
                >
                    {loading ? 'Running Statewise Scraping...' : 'Run Statewise Scraping'}
                </button>
                <button
                    onClick={() => runProcess('run_data_preprocessing')}
                    disabled={loading}
                    className="app-button"
                >
                    {loading ? 'Running Data Preprocessing...' : 'Run Data Preprocessing'}
                </button>
                <button
                    onClick={() => runProcess('run_keyword_extraction')}
                    disabled={loading}
                    className="app-button"
                >
                    {loading ? 'Running Keyword Extraction...' : 'Run Keyword Extraction'}
                </button>
            </div>
            <div className="message-container">
                {message && <p className="app-message">{message}</p>}
            </div>
        </div>
    );
}

export default App;
