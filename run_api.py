"""
Entry point for running the Flask Web API.
"""
from src.presentation.api import app

if __name__ == "__main__":
    print("Starting Portfolio Tracker API on http://127.0.0.1:5001")
    app.run(host='0.0.0.0', port=5001, debug=True)
