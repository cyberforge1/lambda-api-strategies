# sequential/dam_id_collection.py
import os
import requests
import json
from dotenv import load_dotenv

# Load environment variables from the .env file at the project root
load_dotenv()

# Extract the required variables from the .env file
API_KEY = os.getenv("API_KEY")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")

# Define the API URL
BASE_URL = "https://api.onegov.nsw.gov.au"
ENDPOINT = "/waternsw-waterinsights/v1/dams"

# Set up headers for the request
HEADERS = {
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "apikey": API_KEY,
}

# Function to fetch dam data from the API
def fetch_dam_data():
    try:
        # Make a GET request to the API
        response = requests.get(BASE_URL + ENDPOINT, headers=HEADERS)
        # Check if the response is successful
        if response.status_code == 200:
            return response.json()  # Return the response JSON data
        else:
            print(f"Error: Received status code {response.status_code}")
            print(f"Response: {response.text}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return None

# Save the data to a JSON file
def save_to_json(data, file_path):
    try:
        with open(file_path, "w") as json_file:
            json.dump(data, json_file, indent=4)
        print(f"Data saved to {file_path}")
    except Exception as e:
        print(f"An error occurred while saving to JSON: {e}")

# Main execution
if __name__ == "__main__":
    # Fetch data from the API
    dam_data = fetch_dam_data()
    if dam_data:
        # Save the data to a JSON file
        json_file_path = os.path.join(os.path.dirname(__file__), "dam_data.json")
        save_to_json(dam_data, json_file_path)
