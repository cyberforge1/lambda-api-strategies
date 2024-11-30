# sequential/dam_details_collector.py
import os
import requests
import json
import time
from dotenv import load_dotenv

# Load environment variables from the .env file at the project root
load_dotenv()

# Extract the required variables from the .env file
API_KEY = os.getenv("API_KEY")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")

# Define the API base URL
BASE_URL = "https://api.onegov.nsw.gov.au"
ENDPOINT_TEMPLATE = "/waternsw-waterinsights/v1/dams/{dam_id}"

# Set up headers for the request
HEADERS = {
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "apikey": API_KEY,
}

# Function to fetch data for a specific dam by dam_id with retry logic
def fetch_dam_details(dam_id, retries=3, delay=5):
    for attempt in range(retries):
        try:
            endpoint = ENDPOINT_TEMPLATE.format(dam_id=dam_id)
            response = requests.get(BASE_URL + endpoint, headers=HEADERS)
            if response.status_code == 200:
                if attempt > 0:  # Indicates retries were successful
                    print(f"Successfully fetched details for dam_id {dam_id} after {attempt} retries.")
                return response.json()
            elif response.status_code == 408:  # Traffic limit exceeded
                print(f"Traffic limit exceeded for dam_id {dam_id}. Retrying in {delay} seconds... (Attempt {attempt + 1}/{retries})")
                time.sleep(delay)
            elif response.status_code == 422:  # Invalid dam ID or server error
                print(f"Error: Invalid dam_id {dam_id} or internal server error (status 422). Skipping...")
                return None
            else:
                print(f"Error: Received status code {response.status_code} for dam_id {dam_id}")
                print(f"Response: {response.text}")
                return None
        except requests.exceptions.RequestException as e:
            print(f"An error occurred while fetching details for dam_id {dam_id}: {e}")
    print(f"Failed to fetch details for dam_id {dam_id} after {retries} retries.")
    return None

# Function to load the dam_data.json file
def load_dam_data(file_path):
    try:
        with open(file_path, "r") as json_file:
            return json.load(json_file)
    except FileNotFoundError:
        print(f"Error: File {file_path} not found.")
        return None
    except json.JSONDecodeError as e:
        print(f"Error: Failed to decode JSON file {file_path}: {e}")
        return None

# Save the details of all dams to a JSON file
def save_all_dam_details(all_details, output_file_path):
    try:
        with open(output_file_path, "w") as json_file:
            json.dump(all_details, json_file, indent=4)
        print(f"All dam details saved to {output_file_path}")
    except Exception as e:
        print(f"An error occurred while saving the dam details to JSON: {e}")

# Main execution
if __name__ == "__main__":
    input_file_path = os.path.join(os.path.dirname(__file__), "dam_data.json")
    output_file_path = os.path.join(os.path.dirname(__file__), "all_dam_details.json")

    # Load dam data
    dam_data = load_dam_data(input_file_path)
    if dam_data is None:
        print("Unable to proceed due to missing or invalid dam_data.json.")
    else:
        all_dam_details = []
        for dam in dam_data.get("dams", []):
            dam_id = dam.get("dam_id")
            if dam_id is not None:
                print(f"Fetching details for dam_id {dam_id}...")
                dam_details = fetch_dam_details(dam_id)
                if dam_details:
                    all_dam_details.append(dam_details)

        # Save all dam details to a JSON file
        save_all_dam_details(all_dam_details, output_file_path)