import requests
from bs4 import BeautifulSoup
import csv
import re

def scrape_wmo_code_4677(url):
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Failed to fetch the page. Status code: {response.status_code}")
        return []

    soup = BeautifulSoup(response.content, 'html.parser')

    # Locate the section for "Present weather (from manned (code 4677)..."
    header = None
    for h3 in soup.find_all('h3'):
        if "Present weather (from manned (code 4677)" in h3.get_text(strip=True):
            header = h3
            break

    if not header:
        print("Section for WMO code 4677 not found.")
        return []

    # Find the <pre> tag following the <h3> header
    pre_tag = header.find_next('pre')
    if not pre_tag:
        print("No <pre> tag found for WMO code 4677.")
        return []

    # Extract the text from the <pre> tag
    pre_text = pre_tag.get_text()

    # Parse the text to extract weather codes and descriptions
    weather_data = []
    current_code = None
    current_description = []

    for line in pre_text.splitlines():
        # Match lines that start with a weather code (e.g., "00 - Description")
        match = re.match(r"^(\d{2}) - (.+)", line)
        if match:
            # If there's an existing code and description, save it
            if current_code is not None:
                description = " ".join(current_description).strip()
                description = re.sub(r"\s+", " ", description)  # Remove extra spaces
                weather_data.append((current_code, description))
            
            # Start a new weather code and description
            current_code = match.group(1)
            current_description = [match.group(2)]
        else:
            # Append to the current description if it's a continuation
            current_description.append(line)

    # Add the last weather code and description
    if current_code is not None:
        description = " ".join(current_description).strip()
        description = re.sub(r"\s+", " ", description)  # Remove extra spaces
        weather_data.append((current_code, description))

    return weather_data

def save_to_csv(data, filename):
    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['Weather Code', 'Description'])
        writer.writerows(data)
    print(f"Weather codes saved to {filename}")

if __name__ == "__main__":
    url = "https://artefacts.ceda.ac.uk/badc_datadocs/surface/code.html"
    weather_data = scrape_wmo_code_4677(url)
    if weather_data:
        save_to_csv(weather_data, "wmo_code_4677.csv")