import csv
import re
import requests
from bs4 import BeautifulSoup

def scrape_wmo_code_4677(url):
    response = requests.get(url, timeout=10)
    if response.status_code != 200:
        return []

    soup = BeautifulSoup(response.content, 'html.parser')

    header = None
    for h3 in soup.find_all('h3'):
        if "Present weather (from manned (code 4677)" in h3.get_text(strip=True):
            header = h3
            break

    if not header:
        print("Section for WMO code 4677 not found.")
        return []

    pre_tag = header.find_next('pre')
    if not pre_tag:
        return []

    pre_text = pre_tag.get_text()

    weather_data = []
    current_code = None
    current_description = []

    for line in pre_text.splitlines():
        match = re.match(r"^(\d{2}) - (.+)", line)
        if match:
            if current_code is not None:
                description = " ".join(current_description).strip()
                description = re.sub(r"\s+", " ", description)  # Remove extra spaces
                weather_data.append((current_code, description))

            current_code = match.group(1)
            current_description = [match.group(2)]
        else:
            current_description.append(line)

    if current_code is not None:
        description = " ".join(current_description).strip()
        description = re.sub(r"\s+", " ", description)
        weather_data.append((current_code, description))

    return weather_data

def save_to_csv(data, filename):
    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['Weather Code', 'Description'])
        writer.writerows(data)

if __name__ == "__main__":
    URL = "https://artefacts.ceda.ac.uk/badc_datadocs/surface/code.html"
    weather_data = scrape_wmo_code_4677(URL)
    if weather_data:
        save_to_csv(weather_data, "wmo_code_4677.csv")
