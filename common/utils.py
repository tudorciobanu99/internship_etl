import os
from datetime import datetime
import json
import shutil
import csv

def today():
    return datetime.now().strftime("%Y-%m-%d")

def timestamp():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

def save_to_json(data, import_dir_name, import_file_name):
    if not os.path.exists(import_dir_name):
        os.makedirs(import_dir_name)

    file_path = os.path.join(import_dir_name, import_file_name)
    with open(file_path, "w", encoding="utf-8") as outfile:
        json.dump(data, outfile, indent=4)

def get_json_row_count(import_dir_name, import_file_name):
    try:
        file_path = os.path.join(import_dir_name, import_file_name)
        data = open_file(file_path)

        if isinstance(data, str):
            return 1
        elif isinstance(data, dict):
            return sum(
                len(value) if isinstance(value, (dict, list)) else 1
                for value in data.values()
            )
    except (FileNotFoundError, json.JSONDecodeError):
        return 0

def list_all_files_from_directory(directory):
    files = []
    for root, _, filenames in os.walk(directory):
        for filename in filenames:
            files.append(os.path.join(root, filename))
    return files

def get_file_details(filename):
    country_code = filename.split("/")[-1].split("_")[2]
    batch_date = filename.split("/")[-1].split("_")[3].split(".")[0]
    return country_code, batch_date

def open_file(filename):
    try:
        with open(filename, "r", encoding="utf-8") as infile:
            data = json.load(infile)
            return data
    except (FileNotFoundError, json.JSONDecodeError):
        return None

def move_file(file, dir_name, file_name):
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)
    file_path = os.path.join(dir_name, file_name)
    shutil.move(file, file_path)

def get_weather_description(weather_code, csv_file_path="weather_description/wmo_code_4677.csv"):
    try:
        with open(csv_file_path, mode="r", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            for row in reader:
                if row["Weather Code"] == weather_code:
                    return row["Description"]
    except (FileNotFoundError, KeyError):
        return None
