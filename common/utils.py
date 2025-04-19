import os
from datetime import datetime
import json
import shutil
import csv

def today():
    """
    Fetches today's date.

    Returns:
        today (str): String representation of the today's date
            in the YYYY-MM-DD format.
    """

    today = datetime.now().strftime("%Y-%m-%d")
    return today

def timestamp():
    """
    Fetches the timestamp at the given moment.

    Returns:
        timestamp (str): String representation of the timestamp
            in the YYYY-MM-DD HH-MM-SS-mmmm format.
    """

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    return timestamp

def save_to_json(data, import_dir_name, import_file_name):
    """
    Saves data into a .json file.

    Intended args:
        data (dict): The data.
        import_dir_name (str): The name of the directory to which the
            file should be saved.
        import_file_name (str): The name of the file.
    """

    if not os.path.exists(import_dir_name):
        os.makedirs(import_dir_name)

    file_path = os.path.join(import_dir_name, import_file_name)
    with open(file_path, "w", encoding="utf-8") as outfile:
        json.dump(data, outfile, indent=4)

def get_json_row_count(import_dir_name, import_file_name):
    """
    Counts the number of rows in a .json file.

    Args:
        import_dir_name (str): The name of the directory.
        import_file_name (str): The name of the file.

    Returns:
        1: If the file contains just a string.
        row_count (int): If the file contains a dictionary,
            the row count corresponds to the number of keys
            and sub-keys.
        0: If the file does not exist or cannot be decoded.
    """

    try:
        file_path = os.path.join(import_dir_name, import_file_name)
        data = open_file(file_path)

        if isinstance(data, str):
            return 1
        elif isinstance(data, dict):
            row_count = sum(
                len(value) if isinstance(value, (dict, list)) else 1
                for value in data.values()
            )
            return row_count
    except (FileNotFoundError, json.JSONDecodeError):
        return 0

def list_all_files_from_directory(directory):
    """
    Lists all files in a given directory.

    Args:
        directory (str): The name of the directory.

    Returns:
        files (list): The names of the files contained
            in the directory.
    """

    files = []
    for root, _, filenames in os.walk(directory):
        for filename in filenames:
            files.append(os.path.join(root, filename))
    return files

def get_file_details(filename):
    """
    Extracts the country code and batch date for a given file name.
    Inherently expects a file name that fulfills the above condition.

    Args:
        filename (str): The name of the file.

    Returns:
        country_code (str): The country code.
        batch_date (str): The batch date.
    """

    try:
        country_code = filename.split("/")[-1].split("_")[2]
        batch_date = filename.split("/")[-1].split("_")[3].split(".")[0]
        return country_code, batch_date
    except Exception:
        pass

def open_file(filename):
    """
    Extracts the data from a .json file.

    Args:
        filename (str): The name of the file.

    Returns:
        data: The data contained in the file.
    """

    try:
        with open(filename, "r", encoding="utf-8") as infile:
            data = json.load(infile)
            return data
    except (FileNotFoundError, json.JSONDecodeError):
        return None

def move_file(file, dir_name, file_name):
    """
    Moves a file from a source directory to a target directory.

    Args:
        file (str): The full path to the file.
        dir_name (str): The target directory.
        file_name (str): The name of the file in the target
            directory.
    """

    if not os.path.exists(dir_name):
        os.makedirs(dir_name)

    file_path = os.path.join(dir_name, file_name)
    shutil.move(file, file_path)

def get_weather_description(weather_code, csv_file_path="weather_description/wmo_code_4677.csv"):
    """
    Fetches the corresponding weather description to a WMO 4677 weather code.

    Args:
        weather_code (int): A WMO 4677 weather code.
        csv_file_path (str): The path to a .csv file that contains two columns:
            Weather Code | Description

    Returns:
        description (str): The corresponding weather description.
    """

    try:
        with open(csv_file_path, mode="r", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            for row in reader:
                if row["Weather Code"] == weather_code:
                    description = row["Description"]
                    return description
    except (FileNotFoundError, KeyError):
        return None
