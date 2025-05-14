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

def get_row_count(import_dir_name, import_file_name, status_code, api_type):
    """
    Counts the number of rows in a .json file.

    Args:
        import_dir_name (str): The name of the directory.
        import_file_name (str): The name of the file.
        status_code (int): The HTTP status code.
        api_type (str): Either "c" (COVID) or "w" (weather).

    Returns:
        1: If the file exists and is associated with a 200
            HTTP response code.
        0: If the file does not exist or is associated with a
            HTTP response code different than 200. Additionally,
            for the chosen COVID API, for an invalid ISO code, the 
            response returns a 200 status code, surprisingly. The 
            response body does not contain any legible information.
            Therefore, the function returns 0 in this case.
    """

    file_path = os.path.join(import_dir_name, import_file_name)

    if not os.path.exists(file_path):
        return 0
    
    if status_code != 200:
        return 0
    
    if api_type == "c":
        data = open_file(file_path)
        if not data.get("data"):
            return 0 
    return 1
    
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
        country_code = filename.split("/")[-1].split("_")[1]
        batch_date = filename.split("/")[-1].split("_")[2].split(".")[0]
        return country_code, batch_date
    except Exception:
        return None, None

def check_expected_format(filename):
    """
    Checks whether the file follows the expected naming convention,
    (i.e covid/weather_data_countrycode_batchdate), which includes
    a valid batch date and a potential country_code.

    Args:
        filename (str): The name of the file

    Returns:
        country_code (str): The country code.
        batch_date (str): The batch date.
    """

    country_code, batch_date = get_file_details(filename)
    try:
        if all([country_code, batch_date]):
            datetime.strptime(batch_date, "%Y-%m-%d")
            return country_code, batch_date
    except ValueError:
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
                    description = str(row["Description"])
                    return description
    except (FileNotFoundError, KeyError):
        return None
