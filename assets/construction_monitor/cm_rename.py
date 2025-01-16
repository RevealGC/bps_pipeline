''' This script is to rename all cm files into cm specific date format '''


from datetime import datetime, timedelta
from dotenv import load_dotenv
import calendar
import re
import os

load_dotenv()

local = os.getenv("local_path") 

def first_date_of_week(year, week_of_year):
    jan1 = datetime(year, 1, 1)
    days_offset = (calendar.THURSDAY - jan1.weekday())
    first_thursday = jan1 + timedelta(days=days_offset)
    first_week = first_thursday.isocalendar()[1]

    if first_week <= 1:
        week_of_year -= 1

    result = first_thursday + timedelta(weeks=week_of_year)
    return (result - timedelta(days=(result.weekday() - calendar.MONDAY))).strftime('%Y_%m_%d')

def rename_file(filename):
    pattern = r"([a-zA-Z]+)-([a-zA-Z]+)-(\d{4})-(\d+)"
    match = re.match(pattern, filename)
    if match:
        year = match.group(3)
        week_number = match.group(4)
        date_str = first_date_of_week(int(year), int(week_number))
        new_filename = f"cm_orig_{date_str}.csv"
        return new_filename
    return filename

def rename_files_in_directory(directory):
    """
    Renames all files in the given directory based on the rename_file logic.
    Args:
        directory (str): Path to the directory containing files
    """
    if not os.path.exists(directory):
        print(f"Directory does not exist: {directory}")
        return

    for filename in os.listdir(directory):
        old_path = os.path.join(directory, filename)
        if os.path.isfile(old_path):
            new_filename = rename_file(filename)
            if new_filename != filename:
                new_path = os.path.join(directory, new_filename)
                os.rename(old_path, new_path)
                print(f"Renamed: {filename} -> {new_filename}")
            else:
                print(f"No change for: {filename}")
        else:
            print(f"Skipping non-file: {filename}")

directory_path = local 
rename_files_in_directory(directory_path)
