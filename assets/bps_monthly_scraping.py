"""bps_monthly_data.py"""
import os
from urllib.parse import urljoin
# from io import StringIO
import requests
# import pandas as pd
from bs4 import BeautifulSoup
from dagster import asset, IOManager, io_manager, Definitions, resource


class FileStoreIOManager(IOManager):
    """
    This IOManager keeps track of processed files by appending to a text file (processed_files.txt).
    """

    def __init__(self, destination_path):
        self.destination_path = destination_path

    def handle_output(self, context, obj):
        os.makedirs(self.destination_path, exist_ok=True)
        file_path = os.path.join(self.destination_path, obj['filename'])
        with open(file_path, 'wb') as f:
            f.write(obj['content'])

    def load_input(self, context):
        asset_path = context.asset_key.path[-1]
        file_path = os.path.join(self.destination_path, asset_path)
        if os.path.exists(file_path):
            with open(file_path, 'rb') as f:
                return f.read()
        return None


@io_manager(config_schema={"destination_path": str})
def processed_files_io_manager(init_context):
    """IO manager for accessing processed files"""
    return FileStoreIOManager(destination_path=init_context.resource_config["destination_path"])


@resource(config_schema={"destination_path": str})
def file_store_resource(init_context):
    """needs docstring"""
    return {"destination_path": init_context.resource_config["destination_path"]}


def list_files_in_directory(url):
    """
    This function lists all '.txt' files in the given URL directory.
    It does not crawl subdirectories.
    """
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error accessing {url}: {e}")
        return []

    soup = BeautifulSoup(response.content, 'html.parser')

    return [
        urljoin(url, link.get('href'))
        for link in soup.find_all('a')
        if link.get('href', '').endswith('.txt')
        ]


def http_files(directory_urls):
    """
    http_files
    """
    all_txt_files = []
    for url in directory_urls:
        txt_files = list_files_in_directory(url)
        all_txt_files.extend(txt_files)
    return all_txt_files


@asset(io_manager_key="file_store_io_manager")
def download_new_bps_files(context):
    """
    This asset takes the list of files from the HTTP server
    and filters out the files that have already been processed.
    It processes the new files and updates the record of processed files.
    """
    directory_urls = [
        'https://www2.census.gov/econ/bps/Place/Midwest%20Region/',
        'https://www2.census.gov/econ/bps/Place/Northeast%20Region/',
        'https://www2.census.gov/econ/bps/Place/South%20Region/',
        'https://www2.census.gov/econ/bps/Place/West%20Region/',
        ]
    files_to_download = http_files(directory_urls)
    downloaded_files = []

    for file_url in files_to_download:
        filename = os.path.basename(file_url)
        if not context.resources.file_store_io_manager.load_input(context):
            try:
                response = requests.get(file_url, timeout= 5)
                response.raise_for_status()
                context.resources.file_store_io_manager.handle_output(
                    context, {"filename": filename, "content": response.content})
                downloaded_files.append(filename)
                print(f"Downloaded: {filename}")
            except requests.exceptions.RequestException as e:
                print(f"Failed to download {file_url}: {e}")

    return downloaded_files


defs = Definitions(
    assets=[download_new_bps_files],
    resources={
        "file_store_io_manager": processed_files_io_manager.configured(
            {"destination_path": "data/downloaded_files"}),
        "file_store_resource": file_store_resource.configured(
            {"destination_path": "data/downloaded_files"})
    }
)
