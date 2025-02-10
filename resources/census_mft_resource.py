""" 
# census_mft_resource.py
This script transfers files to census via MFT
"""

import base64
import subprocess
from pathlib import Path
import dagster as dg


class MFTClient:
    """
    A client for sending files to a Managed File Transfer (MFT) server using cURL.

    Attributes:
        url (str): The base URL of the MFT server.
        username (str): The username for authentication.
        password (str): The password for authentication.

    Methods:
        send_file(target_file: Path, dest_name: str, dest_folder="") -> subprocess.CompletedProcess:
            Uploads a file to the specified MFT destination folder.
    """

    def __init__(self, url: str, username: str, password: str):
        """
        Initializes the MFTClient with server credentials.

        Args:
            url (str): The base URL of the MFT server.
            username (str): The username for authentication.
            password (str): The password for authentication.
        """
        self.url = url
        self.username = username
        self.password = password

    def send_file(self, target_file: Path, dest_name: str, dest_folder=""):
        """
        Uploads a file to the MFT server.

        Args:
            target_file (Path): The local file path to upload.
            dest_name (str): The name the file should have on the MFT server.
            dest_folder (str, optional): The destination folder on the MFT server. Defaults to "".

        Returns:
            subprocess.CompletedProcess: The result of the cURL command execution.

        Raises:
            FileNotFoundError: If the specified file does not exist.
            ValueError: If authentication credentials are missing.
        """

        # create user token
        file_path = Path(target_file).resolve()
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        folder_url = self.url + dest_folder

        user_token = f"{self.username}:{self.password}"
        encoded_token = base64.b64encode(user_token.encode()).decode()

        # verify file exists

        # create curl command
        mft_str = (
            f'curl -X POST "{folder_url}{dest_name}?packet=1&position=0&final=true" '
            f'-H "Authorization: Basic {encoded_token}" '
            f'-T "{target_file}" -v'
        )

        # send file wit subprocess logging
        curl_result = subprocess.run(
            mft_str, shell=True, capture_output=True, text=True, check=True
        )

        return curl_result


class MFTResource(dg.ConfigurableResource):
    """
    A Dagster resource for interacting with a Managed File Transfer (MFT) server.
    """

    url: str
    username: str
    password: str

    def get_client(self) -> MFTClient:
        """
        Returns an MFTClient instance with the resource's credentials.
        """
        return MFTClient(self.url, self.username, self.password)


# Example usage of MFTClient
if __name__ == "__main__":
    import os

    # Load environment variables for credentials (or set manually)
    MFT_URL = os.getenv(
        "MFT_BASE_URL", "https://mft.econ.census.gov/cfcc/rest/ft/v3/transfer/"
    )
    MFT_USERNAME = os.getenv("your_username", default="MFT_USERNAME")
    MFT_PASSWORD = os.getenv("your_password", default="MFT_PASSWORD")

    # Create an MFTClient instance
    mft_client = MFTClient(url=MFT_URL, username=MFT_USERNAME, password=MFT_PASSWORD)

    # Define file to upload

    try:
        # Upload file to MFT server
        result = mft_client.send_file(
            Path("test_dir/test_file.csv"), "uploaded_test_file.csv", "rgc_rawdata_cm"
        )

        # Print upload response
        print("STDOUT:", result.stdout)
        print("STDERR:", result.stderr)

    except FileNotFoundError as e:
        print("File not found error:", e)
    except subprocess.CalledProcessError as e:
        print("Subprocess error:", e)
