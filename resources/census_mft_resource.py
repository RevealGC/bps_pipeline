""" 
# census_mft_resource.py
This script transfers files to census via MFT
"""

import base64
import re
import subprocess
from typing import Optional
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

    def __init__(
        self,
        url: str,
        token: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
    ):
        """
        Initializes the MFTClient with server credentials.

        Args:
            url (str): The base URL of the MFT server.
            username (str): The username for authentication.
            password (str): The password for authentication.
        """
        self.url = url
        if token is None and (username is None or password is None):
            raise ValueError("Either token or username and password must be provided.")
        if token is not None and (username is not None or password is not None):
            raise ValueError(
                "Only one of token or username and password can be provided."
            )

        self.token = (
            token if token is not None else self._encode_credentials(username, password)
        )

    def _encode_credentials(self, username, password) -> str:
        """
        Encodes the username and password for use in HTTP Basic Authentication.

        Returns:
            str: The encoded credentials.
        """
        return base64.b64encode(f"{username}:{password}".encode()).decode()

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

        # verify file exists

        # create curl command
        mft_str = (
            f'curl -X POST "{folder_url}/{dest_name}?packet=1&position=0&final=true" '
            f'-H "Authorization: Basic {self.token}" '
            f'-T "{target_file}" -v'
        )

        try:
            # send file wit subprocess logging
            result = subprocess.run(
                mft_str, shell=True, capture_output=True, text=True, check=True
            )
            success = True

        except subprocess.CalledProcessError as e:
            result = e
            success = False

        redacted_stdout = self._scrub_sensitive_data(result.stdout)
        redacted_stderr = self._scrub_sensitive_data(result.stderr)

        return {
            "stdout": redacted_stdout,
            "stderr": redacted_stderr,
            "returncode": result.returncode,
            "error": None if success else "File transfer failed.",
        }

    def _scrub_sensitive_data(self, text):
        """
        Removes sensitive information from subprocess output before logging.

        Args:
            text (str): The original stdout or stderr text.

        Returns:
            str: The redacted text with authentication details removed.
        """
        if not text:
            return "No output"

        # Use regex to remove the Authorization header details
        redacted_text = re.sub(
            r"Authorization: Basic [A-Za-z0-9+/=]+",
            "Authorization: Basic ***REDACTED***",
            text,
        )

        return redacted_text


class MFTResource(dg.ConfigurableResource):
    """
    A Dagster resource for interacting with a Managed File Transfer (MFT) server.
    """

    url: str
    username: str
    password: str

    def _encode_credentials(self) -> str:
        """
        Encodes the username and password for use in HTTP Basic Authentication.

        Returns:
            str: The encoded credentials.
        """
        return base64.b64encode(f"{self.username}:{self.password}".encode()).decode()

    def get_client(self) -> MFTClient:
        """
        Returns an MFTClient instance with the resource's credentials.
        """
        encoded_token = self._encode_credentials()
        return MFTClient(url=self.url, token=encoded_token)


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
        res = mft_client.send_file(
            Path("test_dir/test_file.csv"), "uploaded_test_file.csv", "rgc_rawdata_cm"
        )

        # Print upload response
        print("STDOUT:", res["stdout"])
        print("STDERR:", res["stderr"])

    except FileNotFoundError as e:
        print("File not found error:", e)
    except subprocess.CalledProcessError as e:
        print("Subprocess error:", e)
