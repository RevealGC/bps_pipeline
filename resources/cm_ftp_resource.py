"""
cm_ftp_resource.py
"""

# %%
from io import BytesIO
from typing import List
from ftplib import FTP_TLS, error_perm
from dagster import ConfigurableResource
from utilities.dagster_utils import log


class FTPClient:
    """
    An FTP client that provides FTP connection management for retrieving and storing files.
    The connection is closed after each operation.
    """

    def __init__(self, host: str, username: str, password: str, timeout: int = 30):
        self.host = host
        self.username = username
        self.password = password
        self.timeout = timeout
        self.ftp = None  # Maintain connection state

    def disconnect(self):
        """
        Close the FTP connection.
        """
        if self.ftp:
            try:
                self.ftp.quit()
                log("FTP connection closed successfully.")
            except error_perm as e:
                log(f"Failed to close FTP connection: {e}")
            finally:
                self.ftp = None

    def connect(self):
        """
        Connect to the FTP server.
        """
        log(f"Connecting to FTP server {self.host}...")
        self.ftp = FTP_TLS()
        self.ftp.connect(self.host, timeout=self.timeout)
        self.ftp.auth()
        self.ftp.prot_p()
        self.ftp.login(self.username, self.password)
        log("FTP connection established.")

    def list_files(self, remote_dir: str = ".") -> List[str]:
        """
        List files in the specified directory on the FTP server.
        """
        if not self.ftp:
            self.connect()

        try:
            return self.ftp.nlst(remote_dir)
        finally:
            self.disconnect()

    def download_file(self, file_name: str) -> BytesIO:
        """
        Download a file from the FTP server and return it as a BytesIO stream.
        """
        if not self.ftp:
            self.connect()

        file_stream = BytesIO()
        try:
            log(f"Downloading file: {file_name}")
            self.ftp.retrbinary(f"RETR {file_name}", file_stream.write)
            file_stream.seek(0)
            return file_stream
        except Exception as e:
            log(f"Error downloading {file_name}: {e}")
            raise
        finally:
            self.disconnect()


class FTPResource(ConfigurableResource):
    """
    A Dagster resource that initializes the FTP client.
    """

    host: str
    username: str
    password: str
    timeout: int = 30

    def create_client(self) -> FTPClient:
        """
        Create an FTP client with the configured parameters.
        """
        return FTPClient(self.host, self.username, self.password, self.timeout)
