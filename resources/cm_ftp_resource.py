"""
cm_ftp_resource.py
"""

# %%
from io import BytesIO
from typing import Optional, List
from ftplib import FTP_TLS, error_perm
from dagster import ConfigurableResource, InitResourceContext
from utilities.dagster_utils import log


class FTPResource(ConfigurableResource):
    """
    A Dagster resource that provides FTP connection management for retrieving and storing files.
    The connection is closed after each operation.
    """

    host: str
    username: str
    password: str

    def _connect(self, context: Optional[InitResourceContext] = None) -> FTP_TLS:
        """Establish and return a fresh FTP_TLS connection."""
        log(f"Connecting to FTP server {self.host}...", context)
        ftp = FTP_TLS()
        try:
            ftp.connect(self.host)
            ftp.auth()
            ftp.prot_p()
            ftp.login(self.username, self.password)
            log("FTP connection established.", context)
            return ftp
        except Exception as e:
            raise RuntimeError(
                f"Failed to connect to FTP server {self.host}: {e}"
            ) from e

    def list_files(self, remote_dir: str = ".") -> List[str]:
        """
        Retrieve all filenames from the specified FTP directory.
        """
        with self._connect() as ftp:
            try:
                return ftp.nlst(remote_dir)
            except error_perm as e:
                raise RuntimeError(
                    f"Failed to list files in directory '{remote_dir}': {e}"
                ) from e

    def download_file(self, file_name: str) -> BytesIO:
        """
        Retrieve a file from the FTP server and return its content as a BytesIO stream.

        Args:
            file_name (str): The name of the file to fetch.

        Returns:
            BytesIO: A stream containing the file's binary data.
        """
        remote_path = f"{file_name}"
        file_stream = BytesIO()

        with self._connect() as ftp:
            try:
                ftp.retrbinary(f"RETR {remote_path}", file_stream.write)
                file_stream.seek(0)
                log(f"Downloaded file from FTP: {remote_path}", None)
                return file_stream
            except error_perm as e:
                raise RuntimeError(f"Error retrieving file '{remote_path}': {e}") from e
