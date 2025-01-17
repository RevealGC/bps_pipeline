from io import BytesIO
from dagster import ConfigurableResource, InitResourceContext
from ftplib import FTP_TLS
from pydantic import PrivateAttr


class FTPResource(ConfigurableResource):
    """
    A Dagster resource that creates and returns an authenticated FTP_TLS connection
    and also provides the local directory path for file downloads.
    """

    host: str
    username: str
    password: str

    _ftp: FTP_TLS = PrivateAttr()

    def setup_for_execution(self, context: InitResourceContext) -> None:
        """Prepare a connection to the FTP resource"""
        context.log.info(f"Connecting to FTP server: {self.host}")

        # Establish FTP connection
        self._ftp = FTP_TLS()

        self._ftp.connect(self.host)
        self._ftp.auth()
        self._ftp.prot_p()
        self._ftp.login(self.username, self.password)

        context.log.info("FTP connection successful.")

    def fetch_all_filenames(self, remote_dir: str = ".") -> list:
        """Retrieve all filenames from the FTP server"""
        return self._ftp.nlst(remote_dir)

    def fetch_file(self, file_name: str) -> BytesIO:
        """
        Retrieve a file from the FTP server and return its content as a BytesIO stream.

        Args:
            file_name (str): The name of the file to fetch.

        Returns:
            BytesIO: A stream containing the file's binary data.
        """
        file_stream = BytesIO()
        self._ftp.retrbinary(f"RETR {file_name}", file_stream.write)
        file_stream.seek(0)  # Reset the stream position for reading

        return file_stream
