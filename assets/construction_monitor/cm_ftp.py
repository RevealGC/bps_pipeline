''' This script downloads files from CM Server '''

from dotenv import load_dotenv
from ftplib import FTP_TLS
import os

load_dotenv()

def download_files_from_ftp():
    """
    Connects to an FTP server using the provided settings and downloads files to the path on Blackbox.
    
    Args:
        settings (dict): A dictionary with FTP settings (host, username, password, directory).
    """
    ftp_host = os.getenv("sftp_host")
    ftp_user = os.getenv("sftp_user")
    ftp_pass = os.getenv("sftp_pass")
    ftp_lcl = os.getenv("original_path")  

    # Check if the path is set and valid
    if ftp_lcl and os.path.isdir(ftp_lcl):
        # List all files in the local directory
        lcl_files = os.listdir(ftp_lcl)

    # Ensure required keys are present
    if not ftp_host or not ftp_user or not ftp_pass:
        raise ValueError("Missing required FTP settings: host, username, or password.")

    # Connect to the FTP server
    ftp = FTP_TLS()
    print(f"Connecting to FTP server: {ftp_host}")
    ftp.connect(ftp_host)
    ftp.auth()
    ftp.prot_p()
    ftp.login(ftp_user, ftp_pass)
    print("Connected and logged in successfully.")

    # List files on ftp server and download files which are not present on local
    local_tmp_dir = ftp_lcl
    os.makedirs(local_tmp_dir, exist_ok=True)

    files = ftp.nlst() 
    for file_name in files:
        if file_name in lcl_files:
            pass
        else:
            local_file_path = os.path.join(local_tmp_dir, file_name)
            with open(local_file_path, "wb") as local_file:
                print(f"Downloading: {file_name}")
                ftp.retrbinary(f"RETR {file_name}", local_file.write)
            print(f"Downloaded {file_name} to {local_file_path}")
    # Close FTP connection
    ftp.quit()
    
download_files_from_ftp()
