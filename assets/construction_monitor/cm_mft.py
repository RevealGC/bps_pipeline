''' This script transfers files to census via mft '''
import os
import traceback
from dotenv import load_dotenv
from collections import OrderedDict
from datetime import datetime, timedelta

load_dotenv()

# SMTP server configuration
url = os.getenv( "mft_server_cm_path")
user_token = os.getenv( "mft_token")
input_file_path = os.getenv( "local_path")

def transfer_cmd():
    print(f"Start Transfer File to Server: {input_file_path}")
    main_path = os.getcwd()
    try:
        token = user_token
        file_list = []
        file_name = []

        def list_files_recursively(directory):
            # Ensure the directory exists
            if not os.path.exists(directory):
                print(f"The directory {directory} does not exist.")
                return
            for root, dirs, files in os.walk(directory):
                for file in files:
                    if file.endswith('.csv'):
                        file_list.append(os.path.join(root, file))
                        file_name.append(file)

        # List files in the input directory
        list_files_recursively(input_file_path)

        # Log and transfer files not present in the local directory
        for value in file_list:
            dir_name, file_nm = os.path.split(value)
            current_date = datetime.now().strftime('%Y_%m')
            list = os.listdir(dir_name).__contains__(file_nm)
            if ('pattern' not in file_nm and file_nm.__contains__(current_date)):
                print('.............', file_nm, list)
                mft_str = f'curl -X POST "{url}{file_nm}?packet=1&position=0&final=true" --header "Authorization: Basic {token}" -T "{value}" -v'
                print('Transferring:', mft_str)
                send_msg = os.system(mft_str)
                print("\nTransfer File to Server COMPLETE")
            else:
                pass
            
    except Exception as e:
        os.chdir(main_path)
        print(e)
        print(traceback.format_exc())

    os.chdir(main_path)

transfer_cmd()