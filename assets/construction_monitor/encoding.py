''' This script will encode a string using base64 encoding '''

import base64
import getpass

def encode_to_base64(username, password):
    # Combine username and password with a colon
    credentials = f"{username}:{password}"
    # Encode the combined string to Base64
    encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
    return encoded_credentials

if __name__ == "__main__":
    # Prompt user for username
    username = input("Enter username: ")
    # Prompt user for password with input masking
    password = getpass.getpass("Enter password: ")
    
    # Encode credentials
    encoded = encode_to_base64(username, password)
    
    # Print the encoded string
    print("\nEncoded credentials:")
    print(encoded)