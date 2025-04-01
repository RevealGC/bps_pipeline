MFTClient Usage Guide
==============

This document explains how to use the `MFTClient` class to send files to a Managed File Transfer (MFT) server via cURL.

### 1. Overview
`MFTClient` provides a simple interface for uploading files to a remote MFT server. It supports two authentication methods:
- Token-based authentication (by providing a pre-generated token)
- Basic authentication (by providing a username and password)

Only one method can be used at a time.

### 2. Installation and Setup
No external installation is required beyond Python itself and the built-in `subprocess`, `base64`, and `re` modules. `MFTCLient` can be used directly, but can be easily automated using the `MFTResource` extention with dagster.

### 3. Instantiating the Client
#### 3.1 Basic Authentication
``` python
from census_mft_resource import MFTClient

client = MFTClient(
    url="https://mft.example.com/transfer/",  # Base URL of MFT server
    username="my_username",
    password="my_password"
)
```

#### 3.2 Token-based Authentication

``` python
from census_mft_resource import MFTClient

client = MFTClient(
    url="https://mft.example.com/transfer/",
    token="my_encoded_token"
)
```

### 4. Sending Files
To upload a file, call the `send_file` method with:
- `target_file`: A `Path` or filepath string to the local file you want to upload
- `dest_name`: The desired filename on the MFT server
- `dest_folder` (optional): The destination folder/path on the MFT server

``` python
from pathlib import Path

result = client.send_file(
    target_file=Path("local_folder/test_file.csv"),
    dest_name="test_file_remote.csv",
    dest_folder="destination_folder"
)

# Inspect the result
print("STDOUT:", result["stdout"])
print("STDERR:", result["stderr"])
print("Return Code:", result["returncode"])
print("Error:", result["error"])
```
### 5. Error Handling
You may encounter one of the following errors:
- **FileNotFoundError**: Raised if the file you specify does not exist on the local file system.
- **ValueError**: Raised if neither credentials nor a token are provided, or if both are provided simultaneously.
- **subprocess.CalledProcessError**: Wrapped into the `result` dictionary, typically indicating the cURL command failed (e.g., network issues or server-side errors).

# Adding MFT to your dagster pipeline
An additional `MFTResource` is included for integration with Dagster, allowing you to define and provide an `MFTClient` within a Dagster pipeline/resource context.

## Add to MFT resource to your pipeline entrypoint
Add the resource to your definitions object. I recommend adding the class definition to the resources subfolder

```python
import dagster as dg
from census_mft_resource import MFTResource
...

defs = dg.Definitions(
    assets= ...
    sensors=...
    resources={
        ...
        
        "census_mft_resource": MFTResource(
            url=EnvVar("MFT_BASE_URL"),
            username=EnvVar("MFT_USERNAME"),
            password=EnvVar("MFT_PASSWORD"),
        ),
    },
)
```
include values for `MFT_BASE_URL`, `MFT_USERNAME`, and `MFT_PASSWORD` in your ,env file.

**NOTE:** handling of environment variables may be different in production environments.


## Adding MFT push to an Asset or Op

### Push to MFT and capture MFT response
this line of code is added as an opp or as part of an asset.

```python
    # initialize an instance of the MFT resource 
    mft_client = mft_resource.get_client()

    # use the "send_file" method of the MFT resource instance to
    # attempt to send te file. stdout and stderr are captured in results:

    results = mft_client.send_file(
        target_file=file_path,
        dest_name=rename_cm_weekly_file(context.partition_key),
        dest_folder="rgc_rawdata_cm",
    )

```
### Publish the MFT response in Dagster UI
Reformat the stdout and stderr as json for publishing to results. Then
at the end of the asset definition, define a dagster.Output object and pass 
the json formatted stderr and stdout into metadata for logging
```python
    ...
    
    results_md = f"```json\n{json.dumps(results, indent=2)}\n```"
    
    ...

    return dg.Output(
        ...
        metadata={
            ...
            "mft_result": dg.MetadataValue.md(results_md),
        },
    )
```

