# bps_pipeline

This README will help you set up and run the pipeline on their local machine. this guide is primarily intended for developing and testing prior to deployment.

## Overview

This repository contains the BPS pipeline for processing survey data. The pipeline is built using Dagster and includes assets for downloading, processing, and storing survey data.

## Running the Pipeline
To run and test this pipeline on your local machine, follow these steps:

1. Clone the Repository:
```
git clone https://github.com/RevealGC/bps_pipeline.git
cd bps_pipeline
```
2. Install Dependencies:
```
pip install -r pyproject.toml
```
3. set env variables
3. Modify the `default.env` File:
Create or edit the `default.env` file in the root directory of the repository. This file should contain the necessary environment variables required by the pipeline. For example:
```
API_KEY=your_api_key_here
DATABASE_URL=your_database_url_here
```
Replace `username` and `password` with the appropriate values.
for development the provided base_path and permit_data_directories are sufficient but in production, these should point to the output path and archive csv path respectively

```
4. Run Dagster as a Module:
```
dagster dev -m bps_pipeline
```
This command will start the Dagster development server and load the pipeline defined in the bps_pipeline module.
 after which you should be able to access the webserver locally at http://127.0.0.1:3000/

## Directory Structure
bps_pipeline.py: The main pipeline definition file. this is the intended entrypoint into the pipeline
 - assets/: Contains the asset definitions for the pipeline.
 - resources/: Contains resource definitions, such as the Parquet IO manager.
 - sensors/: Contains sensor definitions (if any).
