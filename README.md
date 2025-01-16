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
pip install -r requirements.txt
```
3. Run Dagster as a Module:
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
 - data/: by default all files will be downloaded into subfolders within this repo. 
