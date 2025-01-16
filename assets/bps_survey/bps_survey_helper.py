"""docstring"""

from datetime import datetime
from urllib.parse import urljoin

import requests
import pandas as pd
from bs4 import BeautifulSoup


def parse_census_filename(filename):
    """
    Parse a Census BPS filename (e.g. 'SO0712C.TXT' or 'SO2006A.TXT') into structured metadata.

    1) Remove extension and ensure uppercase.
    2) Extract region (first 2 chars), suffix (last char), and the date part in between.
    3) Determine whether it's monthly or annual based on suffix and/or year.
    4) Return a dictionary of parsed measures.
    """
    name = filename.upper().replace(".TXT", "")

    region_code = name[:2]  # e.g. "SO"
    suffix = name[-1]  # e.g. "C", "Y", "R", "A"
    date_str = name[2:-1]  # e.g. "0712" or "2006"

    if not date_str or not suffix:
        raise ValueError(f"Filename '{filename}' does not follow expected patterns.")

    # Determine data_type from suffix, except R can be monthly or annual
    if suffix in ["C", "Y"]:
        data_type = "monthly"
    elif suffix == "A":
        data_type = "annual"
    elif suffix == "R":
        data_type = "revised"
    else:
        raise ValueError(f"Unexpected suffix '{suffix}' in '{filename}'.")

    # Parse year/month depending on data_type
    if data_type == "annual":
        # Annual: <Region><YYYY><Suffix>
        year = int(date_str)  # e.g. "2006"
        month = None
    else:
        # Monthly: <Region><YYMM><Suffix>
        yy = int(date_str[:2])  # e.g. "07" -> 7
        mm = int(date_str[2:])  # e.g. "12" -> 12

        if 2000 + yy > datetime.now().year:  # If adding 2000 makes year invalid
            year = 1900 + yy  # Use 1900s instead
        else:
            year = 2000 + yy  # Otherwise, it's 2000s
        month = mm

    # Provide a human-readable meaning for the suffix
    if data_type == "monthly":
        if suffix == "C":
            suffix_meaning = "current month"
        elif suffix == "Y":
            suffix_meaning = "year-to-date monthly"
        else:
            raise ValueError(f"Unexpected suffix '{suffix}' in '{filename}'.")
    else:
        # Annual
        if suffix == "A":
            suffix_meaning = "annual summary"
        else:
            raise ValueError(f"Unexpected suffix '{suffix}' in '{filename}'.")

    return {
        # "filename": filename,
        "region_code": region_code,
        "data_type": data_type,  # 'monthly' or 'annual'
        "year": year,
        "month": month,  # None if annual
        "suffix": suffix,  # 'C', 'Y', 'R', or 'A'
        "suffix_meaning": suffix_meaning,
    }


def get_bps_header(path_to_file, var_column_names):
    """
    utility to extract the header data from a BPS survey file.
    Headers are split over two rows and shift over time. this utility
    will extract the first two rows and combine them into a list of column names.

    # Example usage
    url = "https://www2.census.gov/econ/bps/Place/South%20Region/so2401c.txt"
    df = pd.read_csv(url, skiprows=3, header=None, dtype=str)
    total_cols = df.shape[1]
    new_column_names = get_bps_header(url, total_cols)
    """
    # Fetch the file content from the URL
    response = requests.get(path_to_file, timeout=5)
    response.raise_for_status()  # Raise an exception for HTTP errors

    # Split the content into lines
    lines = response.text.splitlines()

    # Extract the first two header lines
    header_line_1 = lines[0].strip().split(",")
    header_line_2 = lines[1].strip().split(",")

    new_col_names = []
    cutoff = var_column_names - 12  # everything up to this index = drifting columns

    for i in range(cutoff):
        # Safely grab top-level and sub-level header labels (or use empty if index out of range)
        top_level = header_line_1[i].strip() if i < len(header_line_1) else ""
        sub_level = header_line_2[i].strip() if i < len(header_line_2) else ""
        # Combine them; adjust to your preferred way of combining
        combined = f"{top_level}_{sub_level}".strip("_")
        # Replace spaces or special chars for cleanliness
        combined = combined.replace(" ", "_").replace("-", "_")
        new_col_names.append(combined)

    # 2) For the last 12 columns, use the known consistent names
    return new_col_names


def census_files_metadata(regions) -> pd.DataFrame:
    """
    scrape Census BPS survey pages for a list of file URLs and parse each file's metadata.
    Return a pandas DataFrame.
    """

    all_txt_file_urls = []
    seen_filenames = set()  # To track filenames we've already added

    for region in regions:
        url = f"https://www2.census.gov/econ/bps/Place/{region}%20Region/"
        response = requests.get(url, timeout=5)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, "html.parser")

        files_data = []

        # Find all rows in the table (often the listing is in <tr> elements)
        rows = soup.find_all("tr")
        for row in rows:
            # Extract <td> cells
            tds = row.find_all("td")
            if len(tds) < 4:
                # Not a valid file row (maybe the header row or an empty row)
                continue

            # The <a> link is usually in the second cell
            link = tds[1].find("a")
            if not link:
                continue

            href = link.get("href", "")
            if not href.endswith(".txt"):
                # Skip rows that aren't .txt files
                continue

            # Build the absolute URL for the file
            file_url = urljoin(url, href)

            # Last modified is often in the 3rd cell, size in the 4th cell
            last_modified = tds[2].get_text(strip=True)
            size = tds[3].get_text(strip=True)

            filename = link.text.strip()

            if filename in seen_filenames:
                continue

            seen_filenames.add(filename)

            # Gather data into a dict
            files_data.append(
                {
                    "filename": filename,
                    "file_url": file_url,
                    "last_modified": last_modified,
                    "size": size,
                }
            )

        all_txt_file_urls.extend(files_data)

    df = pd.DataFrame(all_txt_file_urls)
    parsed_series = df["filename"].apply(parse_census_filename)

    # Convert that series of dicts into a DataFrame
    parsed_df = pd.DataFrame(parsed_series.tolist())

    # Combine original DataFrame with the new columns
    # If you want to replace the original "filename" column, just keep the new one from parsed_df
    # Here we keep both
    combined_df = pd.concat([df, parsed_df], axis=1)

    return pd.DataFrame(combined_df)
