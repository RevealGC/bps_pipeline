"""
CensusHelper:
A helper class to interface with Census URLs, retrieve metadata, parse folder/file structures,
"""

# %%
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup as bs


class CensusHelper:
    """
    A helper class to interface with Census URLs, retrieve metadata, parse folder/file structures,
    and parse BPS header lines.

    Attributes:
        base_url (str): The root URL to fetch metadata from. Trailing slash stripped by default.
        timeout (int): HTTP request timeout in seconds (default: 5).

    Usage:
        helper = CensusHelper("https://www2.census.gov/econ/bps/Place/")
        helper.get_url_metadata()        # fetches folder/file data
        helper.get_subdirectory_metadata(["folderName"], recursion=True)
        # Access data:
        print(helper.subdir_metadata)
        print(helper.folders)
        print(helper.files)
    """

    def __init__(self, url: str, timeout: int = 5):
        """
        Initialize the CensusHelper.

        Args:
            url (str): Base URL for the Census directory.
            timeout (int, optional): HTTP request timeout. Defaults to 5.
        """
        self.base_url = url
        self.timeout = timeout

        self._metadata: list[dict] = []
        self._subdir_metadata: list[dict] = []
        self._folders: list[dict] = []
        self._files: list[dict] = []

    # define a getter for properties subdir_metadata, folders, and files
    @property
    def metadata(self) -> list[dict]:
        """
        Read-only property for metadata (both files and folders) found at the current URL.

        Returns:
            list[dict]: The metadata dictionaries stored from the last call to get_url_metadata.
        """
        return self._metadata

    @property
    def subdir_metadata(self) -> list[dict]:
        """
        Read-only property for metadata (both files and folders) found at the current URL.

        Returns:
            list[dict]: The metadata dictionaries stored from the last call to get_url_metadata.
        """
        return self._metadata

    @property
    def folders(self) -> list[dict]:
        """
        Read-only property for folder entries (size == "").

        Returns:
            list[dict]: Folders in the current directory.
        """
        return self._folders

    @property
    def files(self) -> list[dict]:
        """
        Read-only property for file entries (size != "").

        Returns:
            list[dict]: Files in the current directory.
        """
        return self._files

    def get_url_metadata(self, rows_to_skip: int = 3) -> None:
        """
        Retrieve metadata for files and folders at self.base_url.

        Sets internal fields _subdir_metadata, _folders, and _files accordingly.

        Args:
            rows_to_skip (int, optional): Number of header rows to skip in the HTML table.
                                          Typically 3 for Census. Defaults to 3.
        """
        response = requests.get(self.base_url, timeout=self.timeout)
        response.raise_for_status()

        soup = bs(response.text, "html.parser")
        rows = soup.find_all("tr")[rows_to_skip:]
        self._parse_metadata(rows, self.base_url)

    def _parse_metadata(self, html_results, url) -> None:
        """
        Internal method to parse an HTML table of metadata rows into self.subdir_metadata.
        then Separates subdir_metadata into self.folders (size = "") vs. self.files (size != "").

        Args:
            rows (list): List of <tr> elements from BeautifulSoup.
            base_url (str): The base URL for constructing absolute file URLs.
        """
        parsed = []
        for row in html_results:
            cols = row.find_all("td")
            if len(cols) == 5:
                parsed.append(
                    {
                        "type": cols[0].find("img")["alt"].strip("[]"),
                        "filename": cols[1].text.strip(),
                        "file_url": urljoin(url, cols[1].text.strip()),
                        "last_modified": cols[2].text.strip(),
                        "size": cols[3].text.strip(),
                    }
                )

        self._metadata = parsed
        self._folders = [item for item in parsed if item["size"] == "-"]
        self._files = [item for item in parsed if item["size"] != "-"]

    def get_subdirectory_metadata(
        self,
        folder_names: list[str] = None,
        recursion: bool = False,
        flatten: bool = False,
    ):
        """
        For each folder in `folder_names`, retrieve its subdirectory metadata
        and attach it to folder["subdir_metadata"].

        If `folder_names` is not provided, all known folder names are used.
        If recursion is True, continue fetching sub-folders at deeper levels.

        Args:
            folder_names (list[str], optional): Folder names (matching 'filename' in self.folders).
                                                Defaults to all folder filenames.
            recursion (bool, optional): Whether to recursively descend into subfolders.
        """
        available_folder_names = [f["filename"] for f in self.folders]
        # If no folders specified, take them all. otherwise, check against available folder names to make sure they exist
        if folder_names is None:
            folder_names = [f["filename"] for f in self._folders]

        # Confirm all requested folders exist
        available_folder_names = [f["filename"] for f in self._folders]
        not_found = [fn for fn in folder_names if fn not in available_folder_names]
        if not_found:
            raise ValueError(f"Folder(s) not found in self.folders: {not_found}")

        # For each requested folder, retrieve subdir metadata
        for fn in folder_names:
            folder_dict = next(f for f in self._folders if f["filename"] == fn)
            folder_url = folder_dict["file_url"]

            sub_helper = CensusHelper(folder_url, timeout=self.timeout)
            sub_helper.get_url_metadata()

            folder_dict["sub_helper"] = sub_helper

            # For convenience, also store the direct lists
            folder_dict["subfolders"] = sub_helper.folders
            folder_dict["subfiles"] = sub_helper.files

            if recursion and sub_helper.folders:
                deeper_names = [f["filename"] for f in sub_helper.folders]
                sub_helper.get_subdirectory_metadata(deeper_names, recursion=True)
        if flatten:
            self._flatten_subdirectories()

    def _flatten_subdirectories(self) -> tuple[list[dict], list[dict]]:
        """
        Flatten the recursive folder structure, returning all folders and files
        in a single, flattened view.

        Args:
            folder_dicts (list[dict], optional):
                If provided, start from these folder dictionaries.
                Otherwise, use self._folders (the top-level folders).

        Returns:
            (list[dict], list[dict]):
                A tuple of:
                - all_folders: every folder dictionary found in the entire hierarchy
                - all_files:   every file dictionary found in each folder in the entire hierarchy

        Example:
            helper.get_url_metadata()                      # fetch top-level
            helper.get_subdirectory_metadata(recursion=True)
            all_folders, all_files = helper.flatten_subdirectories()
            # Now you have a flattened list of every folder and file in the tree.
        """
        folder_dicts = self._folders  # top-level

        all_folders = []
        all_files = []

        # Use a BFS (or DFS) queue to traverse all subfolders
        queue = folder_dicts[:]
        while queue:
            folder = queue.pop(0)
            all_folders.append(folder)

            # Collect files from this folder
            subfiles = folder.get("subfiles", [])
            all_files.extend(subfiles)

            # Enqueue subfolders for further traversal
            subfolders = folder.get("subfolders", [])
            queue.extend(subfolders)

        self._folders = all_folders
        self._files = all_files


def parse_filename(
    filename: str, pattern_mappings: list[dict], file_ending: str = ".txt"
) -> dict:
    """
    Parse a filename according to a custom pattern.

    Args:
        filename (str): The file name to parse (e.g., "SO2006A.TXT").
        pattern_mappings (list[dict]): A list of rules describing how to split
                                        and convert each part into a field.
            Example:
                [
                {"start": 0, "end": 2, "field": "region_code"},
                {"start": 2, "end": -1, "field": "date_str"},
                {"start": -1, "end": None, "field": "suffix"},
                ]
            This means:
                region_code = chars [0:2]
                date_str    = chars [2:-1]
                suffix      = chars [-1:]

        file_ending (str, optional): The file extension to remove. Defaults to ".TXT".

    Returns:
        dict: Dictionary of parsed fields based on your pattern_mappings.
    """
    name = filename
    if file_ending in filename:
        name = filename.replace(file_ending, "")

    parsed_result = {}
    for rule in pattern_mappings:
        start = rule["start"]
        end = rule["end"]
        field_name = rule["field"]
        part = name[start:end] if end else name[start:]  # e.g. name[-1:] for suffix
        parsed_result[field_name] = part

    return parsed_result


def parse_filenames(
    filenames: list[str],
    pattern_mappings: list[dict],
    file_ending: str = ".TXT",
):
    """
    Parse multiple filenames with the same custom mapping.

    Args:
        filenames (list[str]): The file names to parse.
        pattern_mappings (list[dict]): Same structure as parse_filename.
        file_ending (str, optional): The file extension to remove (default ".TXT").

    Returns:
        list[dict]: Each dict is the parse result for a single filename.
    """
    return [parse_filename(fn, pattern_mappings, file_ending) for fn in filenames]


def get_bps_header(path_to_file: str, var_column_count: int, timeout=5) -> list[str]:
    """
    Utility to extract the first two lines from a BPS survey file at `path_to_file`
    and build column names.

    Example usage:
        raw_bps_survey = pd.read_csv(url, encoding="utf-8", index_col=False, skiprows=3, header=None, dtype=str)
        total_cols = raw_bps_survey.shape[1]
        id_cols = helper.get_bps_header(url, total_cols)
        new_col_names = id_cols.copy()

    Args:
        path_to_file (str): The file URL or path.
        var_column_count (int): The total number of columns (var_column_names).

    Returns:
        list[str]: Column names from the first two header lines (minus the last 12 columns).
    """
    response = requests.get(path_to_file, timeout=timeout)
    response.raise_for_status()

    lines = response.text.splitlines()
    if len(lines) < 2:
        raise ValueError(f"Expected at least 2 lines in header, got {len(lines)}")

    header_line_1 = lines[0].strip().split(",")
    header_line_2 = lines[1].strip().split(",")

    # everything up to "last 12 columns"
    cutoff = var_column_count - 12
    new_col_names = []
    for i in range(cutoff):
        top_level = header_line_1[i].strip() if i < len(header_line_1) else ""
        sub_level = header_line_2[i].strip() if i < len(header_line_2) else ""
        combined = (
            f"{top_level}_{sub_level}".strip("_").replace(" ", "_").replace("-", "_")
        )
        new_col_names.append(combined)

    return new_col_names


# %%
# dataframes = {}
# for item in helper.folders:
#     sub_df = pd.DataFrame(item['subfiles'])
#     region_name = item["filename"].strip("/")
#     dataframes[region_name] = sub_df
# df = pd.concat(dataframes)
# df
