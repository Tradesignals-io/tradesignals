"""
Path utilities.
"""

import os


def get_abs_path() -> str:
    """
    Get the absolute path of the current file.

    Returns
    -------
    str
        The absolute path of the current file.
    """
    return os.path.abspath(__file__)


def get_parent_dir_name() -> str:
    """
    Get the name of the parent directory of the current file.

    Returns
    -------
    str
        The name of the parent directory.
    """
    return os.path.basename(os.path.dirname(get_abs_path()))


def get_pyproject_root() -> str:
    """
    Get the root directory of the project by moving two levels up from the current file.

    Returns
    -------
    str
        The root directory of the project.
    """
    return os.path.dirname(os.path.dirname(get_abs_path()))


def find_closest_directory_by_file_name(file_name: str, start_path: str = None, include_children: bool = False) -> str:
    """
    Find the closest directory containing the specified file name, starting from the given path
    and optionally including child directories.

    Parameters
    ----------
    file_name : str
        The name of the file to search for.
    start_path : str, optional
        The starting path to begin the search. Defaults to the directory of the current file.
    include_children : bool, optional
        Whether to include child directories in the search. Defaults to False.

    Returns
    -------
    str
        The absolute path to the directory containing the file.

    Raises
    ------
    FileNotFoundError
        If the file is not found in the directory tree.
    """
    if start_path is None:
        start_path = os.path.dirname(get_abs_path())

    current_path = start_path
    while current_path != os.path.dirname(current_path):
        if file_name in os.listdir(current_path):
            return current_path
        if include_children:
            for root, dirs, files in os.walk(current_path):
                if file_name in files:
                    return root
        current_path = os.path.dirname(current_path)
    raise FileNotFoundError(f"No {file_name} file found in the directory tree.")


def find_project_root(start_path: str) -> str:
    """
    Find the root of the project by identifying the most immediate
    parent or current directory if a pyproject.toml file exists.

    Parameters
    ----------
    start_path : str
        The starting path to begin the search.

    Returns
    -------
    str
        The root directory of the project.

    Raises
    ------
    FileNotFoundError
        If no pyproject.toml file is found in the directory tree.
    """
    return find_closest_directory_by_file_name("pyproject.toml", start_path)
