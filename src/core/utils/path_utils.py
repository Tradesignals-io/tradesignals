"""Path utilities for file and directory operations."""

import os

__all__ = [
    'get_current_dir',
    'get_parent_dir_name',
    'get_pyproject_root',
    'find_closest_directory_by_file_name',
    'find_project_root',
]


def get_current_dir() -> str:
    """Retrieve the absolute path of the current working directory.

    Returns:
        str: The absolute path of the current file.

    Example:
        >>> get_current_dir()
        '/path/to/current/file.py'
    """
    return os.path.abspath(__file__)


def get_parent_dir_name() -> str:
    """Retrieve the name of the parent directory.

    Returns:
        str: The name of the parent directory.

    Example:
        >>> get_parent_dir_name()
        'parent_directory_name'
    """
    return os.path.basename(os.path.dirname(get_current_dir()))


def get_pyproject_root() -> str:
    """Retrieve the path to the root directory of the project.

    Root is identified by searching for pyproject.toml in the
    same or parent directories.

    Returns:
        str: The root directory of the project.

    Raises:
        FileNotFoundError: If pyproject.toml is not found in the directory tree.

    Example:
        >>> get_pyproject_root()
        '/path/to/project/root'
    """
    current_path = os.path.dirname(get_current_dir())
    while current_path != os.path.dirname(current_path):
        if 'pyproject.toml' in os.listdir(current_path):
            return current_path
        current_path = os.path.dirname(current_path)
    raise FileNotFoundError("No pyproject.toml file found in the directory tree.")


def find_closest_directory_by_file_name(
    file_name: str,
    start_path: str = None,
    include_children: bool = False,
) -> str:
    """Find the closest directory containing the specified file name.

    Args:
        file_name: (str)
            The name of the file to search for.
        start_path: (str, optional)
            The starting path to begin the search. Defaults to the directory
            of the current file.
        include_children: (bool, optional)
            Whether to include child directories in the search. Defaults to
            False.

    Returns:
        str: The absolute path to the directory containing the file.

    Raises:
        FileNotFoundError: If the file is not found in the directory tree.

    Example:
        >>> find_closest_directory_by_file_name('target_file.txt')
        '/path/to/directory/containing/target_file'
    """
    if start_path is None:
        start_path = os.path.dirname(get_current_dir())

    current_path = start_path
    while current_path != os.path.dirname(current_path):
        if file_name in os.listdir(current_path):
            return current_path
        if include_children:
            for root, _dirs, files in os.walk(current_path):
                if file_name in files:
                    return root
        current_path = os.path.dirname(current_path)
    raise FileNotFoundError(f"No {file_name} file found in the directory tree.")


def find_project_root(start_path: str) -> str:
    """Find the root of the project.

    Args:
        start_path (str): The starting path to begin the search.

    Returns:
        str: The root directory of the project.

    Example:
        >>> find_project_root('/path/to/start')
        '/path/to/project/root'
    """
    return find_closest_directory_by_file_name("pyproject.toml", start_path)
