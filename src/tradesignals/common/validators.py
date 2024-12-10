from __future__ import annotations

import os
from enum import Enum
from os import PathLike
from pathlib import Path
from typing import TypeVar

E = TypeVar("E", bound=Enum)


def validate_path(value: PathLike[str] | str, param: str) -> Path:
    """
    Validate whether the given value is a valid path.

    Parameters
    ----------
    value: PathLike or str
        The value to validate.
    param : str
        The name of the parameter being validated (for any error message).

    Returns
    -------
    Path
        A valid path.

    Raises
    ------
    TypeError
        If value is not a valid path.

    """
    try:
        return Path(value)
    except TypeError:
        raise TypeError(
            f"The `{param}` was not a valid path type. " "Use any of [str, bytes, os.PathLike].",
        ) from None


def validate_file_write_path(value: PathLike[str] | str, param: str) -> Path:
    """
    Validate whether the given value is a valid path to a writable file.

    Parameters
    ----------
    value: PathLike or str
        The value to validate.
    param : str
        The name of the parameter being validated (for any error message).

    Returns
    -------
    Path
        A valid path to a writable file.

    Raises
    ------
    IsADirectoryError
        If path is a directory.
    FileExistsError
        If path exists.
    PermissionError
        If path is not writable.

    """
    path_valid = validate_path(value, param)
    if not os.access(path_valid.parent, os.W_OK):
        raise PermissionError(f"The file `{value}` is not writable.")
    if path_valid.is_dir():
        raise IsADirectoryError(f"The `{param}` was not a path to a file.")
    if path_valid.is_file():
        raise FileExistsError(f"The file `{value}` already exists.")
    return path_valid


def validate_enum(
    value: object,
    enum: type[E],
    param: str,
) -> E:
    """
    Validate whether the given value is either the correct Enum type, or a
    valid value of that enum.

    Parameters
    ----------
    value : Enum or str, optional
        The value to validate.
    enum : type[Enum]
        The valid enum type.
    param : str
        The name of the parameter being validated (for any error message).

    Returns
    -------
    Enum
        A valid member of Enum.

    Raises
    ------
    ValueError
        If value is not valid for the given enum.

    """
    try:
        return enum(value)
    except ValueError:
        if hasattr(enum, "variants"):
            valid = list(map(str, enum.variants()))  # type: ignore [attr-defined]
        else:
            valid = list(map(str, enum))

        raise ValueError(
            f"The `{param}` was not a valid value of {enum.__name__}" f", was '{value}'. Use any of {valid}.",
        ) from None


def validate_enum_or_none(
    value: E | str | None,
    enum: type[E],
    param: str,
) -> E | None:
    """
    Validate whether the given value is either the correct Enum type, a valid
    value of that enum, or None.

    Parameters
    ----------
    value : Enum or str, optional
        The value to validate.
    enum : type[Enum]
        The valid enum type.
    param : str
        The name of the parameter being validated (for any error message).

    Returns
    -------
    Enum or None
        A valid member of Enum or None.

    Raises
    ------
    ValueError
        If value is not valid for the given enum.

    """
    if value is None:
        return None
    return validate_enum(value, enum, param)


def validate_str_not_empty(value: str, param: str) -> str:
    """
    Validate whether a string contains a semantic value.

    A string is considered absent of meaning if:
        - It is empty.
        - It contains only whitespace.
        - It contains unprintable characters.

    Parameters
    ----------
    value: str
        The string to validate.
    param : str
        The name of the parameter being validated (for any error message).

    Raises
    ------
    ValueError
        If the string is not meaningful.

    """
    if not value:
        raise ValueError(f"The `{param}` cannot be an empty string.")
    if str.isspace(value):
        raise ValueError(f"The `{param}` cannot contain only whitepsace.")
    if not str.isprintable(value):
        raise ValueError(f"The `{param}` cannot contain unprintable characters.")
    return value
