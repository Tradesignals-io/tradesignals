"""
Load schema files in this directory and set them as global constants.
"""
import os


def snake_case(name):
    """Convert a given name to snake_case.

    Args:
        name : str
            The name to convert.

    Returns:
        str
            The name in snake_case format.
    """
    return name.replace('-', '_').replace(' ', '_').lower()


def read_file_content(file_path):
    """Read the content of a file.

    Args:
        file_path : str
            The path to the file.

    Returns:
        str
            The content of the file.
    """
    with open(file_path, 'r') as file:
        return file.read()


# Directory containing the schema files
schema_dir = os.path.dirname(__file__)

# Iterate over each file in the directory
for file_name in os.listdir(schema_dir):
    if file_name.endswith('.avsc'):
        # Generate the constant variable name in snake_case
        constant_name = snake_case(file_name.split('.')[0])
        # Read the file content
        file_content = read_file_content(
            os.path.join(schema_dir, file_name)
        )
        # Set the constant variable
        globals()[constant_name] = file_content
        import json
        print(f"{constant_name} = {json.dumps(json.loads(file_content), indent=4)}")


__all__ = [
    snake_case(file_name.split('.')[0]) for file_name in os.listdir(schema_dir)
    if file_name.endswith('.avsc')
]

