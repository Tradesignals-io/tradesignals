#!/usr/bin/env bash

# Function to transform input to snake_case
to_snake_case() {
    echo "$1" | tr '[:upper:]' '[:lower:]' | sed -E 's/[^a-z0-9]+/_/g' | sed -E 's/^_+|_+$//g'
}

# Default Python version
default_python_version="3.11.10"

# Default log level
default_log_level="info"

# Function to create a new microservice
create_microservice() {
    # Prompt user for service name
    read -rp "Enter a name for the service: " raw_name

    # Check if raw name is provided
    if [ -z "$raw_name" ]; then
        echo -e "\033[1;31mService name is required.\033[0m"
        exit 1
    fi

    # Prompt user for Python version
    read -rp "Select Python Version (default is $default_python_version): " python_version

    # Use default Python version if not specified
    python_version="${python_version:-$default_python_version}"

    # Transform the raw name to snake_case
    folder_name=$(to_snake_case "$raw_name")

    # Create the new folder inside the parent directory
    echo -e "\033[1;34mCreating service directory '../tradesignals/services/pipelines/$folder_name'...\033[0m"
    mkdir -p "../tradesignals/services/pipelines/$folder_name"

    # Copy all files from the template to the new folder
    echo -e "\033[1;34mCopying template files to '../tradesignals/services/pipelines/$folder_name/'...\033[0m"
    cp -r ./templates/pipeline/* "../tradesignals/services/pipelines/$folder_name/"

    # Update docker-compose.yml to include the new microservice
    docker_compose_file="../docker-compose.yml"
    service_entry="
  $folder_name:
    image: python:$python_version
    container_name: $folder_name
    depends_on:
      - broker
    volumes:
      - ../tradesignals/services/pipelines/$folder_name:/app
    working_dir: /app
    command: python app.py
"

    # Append the new service entry to the docker-compose file
    echo -e "\033[1;34mUpdating Docker Compose file to include the new service...\033[0m"
    echo "$service_entry" >> "$docker_compose_file"

    # Update services.yaml to include the new service
    services_yaml_file="../tradesignals/services/pipelines/services.yaml"
    service_yaml_entry="
  - name: $folder_name
    path: ../tradesignals/services/pipelines/$folder_name/main.py
    log_level: $default_log_level
    python_version: $python_version
"

    # Append the new service entry to the services.yaml file
    echo -e "\033[1;34mUpdating services.yaml to include the new service...\033[0m"
    echo "$service_yaml_entry" >> "$services_yaml_file"

    echo -e "\033[1;32mService '$folder_name' created successfully in '../tradesignals/services/pipelines/'.\033[0m"
    echo -e "\033[1;32mDocker Compose updated to include the new service '$folder_name' with Python version '$python_version'.\033[0m"
    echo -e "\033[1;32mservices.yaml updated to include the new service '$folder_name'.\033[0m"
}

# Function to list available jobs
list_jobs() {
    echo "Available jobs:"
    echo "1) Create a new microservice"
    # Add more jobs here as needed
}

# Function to dispatch a job based on user selection
dispatch_job() {
    list_jobs
    read -rp "Select a job to execute: " job_choice

    case $job_choice in
        1)
            create_microservice
            ;;
        *)
            echo -e "\033[1;31mInvalid choice. Please select a valid job number.\033[0m"
            ;;
    esac
}

# Main script execution
dispatch_job