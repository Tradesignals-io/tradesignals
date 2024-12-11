#!/bin/bash

# This script helps developers build files for any environment.

# Colors for terminal output
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

# Function to read and parse config.yml
parse_config() {
    if [ ! -f "src/pipelines/config.yml" ]; then
        echo -e "${RED}Error: config.yml not found in src/pipelines/${NC}"
        exit 1
    fi

    # Parse YAML and update docker-compose.yml
    python3 -c '
import yaml
import sys

def update_compose():
    """
    Update docker-compose.yml based on pipeline config.

    Reads pipeline configurations from config.yml and updates
    the docker-compose.yml file with new service definitions.
    """
    with open("src/pipelines/config.yml") as f:
        config = yaml.safe_load(f)

    with open("docker-compose.yml") as f:
        compose = yaml.safe_load(f)

    # Update pipeline services
    for name, pipeline in config["pipelines"].items():
        if name not in compose["services"]:
            compose["services"][name] = {
                "build": {
                    "context": f"./src/pipelines/{pipeline['directory']}",
                    "dockerfile": "Dockerfile"
                },
                "networks": ["pipeline-net"]
            }

    with open("docker-compose.yml", "w") as f:
        yaml.dump(compose, f)

update_compose()
'
    echo -e "${GREEN}Successfully updated docker-compose.yml${NC}"
}

# Function to clean and rebuild
clean_rebuild() {
    echo "Cleaning and rebuilding containers..."
    docker-compose down
    docker system prune -f
    docker-compose build --no-cache
    echo -e "${GREEN}Clean rebuild complete${NC}"
}

# Function to start new environment
start_env() {
    echo "Starting new environment..."
    docker-compose up -d
    echo -e "${GREEN}Environment started${NC}"
}

# Function to start new environment
stop_env() {
    echo "Stopping environment..."
    docker-compose down
    echo -e "${RED}Environment stopped${NC}"
}

# Main menu
while true; do
    echo -e "\nDocker Controller Menu"
    echo "1. Update docker-compose from config.yml"
    echo "2. Clean and Rebuild Containers"
    echo "3. Start New Environment"
    echo "4. Exit"

    read -p "Select an option (1-4): " choice

    case $choice in
        1)
            parse_config
            ;;
        2)
            clean_rebuild
            ;;
        3)
            start_env
            ;;
        4)
            echo "Exiting..."
            exit 0
            ;;
        *)
            echo -e "${RED}Invalid option${NC}"
            ;;
    esac
done
