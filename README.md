# JUDUMAS-FUNCTIONS

# This project houses Azure Functions necessary for the Judumas project.

## Prerequisites

### Tools
- [Azure Functions Core Tools](https://docs.microsoft.com/en-us/azure/azure-functions/functions-run-local?tabs=windows%2Ccsharp%2Cbash)
- [Azure CLI](https://docs.microsoft.com/en-us/cli/azure/install-azure-cli)
- [Azure Storage Emulator](https://docs.microsoft.com/en-us/azure/storage/common/storage-use-emulator)
- [Azure Functions Extension](https://marketplace.visualstudio.com/items?itemName=ms-azuretools.vscode-azurefunctions)
- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/)
- [Python 3](https://www.python.org/downloads)

## Project Structure

The structure of the project is as follows:

```
├── src
│   ├── __init__.py
│   └── azure_api.py
├── .funcignore
├── .gitignore
├── function_app.py
├── getting_started.md
|── host.json
├── local.settings.json
├── README.md
└──  requirements.txt
```

# Setup

## Install necessary packages

```
pip install -r requirements.txt
```

## Running the project

```
func start
```

## Deploying the project

```
func azure functionapp publish judumas --build remote
```

# Azure Functions

## Blob Trigger

The `BlobTrigger` function is triggered when a new blob is uploaded to the specified container. The function reads blob name, create a message with blob name and sends message to the Azure Queue.

- Azure Storage Account - "vilniausduomenys"
- Container - "strava"
