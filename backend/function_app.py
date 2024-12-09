import logging
import json
import azure.functions as func
from azure.storage.blob import BlobServiceClient
from azure.cosmos import CosmosClient

# Set connection strings  
BLOB_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=mediahubstorageaccount;AccountKey=iJ7V3199fwTKHcwaQPzqHRoKHtBsbi1Os7vKK5SDJNSJgo7Ekfe0qoPioos2/+prp3XdIRuH6Suz+AStc219HQ==;EndpointSuffix=core.windows.net"
COSMOS_ENDPOINT = "https://mediahub-metadata.documents.azure.com:443/"
COSMOS_KEY = "A10VDoAG3zKa45pXUQytHBjRCNsWIxm8klOHfOPyOlR6BearC1UNPjqvWqtp5OYAvc7Kl5gWIqXVACDb16RIYQ=="

# Initialize clients
blob_service = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
cosmos_client = CosmosClient(COSMOS_ENDPOINT, COSMOS_KEY)
database = cosmos_client.get_database_client("MediaMetadata")
container = database.get_container_client("media-metadata")


def upload_media(req: func.HttpRequest) -> func.HttpResponse:
    """Handles uploading media and its metadata."""
    logging.info('Processing media upload.')

    try:
        # Parse request data
        file = req.files.get('file')
        if not file:
            return func.HttpResponse("File is missing in the request.", status_code=400)

        metadata = req.get_json()
        if not metadata:
            return func.HttpResponse("Metadata is missing in the request.", status_code=400)

        # Upload file to Blob Storage
        blob_client = blob_service.get_blob_client(container="media-files", blob=file.filename)
        blob_client.upload_blob(file.stream)

        # Store metadata in Cosmos DB
        metadata["url"] = blob_client.url
        container.upsert_item(metadata)

        response = {
            "message": "Media uploaded successfully.",
            "url": blob_client.url,
            "metadata": metadata
        }
        return func.HttpResponse(json.dumps(response), status_code=201, mimetype="application/json")

    except Exception as e:
        logging.error(f"Error during upload: {str(e)}")
        return func.HttpResponse("Internal server error.", status_code=500)


def get_media(req: func.HttpRequest) -> func.HttpResponse:
    """Handles fetching media metadata by ID."""
    media_id = req.route_params.get('id')

    if not media_id:
        return func.HttpResponse("Media ID is missing in the request.", status_code=400)

    try:
        item = container.read_item(media_id, partition_key=media_id)
        return func.HttpResponse(json.dumps(item), status_code=200, mimetype="application/json")

    except Exception as e:
        logging.error(f"Error fetching media: {str(e)}")
        return func.HttpResponse("Media not found.", status_code=404)


def update_media(req: func.HttpRequest) -> func.HttpResponse:
    """Handles updating metadata for existing media."""
    media_id = req.route_params.get('id')

    if not media_id:
        return func.HttpResponse("Media ID is missing in the request.", status_code=400)

    try:
        updated_metadata = req.get_json()
        if not updated_metadata:
            return func.HttpResponse("Metadata is missing in the request.", status_code=400)

        # Fetch and update item
        item = container.read_item(media_id, partition_key=media_id)
        item.update(updated_metadata)
        container.upsert_item(item)

        return func.HttpResponse("Metadata updated successfully.", status_code=200)

    except Exception as e:
        logging.error(f"Error updating media: {str(e)}")
        return func.HttpResponse("Media not found or update failed.", status_code=404)


def delete_media(req: func.HttpRequest) -> func.HttpResponse:
    """Handles deleting media and its metadata."""
    media_id = req.route_params.get('id')

    if not media_id:
        return func.HttpResponse("Media ID is missing in the request.", status_code=400)

    try:
        # Delete from Blob Storage
        blob_client = blob_service.get_blob_client(container="media-files", blob=media_id)
        blob_client.delete_blob()

        # Delete metadata from Cosmos DB
        container.delete_item(media_id, partition_key=media_id)

        return func.HttpResponse("Media deleted successfully.", status_code=200)

    except Exception as e:
        logging.error(f"Error deleting media: {str(e)}")
        return func.HttpResponse("Media not found or deletion failed.", status_code=404)
