import logging
import azure.functions as func
from azure.storage.blob import BlobServiceClient
from azure.cosmos import CosmosClient

# Set connection strings    
BLOB_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=mediahubstorageaccount;AccountKey=iJ7V3199fwTKHcwaQPzqHRoKHtBsbi1Os7vKK5SDJNSJgo7Ekfe0qoPioos2/+prp3XdIRuH6Suz+AStc219HQ==;EndpointSuffix=core.windows.net"
COSMOS_ENDPOINT = "https://mediahub-metadata.documents.azure.com:443/"
COSMOS_KEY = "A10VDoAG3zKa45pXUQytHBjRCNsWIxm8klOHfOPyOlR6BearC1UNPjqvWqtp5OYAvc7Kl5gWIqXVACDb16RIYQ=="

def uploadMedia(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Processing media upload.')

    # Get file and metadata from request
    file = req.files['file']
    metadata = req.get_json()

    # Upload to Blob Storage
    blob_service = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
    blob_client = blob_service.get_blob_client(container="media-files", blob=file.filename)
    blob_client.upload_blob(file.stream)

    # Store metadata in Cosmos DB
    cosmos_client = CosmosClient(COSMOS_ENDPOINT, COSMOS_KEY)
    database = cosmos_client.get_database_client("MediaMetadata")
    container = database.get_container_client("media-metadata")
    metadata["url"] = blob_client.url
    container.upsert_item(metadata)

    return func.HttpResponse(f"Media uploaded successfully: {blob_client.url}", status_code=201)

def getMedia(req: func.HttpRequest) -> func.HttpResponse:
    media_id = req.route_params.get('id')

    cosmos_client = CosmosClient(COSMOS_ENDPOINT, COSMOS_KEY)
    database = cosmos_client.get_database_client("MediaMetadata")
    container = database.get_container_client("media-metadata")

    item = container.read_item(media_id, partition_key=media_id)

    return func.HttpResponse(body=item, status_code=200)

def putMedia(req: func.HttpRequest) -> func.HttpResponse:
    media_id = req.route_params.get('id')
    updated_metadata = req.get_json()

    cosmos_client = CosmosClient(COSMOS_ENDPOINT, COSMOS_KEY)
    database = cosmos_client.get_database_client("MediaMetadata")
    container = database.get_container_client("media-metadata")

    item = container.read_item(media_id, partition_key=media_id)
    item.update(updated_metadata)
    container.upsert_item(item)

    return func.HttpResponse("Metadata updated successfully.", status_code=200)

def deleteMedia(req: func.HttpRequest) -> func.HttpResponse:
    media_id = req.route_params.get('id')

    # Delete from Blob Storage
    blob_service = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
    blob_client = blob_service.get_blob_client(container="media-files", blob=media_id)
    blob_client.delete_blob()

    # Delete metadata from Cosmos DB
    cosmos_client = CosmosClient(COSMOS_ENDPOINT, COSMOS_KEY)
    database = cosmos_client.get_database_client("MediaMetadata")
    container = database.get_container_client("media-metadata")
    container.delete_item(media_id, partition_key=media_id)

    return func.HttpResponse("Media deleted successfully.", status_code=200)