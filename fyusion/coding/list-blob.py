from google.cloud import storage


def list_blobs(bucket_name):
    """Lists all the blobs in the bucket."""
    bucket_name = "prod-fyusion-dataset-us-ws2-bucket"

    storage_client = storage.Client()

    # Note: Client.list_blobs requires at least package version 1.17.0.
    blobs = storage_client.list_blobs(bucket_name)

    for blob in blobs:
        print(blob.name)

if __name__ == "__main__":
    list_blobs(bucket_name="prod-fyusion-dataset-us-ws2-bucket")