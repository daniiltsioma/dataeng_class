from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1, storage
import json

# TODO(developer)
project_id = "buses-419920"
subscription_id = "archive-sub"
# Number of seconds the subscriber should listen for messages
timeout = 30.0

subscriber = pubsub_v1.SubscriberClient()
# The `subscription_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/subscriptions/{subscription_id}`
subscription_path = subscriber.subscription_path(project_id, subscription_id)


message_list = []
def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    response_message = json.loads(message.data.decode('utf-8'))
    message_list.append(response_message)
    print(f"Received message.")
    message.ack()


streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback )
print(f"Listening for messages on {subscription_path}..\n")

def upload_blob_from_memory(bucket_name, contents, destination_blob_name):
    """Uploads a file to the bucket."""

    # The ID of your GCS bucket
    # bucket_name = "buses-bucket"

    # The contents to upload to the file
    # contents = "these are my contents"

    # The ID of your GCS object
    # destination_blob_name = "storage-object-name"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_string(contents)

    print(
        f"{destination_blob_name} with contents {contents} uploaded to {bucket_name}."
    )


# Wrap subscriber in a 'with' block to automatically call close() when done.
with subscriber:
    try:
        # When `timeout` is not set, result() will block indefinitely,
        # unless an exception is encountered first.
        streaming_pull_future.result(timeout=timeout)
    except TimeoutError:
        streaming_pull_future.cancel()  # Trigger the shutdown.
        streaming_pull_future.result()  # Block until the shutdown is complete.
    finally:
        upload_blob_from_memory("buses_bucket", message_list, "buses-storage");
