# This file is very similar to sub.py,
# except rather than displaying acknowledged messages,
# we are just clearing the topic.
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1

# TODO(developer)
project_id = "buses-419920"
subscription_id = "my-sub"

subscriber = pubsub_v1.SubscriberClient()
# The `subscription_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/subscriptions/{subscription_id}`
subscription_path = subscriber.subscription_path(project_id, subscription_id)

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
  message.ack()

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)

# Wrap subscriber in a 'with' block to automatically call close() when done.
with subscriber:
  try:
    # When `timeout` is not set, result() will block indefinitely,
    # unless an exception is encountered first.
    streaming_pull_future.result()
  except TimeoutError:
    streaming_pull_future.cancel()  # Trigger the shutdown.
    streaming_pull_future.result()  # Block until the shutdown is complete.
