import json

# TODO(developer)
project_id = "buses-419920"
topic_id = "my-topic"

publisher = pubsub_v1.PublisherClient()
# The `topic_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/topics/{topic_id}`
topic_path = publisher.topic_path(project_id, topic_id)

with open("bcsample.json") as f:
  data = json.load(f)
  for i in data:
    print(i)
    encoded = json.dumps(i).encode("utf-8")
    # When you publish a message, the client returns a future.
    future = publisher.publish(topic_path, encoded)
    print(future.result())
