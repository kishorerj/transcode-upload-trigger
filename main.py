from google.cloud import pubsub_v1

def hello_gcs(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    file = event
    print(f"Processing file: {file['name']}.")
    print('Event ID: {}'.format(context.event_id))
    print('Event type: {}'.format(context.event_type))
    print('Bucket: {}'.format(event['bucket']))
    print('File: {}'.format(event['name']))
    print('Metageneration: {}'.format(event['metageneration']))
    print('Created: {}'.format(event['timeCreated']))
    print('Updated: {}'.format(event['updated']))

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path("kishorerjbloom", "transfer-topic")

    publisher.publish(topic_path, (event['bucket']+","+event['name']).encode("utf-8"))
    return 'OK'
