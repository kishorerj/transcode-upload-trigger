from google.cloud import pubsub_v1
import os

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

    projectId = os.environ.get('PROJECT_ID')
    if projectId is None:
         print("ERROR project is null")
    topicId = os.environ.get("TOPIC")
    if topicId is None:
         print("ERROR topic is null")
    template_id = os.environ.get('TRANSCODE_TEMPLATE')
    if template_id is None:
         print("ERROR templateid is null")
    out_bucket = os.environ.get('OUTPUT_BUCKET')
    if out_bucket is None:
         print("ERROR output bucket is null") 
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(projectId, topicId)
    publisher.publish(topic_path, (event['bucket']+","+ event['name']+ ","+ template_id+ ","+ out_bucket).encode("utf-8"))
    return 'OK'
