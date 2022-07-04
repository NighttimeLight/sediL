from google.cloud import storage
import traceback
import xmltodict
import json
from google.cloud import pubsub_v1

publisher = pubsub_v1.PublisherClient()
PROJECT_ID = 'bigdata0'

def read_xml_data():
#   request_json = request.get_json()

  project_path = f"projects/{PROJECT_ID}"
  topic_name = 'xml_data_json_publish'
  topic_path = publisher.topic_path(PROJECT_ID, topic_name)
  topic_exists = False
  for topic in publisher.list_topics(request={"project": project_path}):
    if topic.name == f"{project_path}/topics/{topic_name}":
        topic_exists = True
  if not topic_exists:
    topic = publisher.create_topic(request={"name": topic_path})

  storage_client = storage.Client()
  bucket = storage_client.bucket('gcp_project_bd')

  blobs = bucket.list_blobs()

  for blob in blobs:
    try:
        num = blob.name.count('/')
        file_name = blob.name.split('/')[num]
        if file_name == "dni_wolne.xml" and blob.name.split('/')[num - 1] == 'external_data':
            gcs_file = blob.download_as_string()
            data = xmltodict.parse(gcs_file)
            print(json.dumps(data, indent=2))
            message_json = json.dumps({
                'data': {'message': data},
            })
            message_bytes = message_json.encode('unicode_escape')
            publish_future = publisher.publish(topic_path, data=message_bytes)
            publish_future.result()
    except:
        print("An exception occurred")
        traceback.print_exc()

read_xml_data()