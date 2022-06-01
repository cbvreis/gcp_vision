# pip install --upgrade google-api-python-client
# pip install google-cloud
# pip install google-cloud-vision
# pip install -t lib google-auth google-auth-httplib2 google-api-python-client --upgrade
# from google.cloud import storage
# from google.cloud import bigquery


from google.oauth2 import service_account
from google.cloud import bigquery
import uuid


def authenticate(file_path):
    '''
    Authenticate with Google Cloud Platform
    :param file_path:
    :return:
    '''
    if file_path:
        credentials = service_account.Credentials.from_service_account_file(file_path)
        return credentials
    else:
        return None


credentials = authenticate('token/token.json')


def detect_text_vision(path):
    '''
    Detects text in the file
    :param path:
    :return:
    '''
    from google.cloud import vision
    import io
    '''
    client_options = {
      "requests": [
        {
          "image": {
            "source": {
              "imageUri": "IMAGE_URL"
            }
          },
          "features": [
            {
              "type": "DOCUMENT_TEXT_DETECTION"
            }
          ],
          "imageContext": {
            "languageHints": ["en-t-i0-handwrit"]
          }
        }
      ]
    }
    '''
    #client = vision.ImageAnnotatorClient(credentials=credentials,client_options=client_options)
    client = vision.ImageAnnotatorClient(credentials=credentials)

    with io.open(path, 'rb') as image_file:
        content = image_file.read()
    image = vision.Image(content=content)
    response = client.text_detection(image=image)
    full_text = response.full_text_annotation.text
    return full_text


def write_big_query(text,event_date,event_file_name):
    '''
    Write to BigQuery
    :param text:
    :param event_date:
    :param event_file_name:
    :return:
    '''
    client = bigquery.Client(credentials=credentials)
    rows_to_insert = [
        {
            "EVENT_TIME": event_date,
            "EVENT_TEXT":text,
            'EVENT_FILE': event_file_name,
            'EVENT_ID': str(uuid.uuid4())

        }
    ]
    errors = client.insert_rows_json('saudeid-dados-prd.rz_ocr_google.event_text_orc', rows_to_insert)
    if errors == []:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))



def main(event, context):
    texts = detect_text_vision('includes/test.png')
    write_big_query(texts,event,context)