import boto3
import botocore
import os
import logging
import io
from PIL import Image, UnidentifiedImageError
import uuid
from dotenv import load_dotenv
import uuid
import json
from opensearchpy import OpenSearch

# Load environment variables from .env.local file
load_dotenv('.env.local')

# Get AWS credentials and region from environment variables
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
aws_session_token = os.getenv('AWS_SESSION_TOKEN')
aws_region = os.getenv('AWS_REGION')

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
console = logging.StreamHandler()
console.setLevel(level=logging.DEBUG)
formatter =  logging.Formatter('%(levelname)s : %(message)s')
console.setFormatter(formatter)
logger.addHandler(console)

opensearch_endpoint = os.getenv('OPENSEARCH_ENDPOINT')

# Initialize OpenSearch client
opensearch_client = OpenSearch(
    hosts=[opensearch_endpoint],
    http_auth=(os.getenv('OPENSEARCH_USERNAME'), os.getenv('OPENSEARCH_PASSWORD')),
    use_ssl=True,
    verify_certs=True,
)

# OpenSearch index name
index_name = "images_index"

def lambda_handler(event, context):
    # Initialize the Rekognition and S3 clients
    rekognition_client = boto3.client('rekognition')
    s3_client = boto3.client('s3')

    # Collection name
    collection_id = 'scu_faces_collection'
    meta_data_dict = {}

    def create_collection_if_not_exists(collection_id):
        try:
            # Check if the collection already exists
            response = rekognition_client.describe_collection(CollectionId=collection_id)
            logger.info(f"Collection '{collection_id}' already exists.")
        except rekognition_client.exceptions.ResourceNotFoundException:
            # If the collection does not exist, create it
            response = rekognition_client.create_collection(CollectionId=collection_id)
            logger.info(f"Collection '{collection_id}' created successfully.")
        except botocore.exceptions.ClientError as error:
            logger.error(f"An error occurred: {error}")

    def index_metadata_in_opensearch(s3_location, faces, objects):
        """Index metadata into OpenSearch."""
        try:
            document = {
                "s3_location": s3_location,
                "faces_in_picture": faces,
                "objects_in_picture": objects,
            }
            print(document)
            response = opensearch_client.index(index=index_name, body=document)
            logger.info(f"Successfully indexed document into OpenSearch: {response}")
        except Exception as e:
            logger.error(f"Failed to index metadata into OpenSearch: {str(e)}")

    def upload_new_face_to_aws(image_byte_arr, bucket_name):
        # Define the destination bucket and key
        logger.info("Uploading new face to aws with id")
        random_id = str(uuid.uuid4())
        destination_key = "faces/" + random_id
        
        # Upload the image to the destination bucket
        try:
            s3_client.put_object(Bucket=bucket_name, Key=destination_key, Body=image_byte_arr, ContentType='image/jpeg')
            logger.info("Uploaded cropped person into s3 bucket faces")
        except Exception as e:
            logger.info("Failed to upload cropped person into s3 bucket faces")
        
        try:
            meta_data_dict[random_id] = 'person'
            logger.info("Tagged person's image in s3 (for upload later)")
        except Exception as e:
            logger.info("Failed to tag person with image id")

        try:
            response = rekognition_client.index_faces(
                CollectionId=collection_id,
                Image={'Bytes': image_byte_arr},
                ExternalImageId=random_id,  # Use a unique identifier
                DetectionAttributes=['ALL']
            )
            logger.info(f"Response of Rekognition: {response}")
            logger.info("Trained facial recognition model on person")
        except Exception as e:
            logger.info("Failed to train facial recognition model on person")

    def run_model_on_image(event):
        try:
            # Get bucket name and object key from the event triggered by S3
            bucket_name = "scu-hackathon-bucket"  #TODO: event['Records'][0]['s3']['bucket']['name']
            object_key = "photos/drivers4.jpg" #TODO: event['Records'][0]['s3']['object']['key']
            logger.info(f"Processing image from bucket: {bucket_name}, key: {object_key}")
            
            try:
                # Download the image from S3
                s3_object = s3_client.get_object(Bucket=bucket_name, Key=object_key)
                image_data = s3_object['Body'].read()
                logger.info("Downloaded image from S3")

                try:
                    # Open the image
                    group_image = Image.open(io.BytesIO(image_data))
                    logger.info("Opened image")
                except UnidentifiedImageError as e:
                    logger.info(f"Error: Unable to identify image format for object {object_key} in bucket {bucket_name}.")
                    raise e

            except botocore.exceptions.ClientError as e:
                logger.info(f"AWS ClientError: {e.response['Error']['Message']} (Bucket: {bucket_name}, Key: {object_key})")
                raise e


            except Exception as e:
                logger.info(f"Unexpected error: {str(e)}")
                raise e
            
            logger.info("Analyzing image using Rekognition")
            # Call Rekognition to detect faces in the image
            response = rekognition_client.detect_faces(
                Image={
                    'S3Object': {
                        'Bucket': bucket_name,
                        'Name': object_key,
                    }
                },
                Attributes=['ALL']
            )
            
            # Log the number of faces found
            face_count = len(response['FaceDetails'])
            logger.info(f"Number of faces detected: {face_count}")

            # Get bounding boxes of each detected face
            face_bounding_boxes = response['FaceDetails']

            # Iterate over each detected face and crop it for comparison
            logger.info("Going over each face")
            for i, face in enumerate(face_bounding_boxes):
                # Bounding box coordinates
                box = face['BoundingBox']
                width, height = group_image.size
                left = int(box['Left'] * width)
                top = int(box['Top'] * height)
                right = int(left + (box['Width'] * width))
                bottom = int(top + (box['Height'] * height))

                # Crop the face area
                cropped_face = group_image.crop((left, top, right, bottom))
                logger.info("Cropped face")
                
                # Convert cropped face image to bytes
                buffered = io.BytesIO()
                cropped_face.save(buffered, format="JPEG")
                face_image_bytes = buffered.getvalue()
                logger.info("Converted cropped face into bytes")
                
                # Search for this cropped face in the collection
                search_response = rekognition_client.search_faces_by_image(
                    CollectionId=collection_id,
                    Image={'Bytes': face_image_bytes},
                    MaxFaces=1,                # Max number of matching faces to return
                    FaceMatchThreshold=90      # Adjust threshold as needed
                )
                logger.info("Searched for face in faces collection")

                # Check if there's a match
                if search_response['FaceMatches']:
                    for match in search_response['FaceMatches']:
                        logger.info(f"Face {i + 1} matches with {match['Face']['ExternalImageId']} at {match['Similarity']:.2f}% similarity.")
                        random_id = match['Face']['ExternalImageId']
                        destination_key = object_key
                        try:
                            meta_data_dict[random_id] = 'person'
                            logger.info("Tagged person's image in s3")
                        except Exception as e:
                            logger.info("Failed to tag person with image id")
                else:
                    logger.info(f"Face {i + 1} has no match in the collection -> let's upload it.")
                    upload_new_face_to_aws(face_image_bytes, bucket_name)

            logger.info("Moving on to adding tags with objects")
            rekognition_response = rekognition_client.detect_labels(
                Image={
                    'S3Object': {
                        'Bucket': bucket_name,
                        'Name': object_key,
                    }
                },
                MaxLabels=20,
                MinConfidence=75
            )
            
            # Log and return detected labels
            labels = rekognition_response['Labels']
            logger.info(f"Detected labels for {object_key}: {labels}")

            metadata={}
            metadata['objects_in_picture'] = json.dumps({label['Name']: str(label['Confidence']) for label in labels})
            metadata['faces_in_picture'] = json.dumps(meta_data_dict)
        
            # Add tags to the S3 object
            try:
                s3_client.copy_object(
                    Bucket=bucket_name,
                    CopySource={'Bucket': bucket_name, 'Key': object_key},
                    Key=object_key,
                    Metadata=metadata,
                    MetadataDirective='REPLACE'
                )

                logger.info(f"Successfully tagged {object_key} in {bucket_name} with labels.")
            except Exception as e:
                logger.error(f"Error tagging object {object_key}: {str(e)}")
            
            logger.info("Model run successfully on the image.")

            # Index metadata in OpenSearch
            index_metadata_in_opensearch(
                s3_location=f"s3://{bucket_name}/{object_key}",
                faces=meta_data_dict,
                objects=metadata["objects_in_picture"]
            )

            return response
        except botocore.exceptions.ClientError as error:
            logger.error(f"An error occurred while running the model: {error}")
            return None

    # Run the function to create the collection if it doesn't exist
    create_collection_if_not_exists(collection_id)
   
    # Run the model on the image
    return run_model_on_image(event)

#TODO: delete
event = ""
context = ""
lambda_handler(event, context)