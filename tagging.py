import os
import boto3
from dotenv import load_dotenv

# Load environment variables from .env.local file
load_dotenv('.env.local')

# Get AWS credentials and region from environment variables
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
aws_session_token = os.getenv('AWS_SESSION_TOKEN')
aws_region = os.getenv('AWS_REGION')

# Create a Rekognition client with session token
rekognition_client = boto3.client(
    'rekognition',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    aws_session_token=aws_session_token,
    region_name=aws_region
)

# Specify the image file
image_file = 'test_image.jpg'

# Read the image bytes
with open(image_file, 'rb') as image:
    image_bytes = image.read()

# Call Rekognition to detect labels
response = rekognition_client.detect_labels(
    Image={'Bytes': image_bytes},
    MaxLabels=15,
    MinConfidence=70
)

# Print detected labels
for label in response['Labels']:
    print(f"Label: {label['Name']}, Confidence: {label['Confidence']:.2f}%")