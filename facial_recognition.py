import boto3

collection_id = 'my_face_collection'
import boto3
import botocore

# Initialize the Rekognition client
rekognition_client = boto3.client('rekognition')

# Collection name
collection_id = 'scu_faces_collection'

def create_collection_if_not_exists(collection_id):
    try:
        # Check if the collection already exists
        response = rekognition_client.describe_collection(CollectionId=collection_id)
        print(f"Collection '{collection_id}' already exists.")
    except rekognition_client.exceptions.ResourceNotFoundException:
        # If the collection does not exist, create it
        response = rekognition_client.create_collection(CollectionId=collection_id)
        print(f"Collection '{collection_id}' created successfully.")
    except botocore.exceptions.ClientError as error:
        print(f"An error occurred: {error}")

create_collection_if_not_exists(collection_id)

# Open the group image and load it as bytes for initial face detection
with open('drivers4.jpg', 'rb') as image_file:
    image_bytes = image_file.read()
    group_image = Image.open(io.BytesIO(image_bytes))

# Detect faces in the image
response = rekognition.detect_faces(
    Image={'Bytes': image_bytes},
    Attributes=['DEFAULT']
)

# Get bounding boxes of each detected face
face_bounding_boxes = response['FaceDetails']

# Iterate over each detected face and crop it for comparison
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
    
    # Convert cropped face image to bytes
    buffered = io.BytesIO()
    cropped_face.save(buffered, format="JPEG")
    face_image_bytes = buffered.getvalue()
    
    # Search for this cropped face in the collection
    search_response = rekognition.search_faces_by_image(
        CollectionId=collection_id,
        Image={'Bytes': face_image_bytes},
        MaxFaces=1,                # Max number of matching faces to return
        FaceMatchThreshold=90      # Adjust threshold as needed
    )

    # Check if there's a match
    if search_response['FaceMatches']:
        for match in search_response['FaceMatches']:
            print(f"Face {i + 1} matches with {match['Face']['ExternalImageId']} at {match['Similarity']:.2f}% similarity.")
    else:
        print(f"Face {i + 1} has no match in the collection.")
