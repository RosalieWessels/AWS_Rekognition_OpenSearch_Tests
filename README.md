
# Image Search using AWS + AI

### AWS Rekognition + Lambda + S3 + OpenSearch demo

**Demo Video**: 

[![Video Overview](https://img.youtube.com/vi/60OHaW2P9No/0.jpg)](https://www.youtube.com/watch?v=60OHaW2P9No)


**Technical Pipeline:**
When uploading images via the website, they will be added to an AWS S3 bucket. The S3 bucket is configured to kickoff a lambda function upon image upload, which will analyze the contents of the image. 

First, it will use Rekognition in order to generate the top 15 attributes/objects in the image. Then, it will scan the image for faces and go through each face individually. If the person is already in the faces collection, it will tag them with their id. If they are not, it will add them to the collection, and generate an id them. These ids are not matched to a specific person's name unless they opt in by uploading an image of themselves to their account (linking their name + id).

Next, these ids for the faces as well as tags will be added to the s3 object's metadata. They will also be indexed into AWS OpenSearch. 

Check `lambda_function.py` for the full Lambda function code.

When searching for an image on the website using natural language, it will make a query to the OpenSearch index, and return all the relevant images (which are then shown on screen).

Next.js front end code is here: [Scrapbook Github Repo](https://github.com/sage31/INRIX-AWS-Hackathon/tree/Rosalie-w-file-upload) (in Rosalie-w-file-upload branch). 

Devpost for the whole project: [DevPost](https://devpost.com/software/scrapbook-6vz4o5)
