This is a fastapi code and sqlalchemy orm and postgresql is being used for the db.
To run the code create a virtual environment and do pip install -r requirement.txt to install the packages and run command uvicorn main:app --reload or python -m uvicorn main:app --reload.
After that you will see the app running on localhost:8000 and go to localhost:8000/docs to see swagger documentation and test the apis on swagger.
Upload file is asynchronous event a seperate task runs in the background to process files. It takes the files stored in a queue and processes one by one.
Table schemas and db connection are in db.py file.
I have create a txt document which takes event data in txt format. i have attached it in this repo. Use the same file for testing or same format as provided in this file.
