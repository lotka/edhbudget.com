import firebase_admin
from firebase_admin import credentials, firestore

from config import PROJECT_ID


def initialize_firebase():
    if not firebase_admin._apps:
        cred = credentials.ApplicationDefault()
        firebase_admin.initialize_app(cred, {"projectId": PROJECT_ID})


initialize_firebase()
db = firestore.client()
