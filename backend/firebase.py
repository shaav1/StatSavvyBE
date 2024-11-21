import nfl_data_py as nfl
import pandas as pd
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

cred = credentials.Certificate('')
firebase_admin.initialize_app(cred)

db = firestore.client()


def add_player(player_id, name, position):
    employee_ref = db.collection('players').document(player_id)
    employee_ref.set({
        'name': name,
        'position': position,
    })
    print(f"Added player: {name}")


def update_player(player_id, updates):
    player_ref = db.collection('players').document(player_id)
    if player_ref.get().exists:
        player_ref.update(updates)
        print(f"Updated player: {player_id}")
    else:
        player_ref.set(updates)
        print(f"Added new player: {player_id}")


def bulk_delete_field(collection_name, field_name):
    # Get reference to collection
    collection_ref = db.collection(collection_name)

    # Get all documents in collection
    docs = collection_ref.get()

    # Delete field from each document
    for doc in docs:
        doc.reference.update({
            field_name: firestore.DELETE_FIELD
        })
        print(f"Deleted {field_name} from document: {doc.id}")


# Example usage:
#bulk_delete_field('players', 'attempts')
# Example usage
