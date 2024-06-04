import time
import threading
import socket
import json
from pymongo import MongoClient

# MongoDB connection details
MONGO_HOST = 'localhost'
MONGO_PORT = 27017
DB_NAME = 'pillowdb'
COLLECTION_NAME = 'pillows'

# TCP connection details
TCP_HOST = 'localhost'
TCP_PORT = 9999

def get_mongo_client():
    client = MongoClient(MONGO_HOST, MONGO_PORT)
    return client

def read_data_circularly(collection):
    cursor = collection.find({})
    while True:
        for document in cursor:
            yield document
        cursor = collection.find({})
        
def convert_objectid_to_str(document):
    document['_id'] = str(document['_id'])
    return document

def handle_client(client_socket, collection):
    try:
        for document in read_data_circularly(collection):
            document = convert_objectid_to_str(document)
            print(f"Sending document: {json.dumps(document)}")
            client_socket.send((json.dumps(document) + '\n').encode('utf-8'))  # Send the document as a data stream
            time.sleep(10)
    except Exception as e:
        print(f"Error: {e}")
    finally:
        print("Closing client connection")
        client_socket.close()

def start_server(host, port, collection):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((host, port))
    server_socket.listen()
    print(f"Server listening on {host}:{port}")

    while True:
        client_socket, address = server_socket.accept()
        print(f"Client connected from {address[0]}:{address[1]}")
        client_thread = threading.Thread(target=handle_client, args=(client_socket, collection))
        client_thread.start()

if __name__ == "__main__":
    client = get_mongo_client()
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    start_server(TCP_HOST, TCP_PORT, collection)
