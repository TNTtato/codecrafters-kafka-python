import socket  # noqa: F401
import threading
import uuid

from app.request_parser import *
from app.constants import *
from app.request_parser import parse
from app.response_handler import handle_response


def handle_client(client):
    while True:
        req = client.recv(2048)
        ps = parse(req)
        message = handle_response(ps)
        #print(message)
        client.send(message)
        #client.close()

def handle_server(addr, port):
    server = socket.create_server((addr, port), reuse_port=True)
    
    while (True):
        client, cli_addr = server.accept() # wait for client    
        client_thread = threading.Thread(target=handle_client, args=(client,))
        client_thread.start()
        

def main():
    
    #t = threading.Thread(target=handle_server, args=("localhost", 9092))
    #t.start()
    handle_server("localhost", 9092)

    


if __name__ == "__main__":
    main()
