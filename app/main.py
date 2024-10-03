import socket  # noqa: F401
import json

def main():
    
    print("Logs from your program will appear here!")

    server = socket.create_server(("localhost", 9092), reuse_port=True)

    while (True):
        client, cli_addr = server.accept() # wait for client
        
        req = client.recv(2048)
        correlation_id = req[8:12]
        message_length = len(correlation_id).to_bytes(4, byteorder='big')

        message = message_length + correlation_id
        print(message)
        
        client.sendall(message)
        client.close()


if __name__ == "__main__":
    main()
