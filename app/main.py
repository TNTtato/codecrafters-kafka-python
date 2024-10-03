import socket  # noqa: F401

def id_to_bytes(id):
    return id.to_bytes(8)


def main():
    
    print("Logs from your program will appear here!")

    server = socket.create_server(("localhost", 9092), reuse_port=True)

    while (True):
        client, addr = server.accept() # wait for client
        client.send(id_to_bytes(7))
        client.close()


if __name__ == "__main__":
    main()
