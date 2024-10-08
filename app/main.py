import socket  # noqa: F401
import threading

REQUEST_LENGTH = "request_length" 
REQUEST_API_KEY = "request_api_key" 
REQUEST_API_VERSION = "request_api_version" 
CORRELATION_ID = "correlation_id" 

# API Keys

FETCH = int.to_bytes(1, 2, signed=True)
API_VERSION = int.to_bytes(18, 2, signed=True)

UNSUPPORTED_VALUE = int.to_bytes(35, 2, signed=True)

#Versions
MIN_API_VERSION = int.to_bytes(0, 2, signed=True)
MAX_API_VERSION = int.to_bytes(4, 2, signed=True)
MIN_FETCH_VERSION = int.to_bytes(0, 2, signed=True)
MAX_FETCH_VERSION = int.to_bytes(18, 2, signed=True)

DEFAULT_TAG_BUFFER = int.to_bytes(0, 1, signed=True)

ERROR_CODE_ZERO = int.to_bytes(0, 2, signed=True)

def parse_request(req):
    request_length = req[0:4]
    request_api_key = req[4:6]
    request_api_version	= req[6:8]
    correlation_id = req[8:12]

    return {
        REQUEST_LENGTH : request_length,
        REQUEST_API_KEY : request_api_key,
        REQUEST_API_VERSION : request_api_version,
        CORRELATION_ID: correlation_id
    }

def validate_api_version(api_version):
    return api_version >= MAX_API_VERSION and api_version <= MAX_API_VERSION

def build_message_body(parsed_req):
    throttle_time_ms = int.to_bytes(0, 4, signed=True)
    error_code = (
        ERROR_CODE_ZERO 
        if validate_api_version(parsed_req[REQUEST_API_VERSION]) 
        else UNSUPPORTED_VALUE
    )

    return (
        error_code + 
        int.to_bytes(3, 1) + #num_of_api_keys => INT8
        parsed_req[REQUEST_API_KEY] + 
        MIN_API_VERSION + 
        MAX_API_VERSION +
        DEFAULT_TAG_BUFFER +
        FETCH +
        MIN_FETCH_VERSION +
        MAX_FETCH_VERSION +
        DEFAULT_TAG_BUFFER +
        throttle_time_ms + 
        DEFAULT_TAG_BUFFER
    )


def build_message(parsed_req) :

    response_header = parsed_req[CORRELATION_ID]

    response_body = build_message_body(parsed_req)

    message = response_header + response_body

    return len(message).to_bytes(4) + message

def handle_client(client):
    while True:
        req = client.recv(2048)
        ps = parse_request(req)
        message = build_message(ps)
        print(message)
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
    
    print("Logs from your program will appear here!")

    


if __name__ == "__main__":
    main()
