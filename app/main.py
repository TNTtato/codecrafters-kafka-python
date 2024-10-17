import socket  # noqa: F401
import threading
import uuid

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
MAX_FETCH_VERSION = int.to_bytes(16, 2, signed=True)

min_max_versions = {
    FETCH: [MIN_FETCH_VERSION, MAX_FETCH_VERSION], # api key : 1
    API_VERSION: [MIN_API_VERSION, MAX_API_VERSION] # api key: 18
}

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

def validate_api_version(api_key, api_version):
    min_max = min_max_versions[api_key]
    return api_version >= min_max[0] and api_version <= min_max[1]

def build_response_header(parsed_req):
    return parsed_req[CORRELATION_ID] + DEFAULT_TAG_BUFFER if parsed_req[REQUEST_API_KEY] == FETCH else parsed_req[CORRELATION_ID]

def build_response_api_versions(parsed_req):
    throttle_time_ms = int.to_bytes(0, 4, signed=True)
    error_code = (
        ERROR_CODE_ZERO 
        if validate_api_version(parsed_req[REQUEST_API_KEY], parsed_req[REQUEST_API_VERSION]) 
        else UNSUPPORTED_VALUE
    )

    return (
        error_code + 
        int.to_bytes(3, 1) + #num_of_api_keys => INT8
        API_VERSION + 
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

def build_reponse_fetch_v16(parsed_req):
    '''
    Fetch Response (Version: 16) => throttle_time_ms error_code session_id [responses] TAG_BUFFER 
    throttle_time_ms => INT32
    error_code => INT16
    session_id => INT32
    responses => topic_id [partitions] TAG_BUFFER 
        topic_id => UUID
        partitions => partition_index error_code high_watermark last_stable_offset log_start_offset [aborted_transactions] preferred_read_replica records TAG_BUFFER 
            partition_index => INT32
            error_code => INT16
            high_watermark => INT64
            last_stable_offset => INT64
            log_start_offset => INT64
            aborted_transactions => producer_id first_offset TAG_BUFFER 
                producer_id => INT64
                first_offset => INT64
            preferred_read_replica => INT32
            records => COMPACT_RECORDS
    '''
    throttle_time_ms = int.to_bytes(0, 4, signed=True)
    error_code = (
        ERROR_CODE_ZERO 
        if validate_api_version(parsed_req[REQUEST_API_KEY], parsed_req[REQUEST_API_VERSION]) 
        else UNSUPPORTED_VALUE
    )
    session_id = int.to_bytes(0, 4, signed=True)
    response_uuid = uuid.uuid4().bytes
    num_partitions = int.to_bytes(2, 1, signed=True)

    return (
        throttle_time_ms +
        error_code +
        session_id +
        int.to_bytes(1, 1, signed=True) + #num partitions
        DEFAULT_TAG_BUFFER
    )
    


def build_response_fetch(parsed_req):
    if parsed_req[REQUEST_API_VERSION] == int.to_bytes(16, 2, signed=True):
        return build_reponse_fetch_v16(parsed_req)

def build_message_body(parsed_req):
    #TODO build proper messages based on request [api key, api version] 
    api_key = parsed_req[REQUEST_API_KEY]

    if api_key == API_VERSION:
        return build_response_api_versions(parsed_req)
    if api_key == FETCH:
        return build_response_fetch(parsed_req)

def build_message(parsed_req) :

    response_header = build_response_header(parsed_req)

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

    


if __name__ == "__main__":
    main()
