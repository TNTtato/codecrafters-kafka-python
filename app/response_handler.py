from app.constants import *

UNSUPPORTED_VALUE = int.to_bytes(35, 2, signed=True)
UNKWNON_TOPIC = int.to_bytes(100, 2, signed=True)

#Versions

MIN_API_VERSION = int.to_bytes(0, 2, signed=True)
MAX_API_VERSION = int.to_bytes(4, 2, signed=True)
MIN_FETCH_VERSION = int.to_bytes(0, 2, signed=True)
MAX_FETCH_VERSION = int.to_bytes(16, 2, signed=True)

ERROR_CODE_ZERO = int.to_bytes(0, 2, signed=True)
DEFAULT_TAG_BUFFER = int.to_bytes(0, 1, signed=True)

min_max_versions = {
    FETCH: [MIN_FETCH_VERSION, MAX_FETCH_VERSION], # api key : 1
    API_VERSION: [MIN_API_VERSION, MAX_API_VERSION] # api key: 18
}

def validate_api_version(api_key, api_version):
    min_max = min_max_versions[api_key]
    return api_version >= min_max[0] and api_version <= min_max[1]

def build_headers_v1(parsed_req_headers):
    return parsed_req_headers[CORRELATION_ID]

def build_headers_v2(parsed_req_headers):
    return parsed_req_headers[CORRELATION_ID] + DEFAULT_TAG_BUFFER

def build_response_headers(api_key, parsed_req_headers):
    if api_key == 18:
        return build_headers_v1(parsed_req_headers)
    
    return build_headers_v2(parsed_req_headers)

def build_response_api_versions_v4(api_version, parsed_req):
    throttle_time_ms = int.to_bytes(0, 4, signed=True)
    error_code = (
        ERROR_CODE_ZERO 
        if validate_api_version(API_VERSION, int.to_bytes(api_version, 2)) 
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

def build_response_fetch_v16(api_version, req_body):  
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
    error_code = (
        ERROR_CODE_ZERO 
        if validate_api_version(FETCH, int.to_bytes(api_version, 2)) 
        else UNSUPPORTED_VALUE
    )
    throttle_time_ms = int.to_bytes(0, 4, signed=True)
    session_id = req_body[SESSION_ID]
    req_topics = req_body[TOPICS]
    num_request = len(req_topics)
    responses = int.to_bytes(num_request + 1, 1)
    for topic in req_topics:
        responses += topic[TOPIC_ID]
        req_partitons = topic[PARTITIONS]
        num_partitions = len(req_topics)
        responses += int.to_bytes(num_partitions + 1, 1)
        #for partitions
        idx = 0
        for _ in range(1, num_partitions + 1):
            responses += int.to_bytes(idx, 4) #partition_index
            responses += UNKWNON_TOPIC #error code
            responses += int.to_bytes(0, 8) #high_watermark
            responses += int.to_bytes(0,8) #last_stable_offset
            responses += int.to_bytes(0,8) #log_start_offset
            responses += int.to_bytes(0 + 1, 1) #num_aborted_transactions
            responses += int.to_bytes(0,4) #preferred_read_replica
            responses += int.to_bytes(1, 1, signed=True) #COMPACT_RECORDS
            responses += DEFAULT_TAG_BUFFER
        responses += DEFAULT_TAG_BUFFER
    responses += DEFAULT_TAG_BUFFER
    

    return (
        throttle_time_ms +
        error_code +
        session_id +
        responses # responses
    )


def build_response_fetch(api_version, req_body):
    if api_version == 16:
        return build_response_fetch_v16(api_version, req_body)

def build_reponse_api_versions(api_version, req_body):
    #if api_version == 4:
    return build_response_api_versions_v4(api_version, req_body)
    
def build_body(api_key, api_version, req_body):
    if api_key == 1:
        return build_response_fetch(api_version, req_body)
    if api_key == 18:
        return build_reponse_api_versions(api_version, req_body)

def handle_response(parsed_req: dict[str, dict]):
    
    request_headers = parsed_req[HEADERS]
    request_body = parsed_req[BODY]

    api_key = int.from_bytes(request_headers[REQUEST_API_KEY])
    api_version = int.from_bytes(request_headers[REQUEST_API_VERSION])

    response_headers = build_response_headers(api_key, request_headers)
    response = build_body(api_key, api_version, request_body)

    print(response_headers)
    print(response)

    full_message = response_headers + response
    return int.to_bytes(len(full_message), 4) + full_message


