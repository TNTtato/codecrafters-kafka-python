from app.constants import *

UNSUPPORTED_VALUE = int.to_bytes(35, 2, signed=True)

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
    pass

def build_headers_v2(parsed_req_headers):
    pass

def build_response_headers(parsed_req_headers):
    pass

def build_response_api_versions_v4(parsed_req):
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

def build_response_fetch_v16(parsed_req):
    pass

def build_response_fetch(parsed_req):
    pass

def build_reponse_api_versions(parsed_req):
    api_version = parsed_req[API_VERSION]
    if api_version == 4:
        return build_response_api_versions_v4(parsed_req)

def handle_response(parsed_req):
    api_key = int.from_bytes(parsed_req[REQUEST_API_KEY])
    if int.from_bytes(api_key) == 1:
        return build_response_fetch(parsed_req)
    if api_key == 18:
        return build_reponse_api_versions(parsed_req)