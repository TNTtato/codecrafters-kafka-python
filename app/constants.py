HEADERS = 'headers'
BODY = 'body'

# request headers fields
REQUEST_LENGTH = "request_length" 
REQUEST_API_KEY = "request_api_key" 
REQUEST_API_VERSION = "request_api_version" 
CORRELATION_ID = "correlation_id"
TAG_BUFFER = "tag_buffer" 
CLIENT_ID_SIZE = "client_id_size"
CLIENT_ID = "client_id"
OFFSET = "offset"

# api keys

FETCH = int.to_bytes(1, 2, signed=True)
API_VERSION = int.to_bytes(18, 2, signed=True)


'''
Fetch Request (Version: 16) => max_wait_ms min_bytes max_bytes isolation_level session_id session_epoch [topics] [forgotten_topics_data] rack_id TAG_BUFFER 
  max_wait_ms => INT32
  min_bytes => INT32
  max_bytes => INT32
  isolation_level => INT8
  session_id => INT32
  session_epoch => INT32
  topics => topic_id [partitions] TAG_BUFFER 
    topic_id => UUID
    partitions => partition current_leader_epoch fetch_offset last_fetched_epoch log_start_offset partition_max_bytes TAG_BUFFER 
      partition => INT32
      current_leader_epoch => INT32
      fetch_offset => INT64
      last_fetched_epoch => INT32
      log_start_offset => INT64
      partition_max_bytes => INT32
  forgotten_topics_data => topic_id [partitions] TAG_BUFFER 
    topic_id => UUID
    partitions => INT32
  rack_id => COMPACT_STRING
'''
MAX_WAIT_MS = 'max_wait_ms'
MIN_BYTES = 'min_bytes'
MAX_BYTES = 'max_bytes'
ISOLATION_LEVEL = 'isolation_level'
SESSION_ID = 'session_id'
SESSION_EPOCH = 'session_epoch'
TOPICS = 'topics'
TOPIC_ID = 'topic_id'
PARTITIONS = 'partitions'
PARTITION = 'partition'
CURRENT_LEADER_EPOCH = 'current_leader_epoch'
FETCH_OFFSET = 'fetch_offset'
LAST_FETCHED_EPOCH = 'last_fetched_epoch' 
LOG_START_OFFSET = 'log_start_offset'
PARTITION_MAX_BYTES = 'partition_max_bytes'
FORGOTTEN_TOPICS_DATA = 'forgotten_topics_data'
RACK_ID = 'rack_id'