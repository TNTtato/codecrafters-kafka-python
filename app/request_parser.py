from app.constants import *

def parse_request_header_v2(req: bytes) -> dict[str, bytes]:
    request_length = req[0:4]
    request_api_key = req[4:6]
    request_api_version	= req[6:8]
    correlation_id = req[8:12]
    client_id_size = req[12:14]
    offset = int.from_bytes(client_id_size, signed=True)
    client_id = req[14:14 + offset] if offset != -1 else b'\x00'
    offset = (14 if offset == -1 else 14 + offset)
    tag_buffer = req[offset: offset + 1]
    offset += 1

    return {
        REQUEST_LENGTH : request_length,
        REQUEST_API_KEY : request_api_key,
        REQUEST_API_VERSION : request_api_version,
        CORRELATION_ID: correlation_id,
        CLIENT_ID_SIZE: client_id_size,
        CLIENT_ID: client_id,
        TAG_BUFFER: tag_buffer,
        OFFSET: int.to_bytes(offset, 4)
    }


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
def parse_request_fetch_v16(body):
    max_wait_ms = body[0:4]
    min_bytes = body[4:8]
    max_bytes = body[8:12]
    isolation_level = body[12:13]
    session_id = body[13:17]
    session_epoch = body[17:21]
    num_topics = body[21:22]
    topics = []
    offset = 0
    for _ in range(1, int.from_bytes(num_topics, signed=True)) :
        topic_id = body[offset + 22: offset + 16 + 22]
        num_partitions = body[offset + 16 + 22: offset + 17 + 22]
        partitions = []
        for _ in range(1, int.from_bytes(num_partitions, signed=True)):
            partition = body[offset + 17 + 22: offset + 21 + 22]
            current_leader_epoch = body[offset + 21 + 22: offset + 25 + 22]
            fetch_offset = body[offset + 25 + 22: offset + 33 + 22]
            last_fetched_epoch = body[offset + 33 + 22: offset + 37 + 22]
            log_start_offset = body[offset + 37 + 22: offset + 45 + 22]
            partition_max_bytes = body[offset + 45 + 22: offset + 49 + 22]
            tag_buffer = body[offset + 49 + 22: offset + 50 + 22]
            partitions.append({
                PARTITION: partition,
                CURRENT_LEADER_EPOCH: current_leader_epoch,
                FETCH_OFFSET: fetch_offset,
                LAST_FETCHED_EPOCH: last_fetched_epoch,
                LOG_START_OFFSET: log_start_offset,
                PARTITION_MAX_BYTES: partition_max_bytes
            })
            offset += 55
        topics.append({
            TOPIC_ID: topic_id,
            PARTITIONS: partitions
		})
        offset += 1
    num_forgotten_topics_data = body[22 + offset: 23 + offset]
    forgotten_topics_data = []
    for _ in range(1, int.from_bytes(num_forgotten_topics_data, signed=True)):
        topic_id_ftd = body[23 + offset: 23 + offset + 16]
        num_partitions_ftd = body[23 + offset + 16: 23 + offset + 17]
        partitions_ftd = []
        for _ in range(1, int.from_bytes(num_partitions_ftd, signed=True)):
            partition_ftd = body[23 + offset + 17: 23 + offset + 21]
            partitions_ftd.append(partition_ftd)
            offset += 4
        forgotten_topics_data.append({
			TOPIC_ID: topic_id_ftd,
            PARTITIONS: partitions_ftd
		})
        offset += 1
    # TODO: Figure out how to parse varint
    rack_id_len = body[23 + offset: 24 + offset]

        

    return {
		MAX_WAIT_MS: max_wait_ms,
        MIN_BYTES: min_bytes,
        MAX_BYTES: max_bytes,
        ISOLATION_LEVEL: isolation_level,
        SESSION_ID: session_id,
        SESSION_EPOCH: session_epoch,
        TOPICS: topics
    }

def parse_request_body(api_key, api_version, body):
    
    if api_key == 1:
        if api_version == 16:
            return parse_request_fetch_v16(body)

def parse(req):
    headers = parse_request_header_v2(req)
    offset = headers[OFFSET]
    body = parse_request_body(
        int.from_bytes(headers[REQUEST_API_KEY]), 
        int.from_bytes(headers[REQUEST_API_VERSION]), 
        req[int.from_bytes(offset):])
    return {
        HEADERS: headers,
        BODY: body
    }
