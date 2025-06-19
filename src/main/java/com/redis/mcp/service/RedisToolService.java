package com.redis.mcp.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.Record;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamRecords;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

/**
 * Redis Tool Service for handling Redis operations
 * @author yue9527
 */
@Service
public class RedisToolService {

    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;

    /**
     * Constructor for RedisToolService
     * @param redisTemplate StringRedisTemplate instance
     * @param objectMapper ObjectMapper instance
     */
    public RedisToolService(StringRedisTemplate redisTemplate, ObjectMapper objectMapper) {
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
    }

    /**
     * Set a key-value pair in Redis with optional expiration time
     * @param jsonArgs JSON string containing key, value and optional parameters for different data types:
     *                - For string: key, value, expireSeconds
     *                - For list: key, value, index (optional), append (boolean, optional)
     *                - For set: key, value (member to add)
     *                - For zset: key, value (member), score
     *                - For hash: key, field, value
     *                - For stream: key, value (map of fields), id (optional)
     * @return Operation result message
     */
    @Tool(name = "set", description = "Set a Redis key-value pair with optional expiration time, supports string, list, set, zset, hash, and stream types")
    public String setValue(String jsonArgs) {
        try {
            Map<String, Object> args = objectMapper.readValue(jsonArgs, Map.class);
            
            // Validate key parameter
            Object keyObj = args.get("key");
            if (keyObj == null) {
                return "Error: 'key' parameter is required";
            }
            String key = keyObj.toString();
            if (!StringUtils.hasText(key)) {
                return "Error: Empty key provided";
            }
            
            // Get data type parameter or determine from existing key
            String dataTypeStr = (String) args.get("type");
            org.springframework.data.redis.connection.DataType dataType = null;
            
            // If key exists, get its type
            boolean keyExists = Boolean.TRUE.equals(redisTemplate.hasKey(key));
            if (keyExists) {
                dataType = redisTemplate.type(key);
            }
            
            // If type is explicitly specified, use it
            if (dataTypeStr != null) {
                try {
                    dataTypeStr = dataTypeStr.toUpperCase();
                    if ("STRING".equals(dataTypeStr)) {
                        dataType = org.springframework.data.redis.connection.DataType.STRING;
                    } else if ("LIST".equals(dataTypeStr)) {
                        dataType = org.springframework.data.redis.connection.DataType.LIST;
                    } else if ("SET".equals(dataTypeStr)) {
                        dataType = org.springframework.data.redis.connection.DataType.SET;
                    } else if ("ZSET".equals(dataTypeStr)) {
                        dataType = org.springframework.data.redis.connection.DataType.ZSET;
                    } else if ("HASH".equals(dataTypeStr)) {
                        dataType = org.springframework.data.redis.connection.DataType.HASH;
                    } else if ("STREAM".equals(dataTypeStr)) {
                        dataType = org.springframework.data.redis.connection.DataType.STREAM;
                    } else {
                        return "Error: Unsupported data type: " + dataTypeStr;
                    }
                } catch (Exception e) {
                    return "Error: Invalid data type: " + dataTypeStr;
                }
            }
            
            // If type is not specified and key doesn't exist, default to string
            if (dataType == null) {
                // Determine type from parameters
                if (args.containsKey("field")) {
                    dataType = org.springframework.data.redis.connection.DataType.HASH;
                } else if (args.containsKey("score")) {
                    dataType = org.springframework.data.redis.connection.DataType.ZSET;
                } else if (args.containsKey("index")) {
                    dataType = org.springframework.data.redis.connection.DataType.LIST;
                } else {
                    // Default to string
                    dataType = org.springframework.data.redis.connection.DataType.STRING;
                }
            }
            
            // Handle different data types
            if (org.springframework.data.redis.connection.DataType.STRING.equals(dataType)) {
                // String type
                Object valueObj = args.get("value");
                if (valueObj == null) {
                    return "Error: 'value' parameter is required for string operations";
                }
                String value = valueObj.toString();
                
                Integer expireSeconds = (Integer) args.get("expireSeconds");
                if (expireSeconds != null) {
                    redisTemplate.opsForValue().set(key, value, expireSeconds);
                } else {
                    redisTemplate.opsForValue().set(key, value);
                }
                return "Successfully set string key: " + key;
            } 
            else if (org.springframework.data.redis.connection.DataType.LIST.equals(dataType)) {
                // List type
                Object valueObj = args.get("value");
                if (valueObj == null) {
                    return "Error: 'value' parameter is required for list operations";
                }
                String value = valueObj.toString();
                
                // Check if index is provided
                Object indexObj = args.get("index");
                if (indexObj != null) {
                    try {
                        int index = Integer.parseInt(indexObj.toString());
                        redisTemplate.opsForList().set(key, index, value);
                        return "Successfully set value at index " + index + " in list: " + key;
                    } catch (NumberFormatException e) {
                        return "Error: Invalid index format. Must be an integer";
                    }
                } else {
                    // Check if append flag is provided
                    Boolean append = (Boolean) args.get("append");
                    if (Boolean.TRUE.equals(append)) {
                        redisTemplate.opsForList().rightPush(key, value);
                        return "Successfully appended value to list: " + key;
                    } else {
                        redisTemplate.opsForList().leftPush(key, value);
                        return "Successfully prepended value to list: " + key;
                    }
                }
            }
            else if (org.springframework.data.redis.connection.DataType.SET.equals(dataType)) {
                // Set type
                Object valueObj = args.get("value");
                if (valueObj == null) {
                    return "Error: 'value' parameter is required for set operations";
                }
                String value = valueObj.toString();
                
                Long added = redisTemplate.opsForSet().add(key, value);
                return added > 0 ? 
                       "Successfully added member to set: " + key : 
                       "Member already exists in set: " + key;
            }
            else if (org.springframework.data.redis.connection.DataType.ZSET.equals(dataType)) {
                // Sorted Set type
                Object valueObj = args.get("value");
                if (valueObj == null) {
                    return "Error: 'value' parameter is required for sorted set operations";
                }
                String value = valueObj.toString();
                
                Object scoreObj = args.get("score");
                if (scoreObj == null) {
                    return "Error: 'score' parameter is required for sorted set operations";
                }
                
                try {
                    double score = Double.parseDouble(scoreObj.toString());
                    Boolean added = redisTemplate.opsForZSet().add(key, value, score);
                    return added ? 
                           "Successfully added member with score to sorted set: " + key : 
                           "Member updated in sorted set: " + key;
                } catch (NumberFormatException e) {
                    return "Error: Invalid score format. Must be a number";
                }
            }
            else if (org.springframework.data.redis.connection.DataType.HASH.equals(dataType)) {
                // Hash type
                Object fieldObj = args.get("field");
                if (fieldObj == null) {
                    return "Error: 'field' parameter is required for hash operations";
                }
                String field = fieldObj.toString();
                if (!StringUtils.hasText(field)) {
                    return "Error: Empty field provided for hash operation";
                }
                
                Object valueObj = args.get("value");
                if (valueObj == null) {
                    return "Error: 'value' parameter is required for hash operations";
                }
                String value = valueObj.toString();
                
                redisTemplate.opsForHash().put(key, field, value);
                return "Successfully set hash field: " + field + " in key: " + key;
            }
            else if (org.springframework.data.redis.connection.DataType.STREAM.equals(dataType)) {
                // Stream type
                Object valueObj = args.get("value");
                if (valueObj == null || !(valueObj instanceof Map)) {
                    return "Error: 'value' parameter must be a map for stream operations";
                }
                
                Map<String, String> streamEntries = new HashMap<>();
                ((Map<?, ?>) valueObj).forEach((k, v) -> {
                    if (k != null && v != null) {
                        streamEntries.put(k.toString(), v.toString());
                    }
                });
                
                if (streamEntries.isEmpty()) {
                    return "Error: Stream entries map is empty";
                }
                
                String id = (String) args.get("id");
                RecordId recordId;
                
                // Create a MapRecord with the stream key and entries
                MapRecord<String, String, String> record = StreamRecords.newRecord()
                    .ofMap(streamEntries)
                    .withStreamKey(key);
                
                // Add the record to the stream
                // Spring Data Redis doesn't support setting a custom ID directly on the record
                // The ID is generated by Redis when the record is added
                recordId = redisTemplate.opsForStream().add(record);
                
                return "Successfully added entry to stream: " + key + " with ID: " + recordId;
            }
            else {
                return "Error: Unsupported Redis data type: " + dataType;
            }
        } catch (IOException e) {
            return "Error parsing JSON arguments: " + e.getMessage();
        } catch (Exception e) {
            return "Operation failed: " + e.getMessage();
        }
    }

    /**
     * Get value from Redis by key
     * @param jsonArgs JSON string containing the key and optional field/index/score parameters for different data types
     * @return Retrieved value or error message
     */
    @Tool(name = "get", description = "Get value from Redis by key, supports string, list, set, zset, hash, and stream types")
    public String getValue(String jsonArgs) {
        try {
            Map<String, Object> args = new HashMap<>();
            if (StringUtils.hasLength(jsonArgs)) {
                args = objectMapper.readValue(jsonArgs, Map.class);
            }
            
            Object keyObj = args.get("key");
            if (keyObj == null) {
                return "Error: 'key' parameter is required";
            }
            
            String key = keyObj.toString();
            if (!StringUtils.hasText(key)) {
                return "Error: Empty key provided";
            }
            
            // Check if key exists
            if (!Boolean.TRUE.equals(redisTemplate.hasKey(key))) {
                return "Key not found: " + key;
            }
            
            // Get the type of the key
            org.springframework.data.redis.connection.DataType dataType = redisTemplate.type(key);
            
            // Handle different data types
            if (org.springframework.data.redis.connection.DataType.STRING.equals(dataType)) {
                // String type
                String result = redisTemplate.opsForValue().get(key);
                return result != null ? result : "Key exists but value could not be retrieved: " + key;
            } 
            else if (org.springframework.data.redis.connection.DataType.LIST.equals(dataType)) {
                // List type
                Object indexObj = args.get("index");
                if (indexObj != null) {
                    // Get specific element by index
                    try {
                        int index = Integer.parseInt(indexObj.toString());
                        String element = redisTemplate.opsForList().index(key, index);
                        if (element == null) {
                            return "Index out of range or null element at index: " + index;
                        }
                        return element;
                    } catch (NumberFormatException e) {
                        return "Error: Invalid index format. Must be an integer";
                    }
                } else {
                    // Get all elements
                    Long size = redisTemplate.opsForList().size(key);
                    if (size == null || size == 0) {
                        return "List is empty for key: " + key;
                    }
                    
                    List<String> elements = redisTemplate.opsForList().range(key, 0, size - 1);
                    StringBuilder result = new StringBuilder("List contents for key: " + key + "\n");
                    for (int i = 0; i < elements.size(); i++) {
                        result.append(i).append(": ").append(elements.get(i)).append("\n");
                    }
                    return result.toString();
                }
            }
            else if (org.springframework.data.redis.connection.DataType.SET.equals(dataType)) {
                // Set type
                Set<String> members = redisTemplate.opsForSet().members(key);
                if (members == null || members.isEmpty()) {
                    return "Set is empty for key: " + key;
                }
                
                StringBuilder result = new StringBuilder("Set contents for key: " + key + "\n");
                for (String member : members) {
                    result.append(member).append("\n");
                }
                return result.toString();
            }
            else if (org.springframework.data.redis.connection.DataType.ZSET.equals(dataType)) {
                // Sorted Set type
                Object memberObj = args.get("member");
                if (memberObj != null) {
                    // Get score of specific member
                    String member = memberObj.toString();
                    Double score = redisTemplate.opsForZSet().score(key, member);
                    if (score == null) {
                        return "Member not found in sorted set: " + member;
                    }
                    return "Score of '" + member + "': " + score;
                } else {
                    // Get all members with scores
                    Set<ZSetOperations.TypedTuple<String>> tuples = redisTemplate.opsForZSet().rangeWithScores(key, 0, -1);
                    if (tuples == null || tuples.isEmpty()) {
                        return "Sorted set is empty for key: " + key;
                    }
                    
                    StringBuilder result = new StringBuilder("Sorted set contents for key: " + key + "\n");
                    for (ZSetOperations.TypedTuple<String> tuple : tuples) {
                        result.append(tuple.getValue()).append(": ").append(tuple.getScore()).append("\n");
                    }
                    return result.toString();
                }
            }
            else if (org.springframework.data.redis.connection.DataType.HASH.equals(dataType)) {
                // Hash type
                Object fieldObj = args.get("field");
                if (fieldObj != null) {
                    // Get specific field
                    String field = fieldObj.toString();
                    if (!StringUtils.hasText(field)) {
                        return "Error: Empty field provided for hash operation";
                    }
                    
                    String hashValue = redisTemplate.opsForHash().get(key, field) != null ? 
                                     redisTemplate.opsForHash().get(key, field).toString() : null;
                                     
                    if (hashValue == null) {
                        return "Hash field not found: " + field + " in key: " + key;
                    }
                    return hashValue;
                } else {
                    // Get all hash entries
                    Map<Object, Object> entries = redisTemplate.opsForHash().entries(key);
                    if (entries.isEmpty()) {
                        return "Hash is empty for key: " + key;
                    }
                    
                    StringBuilder result = new StringBuilder("Hash contents for key: " + key + "\n");
                    for (Map.Entry<Object, Object> entry : entries.entrySet()) {
                        result.append(entry.getKey()).append(": ")
                              .append(entry.getValue()).append("\n");
                    }
                    return result.toString();
                }
            }
            else if (org.springframework.data.redis.connection.DataType.STREAM.equals(dataType)) {
                // Stream type
                Object countObj = args.get("count");
                int count = 10; // Default count
                if (countObj != null) {
                    try {
                        count = Integer.parseInt(countObj.toString());
                    } catch (NumberFormatException e) {
                        return "Error: Invalid count format. Must be an integer";
                    }
                }
                
                List<MapRecord<String, Object, Object>> records = redisTemplate.opsForStream().read(
                    StreamReadOptions.empty().count(count),
                    StreamOffset.fromStart(key)
                );
                
                if (records == null || records.isEmpty()) {
                    return "Stream is empty or no records found for key: " + key;
                }
                
                StringBuilder result = new StringBuilder("Stream contents for key: " + key + "\n");
                for (MapRecord<String, Object, Object> record : records) {
                    result.append("ID: ").append(record.getId()).append("\n");
                    result.append("Values: ").append(record.getValue()).append("\n\n");
                }
                return result.toString();
            }
            else {
                return "Unsupported Redis data type for key: " + key + " (Type: " + dataType + ")";
            }
        } catch (IOException e) {
            return "Invalid JSON format: " + e.getMessage().split(":")[0];
        } catch (Exception e) {
            return "Operation failed: " + e.getMessage();
        }
    }

    /**
     * Delete one or multiple keys from Redis, or delete specific elements from complex data types
     * @param jsonArgs JSON string containing parameters:
     *                - key: String or List - The key(s) to delete
     *                - field: String - For hash type, the field to delete
     *                - member: String - For set/zset type, the member to delete
     *                - index: Integer - For list type, the index to delete
     *                - count: Integer - For list type with value, the count of occurrences to remove
     *                - value: String - For list type with count, the value to remove
     *                - id: String - For stream type, the message ID to delete
     * @return Operation result message
     */
    @Tool(name = "delete", description = "Delete Redis keys or specific elements from complex data types (string, list, set, zset, hash, stream)")
    public String deleteValue(String jsonArgs) {
        try {
            Map<String, Object> args = objectMapper.readValue(jsonArgs, Map.class);
            Object keyObj = args.get("key");
            
            if (keyObj == null) {
                return "Error: 'key' parameter is required";
            }
            
            // Handle multiple keys deletion
            if (keyObj instanceof List) {
                List<String> keys = ((List<?>) keyObj).stream()
                    .filter(k -> k != null)
                    .map(Object::toString)
                    .collect(Collectors.toList());
                
                if (keys.isEmpty()) {
                    return "Error: No valid keys provided";
                }
                
                Long deletedCount = redisTemplate.delete(keys);
                return "Successfully deleted " + deletedCount + " keys";
            } 
            
            // Handle single key operations
            String key = keyObj.toString();
            if (!StringUtils.hasText(key)) {
                return "Error: Empty key provided";
            }
            
            // Check if key exists
            if (!Boolean.TRUE.equals(redisTemplate.hasKey(key))) {
                return "Key not found: " + key;
            }
            
            // Check if we're deleting a specific element from a complex data type
            Object fieldObj = args.get("field");
            Object memberObj = args.get("member");
            Object indexObj = args.get("index");
            Object valueObj = args.get("value");
            Object countObj = args.get("count");
            Object idObj = args.get("id");
            
            // If no specific element parameters, delete the entire key
            if (fieldObj == null && memberObj == null && indexObj == null && 
                (valueObj == null || countObj == null) && idObj == null) {
                Boolean deleted = redisTemplate.delete(key);
                return deleted ? "Successfully deleted key: " + key : "Failed to delete key: " + key;
            }
            
            // Get the type of the key
            org.springframework.data.redis.connection.DataType dataType = redisTemplate.type(key);
            
            // Handle different data types
            if (org.springframework.data.redis.connection.DataType.HASH.equals(dataType) && fieldObj != null) {
                // Delete hash field
                String field = fieldObj.toString();
                Long removed = redisTemplate.opsForHash().delete(key, field);
                return removed > 0 ? 
                       "Successfully deleted field '" + field + "' from hash: " + key : 
                       "Field not found in hash: " + field;
            } 
            else if (org.springframework.data.redis.connection.DataType.SET.equals(dataType) && memberObj != null) {
                // Remove member from set
                String member = memberObj.toString();
                Boolean removed = redisTemplate.opsForSet().remove(key, member) > 0;
                return removed ? 
                       "Successfully removed member '" + member + "' from set: " + key : 
                       "Member not found in set: " + member;
            } 
            else if (org.springframework.data.redis.connection.DataType.ZSET.equals(dataType) && memberObj != null) {
                // Remove member from sorted set
                String member = memberObj.toString();
                Boolean removed = redisTemplate.opsForZSet().remove(key, member) > 0;
                return removed ? 
                       "Successfully removed member '" + member + "' from sorted set: " + key : 
                       "Member not found in sorted set: " + member;
            } 
            else if (org.springframework.data.redis.connection.DataType.LIST.equals(dataType)) {
                if (indexObj != null) {
                    // Remove element at specific index by setting it to a placeholder and then removing the placeholder
                    try {
                        int index = Integer.parseInt(indexObj.toString());
                        Long size = redisTemplate.opsForList().size(key);
                        
                        if (size == null || index < 0 || index >= size) {
                            return "Error: Index out of range";
                        }
                        
                        // Use a temporary placeholder value
                        String placeholder = "__DELETED__" + System.currentTimeMillis();
                        redisTemplate.opsForList().set(key, index, placeholder);
                        redisTemplate.opsForList().remove(key, 1, placeholder);
                        
                        return "Successfully removed element at index " + index + " from list: " + key;
                    } catch (NumberFormatException e) {
                        return "Error: Invalid index format. Must be an integer";
                    }
                } 
                else if (valueObj != null && countObj != null) {
                    // Remove count occurrences of value
                    String value = valueObj.toString();
                    try {
                        int count = Integer.parseInt(countObj.toString());
                        Long removed = redisTemplate.opsForList().remove(key, count, value);
                        return "Successfully removed " + removed + " occurrence(s) of '" + value + "' from list: " + key;
                    } catch (NumberFormatException e) {
                        return "Error: Invalid count format. Must be an integer";
                    }
                }
                else {
                    return "Error: For list operations, provide either 'index' or both 'value' and 'count'";
                }
            } 
            else if (org.springframework.data.redis.connection.DataType.STREAM.equals(dataType) && idObj != null) {
                // Delete message from stream
                String id = idObj.toString();
                Long removed = redisTemplate.opsForStream().delete(key, id);
                return removed > 0 ? 
                       "Successfully deleted message with ID '" + id + "' from stream: " + key : 
                       "Message ID not found in stream: " + id;
            } 
            else {
                // Fallback to deleting the entire key
                Boolean deleted = redisTemplate.delete(key);
                return deleted ? 
                       "Successfully deleted key: " + key : 
                       "Failed to delete key: " + key;
            }
        } catch (IOException e) {
            return "Invalid JSON format: " + e.getMessage().split(":")[0];
        } catch (Exception e) {
            return "Operation failed: " + e.getMessage();
        }
    }

    /**
     * List Redis keys matching a pattern using SCAN command for better performance
     * @param jsonArgs JSON string containing optional pattern and batchSize
     * @return List of matching keys or error message
     */
    @Tool(name = "list", description = "List Redis keys matching a pattern")
    public String listKeys(String jsonArgs) {
        try {
            Map<String, Object> args = new HashMap<>();
            if (StringUtils.hasLength(jsonArgs)) {
                args = objectMapper.readValue(jsonArgs, Map.class);
            }
            String pattern = (String) args.getOrDefault("pattern", "*");
            Integer batchSize = (Integer) args.getOrDefault("batchSize", 100);
            
            // Use ScanOptions to configure the SCAN operation
            ScanOptions options = ScanOptions.scanOptions()
                    .match(pattern)
                    .count(batchSize)
                    .build();
            
            // Use SCAN instead of KEYS for better performance with large datasets
            List<String> keys = new ArrayList<>();
            redisTemplate.execute((RedisCallback<Void>) connection -> {
                try (Cursor<byte[]> cursor = connection.scan(options)) {
                    while (cursor.hasNext()) {
                        keys.add(new String(cursor.next()));
                    }
                } catch (Exception e) {
                    throw new RuntimeException("Error scanning Redis keys: " + e.getMessage(), e);
                }
                return null;
            });
            
            return keys.size() > 0 ? "Found keys:\n" + String.join("\n", keys) : "No keys found matching the pattern";
        } catch (IOException e) {
            return "Error parsing JSON arguments: " + e.getMessage();
        } catch (Exception e) {
            return "Operation failed: " + e.getMessage();
        }
    }
}