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
     * @param jsonArgs JSON string containing key, value and optional expireSeconds
     * @return Operation result message
     */
    @Tool(name = "set", description = "Set a Redis key-value pair with optional expiration time")
    public String setValue(String jsonArgs) {
        try {
            Map<String, Object> args = objectMapper.readValue(jsonArgs, Map.class);
            String key = (String) args.get("key");
            String value = (String) args.get("value");
            Integer expireSeconds = (Integer) args.get("expireSeconds");
            if (expireSeconds != null) {
                redisTemplate.opsForValue().set(key, value, expireSeconds);
            } else {
                redisTemplate.opsForValue().set(key, value);
            }
            return "Successfully set key: " + key;
        } catch (IOException e) {
            return "Error parsing JSON arguments: " + e.getMessage();
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
     * Delete one or multiple keys from Redis
     * @param jsonArgs JSON string containing key(s) to delete
     * @return Operation result message
     */
    @Tool(name = "delete", description = "Delete one or multiple keys from Redis")
    public String deleteValue(String jsonArgs) {
        try {
            Map<String, Object> args = objectMapper.readValue(jsonArgs, Map.class);
            Object keyObj = args.get("key");
            
            if (keyObj == null) {
                return "Error: 'key' parameter is required";
            }
            
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
            } else {
                String key = keyObj.toString();
                if (!StringUtils.hasText(key)) {
                    return "Error: Empty key provided";
                }
                
                Boolean deleted = redisTemplate.delete(key);
                return deleted ? "Successfully deleted key: " + key : "Key not found: " + key;
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