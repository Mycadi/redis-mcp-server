package com.redis.mcp.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;

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
     * @param jsonArgs JSON string containing the key
     * @return Retrieved value or error message
     */
    @Tool(name = "get", description = "Get value from Redis by key")
    public String getValue(String jsonArgs) {
        try {
            Map<String, Object> args = objectMapper.readValue(jsonArgs, Map.class);
            String key = (String) args.get("key");
            
            if (!StringUtils.hasText(key)) {
                return "Error: 'key' parameter is required";
            }
            
            String result = redisTemplate.opsForValue().get(key);
            return result != null ? result : "Key not found: " + key;
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