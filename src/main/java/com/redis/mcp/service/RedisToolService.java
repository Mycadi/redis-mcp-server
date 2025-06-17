package com.redis.mcp.service;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

/**
 * Redis Tool Service for handling Redis operations
 * @author yue9527
 */
@Service
@Slf4j
public class RedisToolService {

    private final StringRedisTemplate redisTemplate;

    /**
     * Constructor for RedisToolService
     * @param redisTemplate StringRedisTemplate instance
     */
    public RedisToolService(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }
    
    /**
     * Parse JSON string to Map
     * @param jsonString JSON string to parse
     * @return Parsed Map object
     * @throws IOException If parsing fails
     */
    private Map<String, Object> parseJson(String jsonString) throws IOException {
        if (!StringUtils.hasLength(jsonString)) {
            return new HashMap<>();
        }
        try {
            return JSON.parseObject(jsonString);
        } catch (Exception e) {
            log.error("Error parsing JSON: {}", jsonString, e);
            throw new IOException("Invalid JSON format: " + e.getMessage());
        }
    }

    /**
     * Set a key-value pair in Redis with optional expiration time
     * @param jsonArgs JSON string containing key, value and optional expireSeconds
     * @return Operation result message
     */
    @Tool(name = "set", description = "Set a Redis key-value pair with optional expiration time")
    public String setValue(String jsonArgs) {
        long startTime = System.currentTimeMillis();
        try {
            Map<String, Object> args = parseJson(jsonArgs);
            String key = (String) args.get("key");
            String value = (String) args.get("value");
            Integer expireSeconds = (Integer) args.get("expireSeconds");
            
            if (!StringUtils.hasText(key)) {
                return "Error: 'key' parameter is required";
            }
            
            if (!StringUtils.hasText(value)) {
                return "Error: 'value' parameter is required";
            }
            
            // Use Redis connection directly for better performance
            redisTemplate.execute((RedisConnection connection) -> {
                byte[] keyBytes = key.getBytes();
                byte[] valueBytes = value.getBytes();
                connection.stringCommands().set(keyBytes, valueBytes);
                
                if (expireSeconds != null && expireSeconds > 0) {
                    connection.expire(keyBytes, expireSeconds);
                }
                return null;
            });
            
            long duration = System.currentTimeMillis() - startTime;
            log.debug("Set operation for key '{}' completed in {} ms", key, duration);
            return "Successfully set key: " + key;
        } catch (IOException e) {
            log.error("Error in setValue operation: {}", e.getMessage(), e);
            return "Error parsing JSON arguments: " + e.getMessage();
        } catch (Exception e) {
            log.error("Unexpected error in setValue operation: {}", e.getMessage(), e);
            return "Operation failed: " + e.getMessage();
        }
    }

    /**
     * Get value from Redis by key
     * @param jsonArgs JSON string containing the key
     * @return Retrieved value or error message
     */
    @Tool(name = "get", description = "Get value from Redis by key")
    public String getValue(String jsonArgs) {
        long startTime = System.currentTimeMillis();
        try {
            Map<String, Object> args = parseJson(jsonArgs);
            String key = (String) args.get("key");
            
            if (!StringUtils.hasText(key)) {
                return "Error: 'key' parameter is required";
            }
            
            // Use Redis connection directly for better performance
            String result = redisTemplate.execute((RedisConnection connection) -> {
                byte[] valueBytes = connection.stringCommands().get(key.getBytes());
                return valueBytes != null ? new String(valueBytes) : null;
            });
            
            long duration = System.currentTimeMillis() - startTime;
            log.debug("Get operation for key '{}' completed in {} ms", key, duration);
            return result != null ? result : "Key not found: " + key;
        } catch (IOException e) {
            log.error("Error in getValue operation: {}", e.getMessage(), e);
            return "Invalid JSON format: " + e.getMessage().split(":")[0];
        } catch (Exception e) {
            log.error("Unexpected error in getValue operation: {}", e.getMessage(), e);
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
        long startTime = System.currentTimeMillis();
        try {
            Map<String, Object> args = parseJson(jsonArgs);
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
                
                // Use pipeline for batch operations
                Long deletedCount = redisTemplate.execute((RedisConnection connection) -> {
                    connection.openPipeline();
                    int count = 0;
                    for (String key : keys) {
                        if (connection.keyCommands().del(key.getBytes()) > 0) {
                            count++;
                        }
                    }
                    connection.closePipeline();
                    return (long) count;
                });
                
                long duration = System.currentTimeMillis() - startTime;
                log.debug("Batch delete operation for {} keys completed in {} ms", keys.size(), duration);
                return "Successfully deleted " + deletedCount + " keys";
            } else {
                String key = keyObj.toString();
                if (!StringUtils.hasText(key)) {
                    return "Error: Empty key provided";
                }
                
                Boolean deleted = redisTemplate.execute((RedisConnection connection) -> 
                    connection.keyCommands().del(key.getBytes()) > 0);
                
                long duration = System.currentTimeMillis() - startTime;
                log.debug("Delete operation for key '{}' completed in {} ms", key, duration);
                return deleted ? "Successfully deleted key: " + key : "Key not found: " + key;
            }
        } catch (IOException e) {
            log.error("Error in deleteValue operation: {}", e.getMessage(), e);
            return "Invalid JSON format: " + e.getMessage().split(":")[0];
        } catch (Exception e) {
            log.error("Unexpected error in deleteValue operation: {}", e.getMessage(), e);
            return "Operation failed: " + e.getMessage();
        }
    }

    /**
     * List Redis keys matching a pattern
     * @param jsonArgs JSON string containing optional pattern
     * @return List of matching keys or error message
     */
    @Tool(name = "list", description = "List Redis keys matching a pattern")
    public String listKeys(String jsonArgs) {
        long startTime = System.currentTimeMillis();
        try {
            Map<String, Object> args = new HashMap<>();
            if (StringUtils.hasLength(jsonArgs)) {
                args = parseJson(jsonArgs);
            }
            String pattern = (String) args.getOrDefault("pattern", "*");
            Integer limit = (Integer) args.getOrDefault("limit", 1000); // Add limit to prevent large result sets
            
            // Use scan command instead of keys for better performance on large databases
            List<String> keys = redisTemplate.execute((RedisConnection connection) -> {
                List<String> result = new java.util.ArrayList<>();
                org.springframework.data.redis.core.ScanOptions options = 
                    org.springframework.data.redis.core.ScanOptions.scanOptions().match(pattern).count(limit).build();
                try (org.springframework.data.redis.core.Cursor<byte[]> cursor = 
                        connection.keyCommands().scan(options)) {
                    while (cursor.hasNext() && result.size() < limit) {
                        result.add(new String(cursor.next()));
                    }
                }
                return result;
            });
            
            long duration = System.currentTimeMillis() - startTime;
            log.debug("List keys operation with pattern '{}' completed in {} ms, found {} keys", 
                    pattern, duration, keys.size());
            
            return keys.size() > 0 ? 
                "Found keys" + (keys.size() >= limit ? " (limited to " + limit + ")" : "") + ":\n" + 
                String.join("\n", keys) : 
                "No keys found matching the pattern";
        } catch (IOException e) {
            log.error("Error in listKeys operation: {}", e.getMessage(), e);
            return "Error parsing JSON arguments: " + e.getMessage();
        } catch (Exception e) {
            log.error("Unexpected error in listKeys operation: {}", e.getMessage(), e);
            return "Operation failed: " + e.getMessage();
        }
    }
    
    /**
     * Get information about Redis server
     * @param jsonArgs JSON string (not used)
     * @return Redis server information
     */
    @Tool(name = "info", description = "Get Redis server information")
    public String getInfo(String jsonArgs) {
        long startTime = System.currentTimeMillis();
        try {
            String info = redisTemplate.execute((RedisConnection connection) -> {
                Map<String, String> infoMap = new HashMap<>();
                Properties props = connection.info();
                StringBuilder result = new StringBuilder();
                
                // Add key server stats
                result.append("Redis Server Information:\n");
                for (String key : new String[] {"redis_version", "uptime_in_seconds", "connected_clients", 
                                              "used_memory_human", "total_connections_received"}) {
                    if (props.containsKey(key)) {
                        result.append(key).append(": ").append(props.getProperty(key)).append("\n");
                    }
                }
                
                // Add database stats
                result.append("\nDatabase Stats:\n");
                Long dbSize = connection.dbSize();
                result.append("Keys count: ").append(dbSize);
                
                return result.toString();
            });
            
            long duration = System.currentTimeMillis() - startTime;
            log.debug("Info operation completed in {} ms", duration);
            
            return info;
        } catch (Exception e) {
            log.error("Error in getInfo operation: {}", e.getMessage(), e);
            return "Failed to get Redis info: " + e.getMessage();
        }
    }
}