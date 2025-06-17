package com.redis.mcp.config;

import java.time.Duration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import lombok.extern.slf4j.Slf4j;

/**
 * Configuration class for Redis MCP Server
 * Uses Spring Boot's auto-configuration for Redis
 * @author yue9527
 */
@Configuration
@Slf4j
public class RedisMcpServerConfiguration {

    @Value("${spring.data.redis.host:localhost}")
    private String redisHost;
    
    @Value("${spring.data.redis.port:6379}")
    private int redisPort;
    
    /**
     * Creates a StringRedisTemplate bean for Redis operations
     * Uses auto-configured RedisConnectionFactory
     * @param redisConnectionFactory Auto-configured RedisConnectionFactory instance
     * @return Configured StringRedisTemplate
     */

    @Bean
    public StringRedisTemplate redisTemplate(RedisConnectionFactory redisConnectionFactory) {
        StringRedisTemplate template = new StringRedisTemplate();
        template.setConnectionFactory(redisConnectionFactory);
        template.setKeySerializer(new StringRedisSerializer());
        template.setValueSerializer(new StringRedisSerializer());
        template.setHashKeySerializer(new StringRedisSerializer());
        template.setHashValueSerializer(new StringRedisSerializer());
        template.setEnableTransactionSupport(false); // Disable transactions for better performance
        template.afterPropertiesSet();
        
        log.info("Configured Redis connection to {}:{} with optimized template settings", redisHost, redisPort);
        return template;
    }

}