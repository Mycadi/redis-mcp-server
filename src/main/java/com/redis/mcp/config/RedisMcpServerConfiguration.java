package com.redis.mcp.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/**
 * Configuration class for Redis MCP Server
 * @author yue9527
 */
@Configuration
public class RedisMcpServerConfiguration {

    /**
     * Creates a LettuceConnectionFactory bean for Redis connection
     * @return Configured LettuceConnectionFactory
     */
    @Bean
    public LettuceConnectionFactory redisConnectionFactory() {
        String redisUrl = System.getProperty("redis.url", "redis://localhost:6379");
        String authPart = redisUrl.contains("@") ? redisUrl.split("@")[0].replace("redis://", "") : "";
        String hostPortPart = redisUrl.contains("@") ? redisUrl.split("@")[1] : redisUrl.replace("redis://", "");
        
        String[] credentials = authPart.split(":");
        String host = hostPortPart.split(":")[0];
        int port = Integer.parseInt(hostPortPart.split(":")[1]);
        
        RedisStandaloneConfiguration config = new RedisStandaloneConfiguration(host, port);
        if (credentials.length > 1) {
            config.setPassword(credentials[1]);
        }
        return new LettuceConnectionFactory(config);
    }

    /**
     * Creates a StringRedisTemplate bean for Redis operations
     * @param redisConnectionFactory LettuceConnectionFactory instance
     * @return Configured StringRedisTemplate
     */
    @Bean
    public StringRedisTemplate redisTemplate(LettuceConnectionFactory redisConnectionFactory) {
        StringRedisTemplate template = new StringRedisTemplate();
        template.setConnectionFactory(redisConnectionFactory);
        template.setKeySerializer(new StringRedisSerializer());
        template.setValueSerializer(new StringRedisSerializer());
        return template;
    }

}