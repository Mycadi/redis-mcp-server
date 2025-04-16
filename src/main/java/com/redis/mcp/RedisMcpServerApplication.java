package com.redis.mcp;

import com.redis.mcp.service.RedisToolService;
import org.springframework.ai.tool.ToolCallbackProvider;
import org.springframework.ai.tool.method.MethodToolCallbackProvider;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

/**
 * Main application class for Redis MCP Server
 * @author yue9527
 */
@SpringBootApplication
public class RedisMcpServerApplication {

    /**
     * Main method to start the Spring Boot application
     * @param args Command line arguments
     */
    public static void main(String[] args) {
        SpringApplication.run(RedisMcpServerApplication.class, args);
    }

    /**
     * Creates a ToolCallbackProvider bean for Redis tools
     * @param redisToolService RedisToolService instance
     * @return Configured ToolCallbackProvider
     */
    @Bean
    public ToolCallbackProvider redisTools(RedisToolService redisToolService) {
        return MethodToolCallbackProvider.builder().toolObjects(redisToolService).build();
    }

}
