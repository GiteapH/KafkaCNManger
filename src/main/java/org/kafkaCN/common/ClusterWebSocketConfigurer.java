package org.kafkaCN.common;

import org.kafkaCN.sockets.ClusterHander;
import org.kafkaCN.sockets.TopicHander;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

/**
 * @author 吕杨平
 * @version 1.0
 * @description: socket
 * @date 2023/10/13 13:47
 */
@Configuration
@EnableWebSocket
public class ClusterWebSocketConfigurer implements WebSocketConfigurer {

    @Autowired
    private TopicHander topicHander;

    @Autowired
    private ClusterHander clusterHander;
    @Bean
    public TaskScheduler taskScheduler() {
        ThreadPoolTaskScheduler scheduling = new ThreadPoolTaskScheduler();
        scheduling.setPoolSize(10);
        scheduling.initialize();
        return scheduling;
    }

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry
                .addHandler(topicHander, "topicMonitor")
                .addHandler(clusterHander,"clusterMonitor")
                //允许跨域
                .setAllowedOrigins("*");
    }
}