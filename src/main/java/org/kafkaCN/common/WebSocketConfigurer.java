package org.kafkaCN.common;

import org.apache.catalina.Cluster;
import org.kafkaCN.sockets.ClusterHander;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
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
public class WebSocketConfig implements WebSocketConfigurer {

    @Autowired
    private ClusterHander clusterHander;

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry
                .addHandler(clusterHander, "cluster")
                //允许跨域
                .setAllowedOrigins("*");
    }
}