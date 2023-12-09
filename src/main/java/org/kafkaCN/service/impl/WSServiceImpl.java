package org.kafkaCN.service.impl;


import org.kafkaCN.service.WSService;
import org.kafkaCN.sockets.ClusterSessionManager;
import org.springframework.stereotype.Service;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import java.io.IOException;

/**
 * @author 吕杨平
 * @version 1.0
 * @description: cluster socket服务类
 * @date 2023/10/14 11:31
 */
@Service
public class WSServiceImpl implements WSService {

    @Override
    public void sendMes(String clusterId, String text) {
        if(ClusterSessionManager.SESSION_POOL.containsKey(clusterId)) {
            WebSocketSession webSocketSession = ClusterSessionManager.get(clusterId);
            try {
                webSocketSession.sendMessage(new TextMessage(text));
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
    }
}
