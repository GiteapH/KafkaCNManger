package org.kafkaCN.service;

import org.springframework.web.socket.WebSocketSession;

public interface WSService {
    void sendMes(String clusterId,String text);
}
