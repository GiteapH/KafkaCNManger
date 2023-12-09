package org.kafkaCN.sockets;

/**
 * @author 吕杨平
 * @version 1.0
 * @description: TODO
 * @date 2023/10/13 13:49
 */

import lombok.extern.slf4j.Slf4j;
import org.kafkaCN.Exception.ContainException;
import org.kafkaCN.utils.JMXMBean;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.BinaryMessage;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.AbstractWebSocketHandler;

import javax.management.remote.JMXConnector;
import java.util.HashMap;
import java.util.Map;

/**
 * ws消息处理类
 */
@Component
@Slf4j
public class TopicHander extends AbstractWebSocketHandler {

    private Map<String,String> encodeQuery(String query){
        HashMap<String,String> queryMap = new HashMap<>();
        for(String s:query.split("&")){
            String[] strs = s.split("=");
            queryMap.put(strs[0],strs[1]);
        }
        return queryMap;
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws Exception {
        log.info("建立ws连接");
        Map<String, String> queryMap = encodeQuery(session.getUri().getQuery());
        ClusterSessionManager.add(queryMap.get("host")+queryMap.get("port")+"?topic",session);
        try {
            JMXMBean.create(queryMap.get("host"), Integer.valueOf(queryMap.get("port")));
        } catch (ContainException ignored) {

        }
        log.info(ClusterSessionManager.SESSION_POOL.toString());
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
        log.info("发送文本消息");
        // 获得客户端传来的消息
        String payload = message.getPayload();
        
    }

    @Override
    protected void handleBinaryMessage(WebSocketSession session, BinaryMessage message) throws Exception {
        log.info("发送二进制消息");
    }

    @Override
    public void handleTransportError(WebSocketSession session, Throwable exception) throws Exception {
        log.error("异常处理");
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) throws Exception {
        Map<String, String> queryMap = encodeQuery(session.getUri().getQuery());
        ClusterSessionManager.removeAndClose(queryMap.get("host")+queryMap.get("port")+"?topic");
        JMXConnector remove = JMXMBean.mBeanServerConnectionMap.remove(queryMap.get("host") + queryMap.get("port"));
        remove.close();
    }
}