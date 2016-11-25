package com.github.andy.rabbit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @Author yan.s.g
 * @Date 2016年11月25日 下午3:01
 */
public class RabbitConnectionFactoryHub {

    private static Logger LOG = LoggerFactory.getLogger(RabbitConnectionFactoryHub.class);

    private static final String KEY_FORMAT = "{}-{}-{}";

    private static final ConcurrentHashMap<String, ConnectionFactory> connectionFactoryStore = new ConcurrentHashMap<String, ConnectionFactory>();

    public static final ConnectionFactory getConnectionFactory(RabbitConfigKey rabbitConfigKey) {
        String strKey = buildStrKey(rabbitConfigKey);
        ConnectionFactory connectionFactory = connectionFactoryStore.get(strKey);
        if (connectionFactory == null) {
            synchronized (connectionFactoryStore) {
                connectionFactory = connectionFactoryStore.get(strKey);
                if (connectionFactory == null) {
                    try {
                        CachingConnectionFactory newConnectionFactory = new CachingConnectionFactory(rabbitConfigKey.getHost());
                        newConnectionFactory.setPort(rabbitConfigKey.getPort());
                        newConnectionFactory.setUsername(rabbitConfigKey.getUsername());
                        newConnectionFactory.setPassword(rabbitConfigKey.getPassword());
                        newConnectionFactory.setVirtualHost(rabbitConfigKey.getVirtualHost());
                        connectionFactoryStore.putIfAbsent(strKey, newConnectionFactory);
                        connectionFactory = connectionFactoryStore.get(strKey);
                    } catch (Throwable e) {
                        LOG.error("create RabbitConnectionFactory error.", e);
                        throw new RuntimeException(e);
                    }
                }
            }
        }

        return connectionFactory;
    }

    private static final String buildStrKey(RabbitConfigKey rabbitConfigKey) {
        return String.format(KEY_FORMAT, rabbitConfigKey.getHost(), rabbitConfigKey.getPort() + "", rabbitConfigKey.getVirtualHost());
    }
}
