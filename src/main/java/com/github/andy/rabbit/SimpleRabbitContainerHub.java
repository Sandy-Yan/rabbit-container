package com.github.andy.rabbit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @Author yan.s.g
 * @Date 2016年11月25日 下午3:01
 */
public class SimpleRabbitContainerHub {

    private static Logger LOG = LoggerFactory.getLogger(SimpleRabbitContainerHub.class);

    private static final String KEY_FORMAT = "{}-{}-{}";

    private static final ConcurrentHashMap<String, SimpleRabbitContainer> simpleRabbitContainerStore = new ConcurrentHashMap<String, SimpleRabbitContainer>();

    public static final SimpleRabbitContainer get(RabbitBaseKey rabbitBaseKey, RabbitConfigKey rabbitConfigKey) {
        String strKey = buildStringKey(rabbitBaseKey);
        SimpleRabbitContainer rabbitContainer = simpleRabbitContainerStore.get(strKey);
        if (rabbitContainer == null) {
            synchronized (simpleRabbitContainerStore) {
                rabbitContainer = simpleRabbitContainerStore.get(strKey);
                if (rabbitContainer == null) {
                    try {
                        ConnectionFactory connectionFactory = RabbitConnectionFactoryHub.getConnectionFactory(rabbitConfigKey);
                        SimpleRabbitContainer newRabbitContainer = new SimpleRabbitContainer(connectionFactory, rabbitBaseKey.getExchange(), rabbitBaseKey.getRoutingKey(), rabbitBaseKey.getQueue());
                        simpleRabbitContainerStore.putIfAbsent(strKey, newRabbitContainer);
                        rabbitContainer = simpleRabbitContainerStore.get(strKey);
                    } catch (Throwable e) {
                        LOG.error("create RabbitContainer error.", e);
                        throw new RuntimeException(e);
                    }
                }
            }
        }

        return rabbitContainer;
    }

    public static final SimpleRabbitContainer get(RabbitKey rabbitKey) {
        return get(rabbitKey, rabbitKey.getRabbitConfigKey());
    }

    public static final RabbitTemplate getRabbitTemplate(RabbitBaseKey rabbitBaseKey, RabbitConfigKey rabbitConfigKey) {
        SimpleRabbitContainer rabbitContainer = get(rabbitBaseKey, rabbitConfigKey);
        if (rabbitContainer != null) {
            return rabbitContainer.getRabbitTemplate();
        }
        return null;
    }

    private static final String buildStringKey(RabbitBaseKey rabbitBaseKey) {
        return String.format(KEY_FORMAT, rabbitBaseKey.getExchange(), rabbitBaseKey.getRoutingKey(), rabbitBaseKey.getQueue());
    }
}
