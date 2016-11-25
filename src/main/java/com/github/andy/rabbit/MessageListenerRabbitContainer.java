package com.github.andy.rabbit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpConnectException;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;

import java.util.Map;

/**
 * @Author yan.s.g
 * @Date 2016年11月25日 下午3:01
 */
public class MessageListenerRabbitContainer {

    private static final Logger logger = LoggerFactory.getLogger(MessageListenerRabbitContainer.class);

    private String exchange;

    private String exchangeType = "direct";

    private boolean durable = true;

    private boolean autoDelete = false;

    private String routingKey;

    private String queue;

    private ConnectionFactory connectionFactory;

    private SimpleMessageListenerContainer simpleMessageListenerContainer;

    private MessageListener messageListener;

    private boolean autoStart = true;

    private AcknowledgeMode acknowledgeMode = AcknowledgeMode.AUTO;

    public MessageListenerRabbitContainer(ConnectionFactory connectionFactory, String exchange, String routingKey, String queue, MessageListener messageListener, boolean autoStart) {
        this.connectionFactory = connectionFactory;
        this.exchange = exchange;
        this.routingKey = routingKey;
        this.queue = queue;
        this.messageListener = messageListener;
        this.autoStart = autoStart;
        try {
            // 初始化
            init();
        } catch (Throwable e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public String getExchange() {
        return exchange;
    }

    public String getExchangeType() {
        return exchangeType;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    public String getQueue() {
        return queue;
    }

    public boolean isDurable() {
        return durable;
    }

    public boolean isAutoDelete() {
        return autoDelete;
    }

    public ConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    public MessageListener getMessageListener() {
        return messageListener;
    }

    private void init() throws Exception {
        Exchange newExchage = new Exchange() {
            @Override
            public String getName() {
                return exchange;
            }

            @Override
            public String getType() {
                return exchangeType;
            }

            @Override
            public boolean isDurable() {
                return durable;
            }

            @Override
            public boolean isAutoDelete() {
                return autoDelete;
            }

            @Override
            public Map<String, Object> getArguments() {
                return null;
            }
        };
        try {
            RabbitAdmin rabbitAdmin = new RabbitAdmin(connectionFactory);

            rabbitAdmin.declareExchange(newExchage);

            rabbitAdmin.declareQueue(new Queue(queue));

            rabbitAdmin.declareBinding(new Binding(queue, Binding.DestinationType.QUEUE, exchange, routingKey, null));
        } catch (AmqpConnectException e) {
            throw new RuntimeException("初始化消息队列失败", e);
        }

        simpleMessageListenerContainer = new SimpleMessageListenerContainer(connectionFactory);
        simpleMessageListenerContainer.setQueues(new Queue(queue));
        simpleMessageListenerContainer.setMessageListener(messageListener);
        simpleMessageListenerContainer.setAcknowledgeMode(acknowledgeMode);
        if (autoStart) {
            simpleMessageListenerContainer.start();
        }
    }

    public boolean isAutoStart() {
        return autoStart;
    }

    public void start() {
        simpleMessageListenerContainer.start();
    }

    public AcknowledgeMode getAcknowledgeMode() {
        return acknowledgeMode;
    }

}
