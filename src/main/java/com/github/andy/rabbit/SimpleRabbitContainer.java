package com.github.andy.rabbit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpConnectException;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.util.Map;

/**
 * @Author yan.s.g
 * @Date 2016年11月25日 下午3:01
 */
public class SimpleRabbitContainer {

    private static final Logger logger = LoggerFactory.getLogger(SimpleRabbitContainer.class);

    private String exchange;

    private String exchangeType = "direct";

    private boolean durable = true;

    private boolean autoDelete = false;

    private String routingKey;

    private String queue;

    private ConnectionFactory connectionFactory;

    private RabbitTemplate rabbitTemplate;

    public SimpleRabbitContainer(ConnectionFactory connectionFactory, String exchange, String routingKey, String queue) {
        this.connectionFactory = connectionFactory;
        this.exchange = exchange;
        this.routingKey = routingKey;
        this.queue = queue;
        try {
            // 初始化
            init();
        } catch (Throwable e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public RabbitTemplate getRabbitTemplate() {
        return rabbitTemplate;
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

            rabbitTemplate = new RabbitTemplate(connectionFactory);
            rabbitTemplate.setExchange(this.exchange);
            rabbitTemplate.setRoutingKey(this.routingKey);
            rabbitTemplate.setQueue(this.queue);

        } catch (AmqpConnectException e) {
            throw new RuntimeException("初始化消息队列失败", e);
        }
    }

}
