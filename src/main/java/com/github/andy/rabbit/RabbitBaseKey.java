package com.github.andy.rabbit;

/**
 * @Author yan.s.g
 * @Date 2016年11月25日 下午3:01
 */
public interface RabbitBaseKey {

    public String getExchange();

    public String getRoutingKey();

    public String getQueue();

}
