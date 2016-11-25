package com.github.andy.rabbit;

/**
 * @Author yan.s.g
 * @Date 2016年11月25日 下午3:01
 */
public interface RabbitConfigKey {

    public String getHost();

    public int getPort();

    public String getUsername();

    public String getPassword();

    public String getVirtualHost();
}
