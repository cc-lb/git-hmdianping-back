package com.hmdp.config;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @Classname RedissonConfig
 * @Description
 * @Date 2023/9/27 12:39
 * @Created by cc
 */
@Configuration
public class RedissonConfig   {

    @Bean
    public RedissonClient redissonClient(){
        // 配置
        Config config = new Config();
        config.useSingleServer().setAddress("redis://192.168.6.166:6379")
                .setPassword("rootasd");
        // 创建RedissonClient对象
        return Redisson.create(config);
    }
}
