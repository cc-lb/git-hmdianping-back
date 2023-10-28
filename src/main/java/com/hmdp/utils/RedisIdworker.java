package com.hmdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Repository;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * 生成全局ID，利用Redis自增
 * @Classname RedisIdworker
 * @Description
 * @Date 2023/6/23 20:14
 * @Created by cc
 */

@Repository
public class RedisIdworker {


    /**
     * 开始时间戳
     */
    private static final long BEGIN_TIMESTAMP = 1687478400L;

     /**
     * 序列号的位数
     */
    private static final int COUNT_BITS = 32;



  private  final StringRedisTemplate stringRedisTemplate;

    public RedisIdworker(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }
/*
    public RedisIdworker(StringRedisTemplate stringRedisTemplate) {

        this.stringRedisTemplate = stringRedisTemplate;
    }
*/
    /**
     * 生成全局Id：64位
     * 0（表示正数） 31位（时间戳） 32位（每一天）
     * @param keyPrefix
     * @return
     */
    public long nextId(String keyPrefix) {
        //31位
        LocalDateTime now = LocalDateTime.now();
        long epochSecond = now.toEpochSecond(ZoneOffset.UTC);
        long timeStamp=epochSecond-BEGIN_TIMESTAMP;

        //32位
        String date = now.format(DateTimeFormatter.ofPattern("yy:MM:dd"));
        Long count = stringRedisTemplate.opsForValue().increment("icr:" + keyPrefix + ":" + date);

        return timeStamp<<COUNT_BITS|count;

    }
    /*
    public static void main(String[] args){
        LocalDateTime time = LocalDateTime.of(2023, 6,23,0,0);
        System.out.println(time.toEpochSecond(ZoneOffset.UTC));


    }
    */

}
