package com.hmdp.utils;

import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @Classname SimpleRedisLock
 * @Description
 * @Date 2023/9/20 12:06
 * @Created by cc
 */
public class SimpleRedisLock {

    StringRedisTemplate stringRedisTemplate;
    String name;
    private static final String LOCK_PREFIX="lock:";
    static DefaultRedisScript<Long> redisScript;

    static {
       redisScript = new DefaultRedisScript<>();
        redisScript.setLocation(new ClassPathResource("unlock.lua"));
        redisScript.setResultType(Long.class);
    }


    public SimpleRedisLock(StringRedisTemplate stringRedisTemplate, String name){
        this.stringRedisTemplate=stringRedisTemplate;
        this.name=name;
    }

   public Boolean tryLock(long timeout){
        long threadId = Thread.currentThread().getId();
        String key=LOCK_PREFIX+name;

        Boolean bool = stringRedisTemplate.opsForValue().setIfAbsent(key, threadId+ "", timeout, TimeUnit.SECONDS);
        return Boolean.TRUE.equals(bool);

    }

    /*
     //不保证原子性
  public   void unLock( ){
        //防止误删
        long threadId= Thread.currentThread().getId();
        String value= stringRedisTemplate.opsForValue().get(LOCK_PREFIX + name);
        if(value.equals(threadId+"")) stringRedisTemplate.delete(LOCK_PREFIX+name);

    }

     */


    public void unLock(){
        long threadId = Thread.currentThread().getId();
        List<String> key = Collections.singletonList(LOCK_PREFIX + name);
        stringRedisTemplate.execute(redisScript, key,threadId+"");
    }

}
