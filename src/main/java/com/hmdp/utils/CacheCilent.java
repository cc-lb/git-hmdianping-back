package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;


import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;

/**
 * @Classname CacheCilent
 * @Description
 * @Date 2023/6/23 14:21
 * @Created by cc
 */

@Slf4j
@Component
public class CacheCilent {


    private final StringRedisTemplate stringRedisTemplate;

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    public CacheCilent(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }


    public void set(String key, Object value, Long time, TimeUnit unit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    public void setWithLogicalExpireTime(String key, Object value, Long time, TimeUnit unit) {
        // 设置逻辑过期
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        // 写入Redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }
    /**
     *    //设置缓存穿透工具类，
     *    函数式编程：既有参数，又有返回结果
     * @param prefixKey
     * @param id
     * @param type
     * @param function
     * @param <R>
     * @param <ID>
     * @return
     */
    public <R ,ID>R queryWithPassThrough(String prefixKey, ID id, Class<R>type, Function<ID,R> function
            , Long TTL,TimeUnit timeUnit){

        String key=prefixKey+id;
        //存储hash数据结构更好，为什么存String类型
        String json = stringRedisTemplate.opsForValue().get(key);
        R r=null;
        if(!StrUtil.isBlank(json)){
            r= JSONUtil.toBean(json, type);
            return  r;
        }

        //判断命中值是否为空
        if(json!=null) {
            //return Result.fail("商户不存在");//缓存穿透
            return null;
        }

       r=function.apply(id);

        //(int)Math.random()*6
        if(r!=null)
            stringRedisTemplate.opsForValue()
                    .set(key,JSONUtil.toJsonStr(r),  TTL+(int)Math.random()*6, timeUnit);
        else {
            stringRedisTemplate.opsForValue()
                    .set(key, "",  TTL + (int) Math.random() * 6, timeUnit);
            return null;
        }


        return  r;
    }



    // 设置缓存击穿工具类BY逻辑过期，
    public<R,ID>R queryWithLogicalExpireTime( String prefixKey,ID id,Class<R>type
            ,Function<ID,R> function,Long TTL,TimeUnit timeUnit) {
        String key=prefixKey+id;
        //存储hash数据结构更好，为什么存String类型
        String json = stringRedisTemplate.opsForValue().get(key);




        //存在
        if(StrUtil.isBlank(json)){
            return null;
        }

        //存在
        // if(StrUtil.isBlank(json)&&json!=null) return null;



        //缓存中得到数据
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);//工具类中的RedisData类有没有改呢
        JSONObject jsonObject = (JSONObject) redisData.getData();
        R r = JSONUtil.toBean(jsonObject, type);
        LocalDateTime expireTime = redisData.getExpireTime();

        //是否过期
        if(expireTime.isAfter(LocalDateTime.now())) return r;


        //过期了更新数据

        //上锁
        //如果得到锁成功则更改，没有则返回过期数据
        String lockKey = LOCK_SHOP_KEY + id;
        Boolean isLock=tyLock(lockKey);
        try {

            if(isLock) {


                //再次查看是否过期，其他线程有无已经修改
                redisData = JSONUtil.toBean(json, RedisData.class);
                jsonObject = (JSONObject) redisData.getData();
                r = JSONUtil.toBean(jsonObject, type);
                expireTime = redisData.getExpireTime();
                if(expireTime.isAfter(LocalDateTime.now())) return r;


                CACHE_REBUILD_EXECUTOR.submit(()->
                {
                    function.apply(id);
                });

            }

            // return Result.ok(stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY+id))
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            unLock(lockKey);
        }


        return r;
    }


    // 设置缓存击穿工具类BY互斥锁
    public <ID,R>R queryWithMutex(String prefixKey ,ID id,Class<R> type,Function<ID,R> function
            ,Long TTL,TimeUnit timeUnit){


        String key=prefixKey+id;
        //存储hash数据结构更好，为什么存String类型
        String json = stringRedisTemplate.opsForValue().get(key);
        R r=null;
        if(!StrUtil.isBlank(json)){
           r= JSONUtil.toBean(json,type);
            return  r;
        }

        //判断命中值是否为空
        if(json!=null) {
            // return Result.fail("商户不存在");//缓存穿透
            return null;
        }

        String lockKey=LOCK_SHOP_KEY+id;
        try {
            Boolean isLock=false;



            //上锁方法： 1. CAS的上锁过程
            while(true){
                isLock=tyLock(lockKey);
                if(isLock) break;
                Thread.sleep(100);

                //看其他线程是否已修改缓存
                json= stringRedisTemplate.opsForValue().get(key);
                r=null;
                if(!StrUtil.isBlank(json)){
                    r = JSONUtil.toBean(json, type);
                    return  r;
                }
                //判断命中值是否为空
                if(json!=null) {
                    // return Result.fail("商户不存在");//缓存穿透
                    return null;
                }


            }

            /*
            //    2.   递归的上锁过程
            if(!isLock){
                  // 失败，则休眠重试
                Thread.sleep(50);
                return queryWithMutex(id);
            }

            */


            // 锁获取完成后再次查看缓存，查看其他线程是否已修改值
            json = stringRedisTemplate.opsForValue().get(key);
            r=null;
            if(!StrUtil.isBlank(json)){
                r= JSONUtil.toBean(json, type);
                return  r;
            }
            //判断命中值是否为空
            if(json!=null) {
                // return Result.fail("商户不存在");//缓存穿透
                return null;
            }



            r=function.apply(id);

            //(int)Math.random()*6
            if(r!=null) {
                stringRedisTemplate.opsForValue()
                        .set(key, JSONUtil.toJsonStr(r),  TTL + (int) Math.random() * 6, timeUnit  );
            }
            else {
                stringRedisTemplate.opsForValue()
                        .set(key, "",  TTL + (int) Math.random() * 6, timeUnit) ;
                return null;
            }
            // unLock(lockKey);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {

            unLock(lockKey);
        }


        return r;
    }


    private Boolean  tyLock(String key){

        //ifAbsent为redis中setnx方法
        Boolean ifAbsent = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10 + (int) Math.random() * 6, TimeUnit.SECONDS);

         return  BooleanUtil.isTrue(ifAbsent);
    }

    private  void unLock(String key){
        stringRedisTemplate.delete(key);
    }




}
