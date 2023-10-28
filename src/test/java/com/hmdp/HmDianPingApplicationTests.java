package com.hmdp;

import com.hmdp.entity.Shop;
import com.hmdp.service.IShopService;
import com.hmdp.utils.RedisIdworker;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoLocation;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.SHOP_GEO_KEY;

@SpringBootTest
class HmDianPingApplicationTests {

    @Resource
    StringRedisTemplate stringRedisTemplate;
    @Resource
    RedisIdworker redisIdworker;
    @Resource
    IShopService shopService;

      ExecutorService pool = Executors.newFixedThreadPool(500);

    @Test
    public void testIdWorker() throws InterruptedException {
        System.out.println(stringRedisTemplate);
        System.out.println(redisIdworker);
        CountDownLatch latch = new CountDownLatch(300);

        long begin = System.currentTimeMillis();
        for(int i=0;i<300;i++){
            pool.submit(()->{
                for(int j=0;j<100;j++){
                    long order = redisIdworker.nextId("order");
                    System.out.println(order);
                }
                latch.countDown();
            });
        }
        latch.await();
        long end
                = System.currentTimeMillis();
        System.out.println("时间："+(end-begin));


    }

    /**
     * 载入商铺信息到redis中
     * 提取商铺信息
     * stream流分类（商铺类型）
     * 以类型id为key，存入相同类型id的商铺的位置信息
     */
    @Test
    void loadShop(){
        List<Shop> shopsList= shopService.list();
        Map<Long, List<Shop>> shopsMap = shopsList.stream()
                .collect(Collectors.groupingBy(Shop::getTypeId));

    for(Map.Entry<Long,List<Shop>> entry:shopsMap.entrySet()){
        List<Shop> shops = entry.getValue();

        List<RedisGeoCommands.GeoLocation<String>> locations = new ArrayList<>(shops.size());
        Long typeId = entry.getKey();
        String key = SHOP_GEO_KEY + typeId;
        for(Shop shop:shops) {
             locations.add(new RedisGeoCommands.GeoLocation<>(shop.getId().toString()
             ,new Point(shop.getX(), shop.getY())));
        }

        //这里add使用了迭代器
        stringRedisTemplate.opsForGeo()
                .add(key,  locations);


    }


    }


}
