package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheCilent;
import com.hmdp.utils.RedisData;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.*;
import static com.hmdp.utils.SystemConstants.DEFAULT_PAGE_SIZE;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author cc
 * @since 2023-6-23
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

        @Resource
    StringRedisTemplate stringRedisTemplate;

        @Resource
    CacheCilent cacheCilent;
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);



    /**
     * 解决商户信息的缓存问题：
     * 1.缓存穿透：
     * 两种思路：缓存空对象，布隆过滤
     * 2.缓存雪崩：
     * 两种思路：key值设置随机值
     * 3.缓存击穿：
     * 两种思路：互斥锁，逻辑过期
     *
     * @param id
     * @return
     */
    @Override
    public Result queryById(Long id) {
         //   Result result=null;
       //  result=queryWithPassThrough(id);//防止内存穿透

         Result   result=queryWithMutex(id);//防止缓存击穿BY互斥锁
       //Result result=queryWithLogicalExpireTime(id);  //防止缓存击穿BY逻辑过期


  //关于防止缓存击穿，从事实依据上有一点不同。同样是解决热点key问题，逻辑过期需要手动将热点key加入缓存

        //封装上面方法为工具类
        /*
        Shop shop=cacheCilent.queryWithPassThrough(CACHE_SHOP_KEY, id, Shop.class
                ,  this::getById
               , CACHE_SHOP_TTL, TimeUnit.MINUTES);
        */


       /*
        Shop shop=cacheCilent.queryWithMutex( CACHE_SHOP_KEY  ,id, Shop.class
                ,  this::getById
                , CACHE_SHOP_TTL, TimeUnit.MINUTES);
        */
        //if(shop==null) Result.fail("商户不存在。。");
        if(result==null) Result.fail("商户不存在。。");


        return Result.ok();

    }




    /**
     * 防止缓存穿透
     * 当查询数据时，从缓存中查取，有则返回，无则返回失误，还需判断命中值为为空（就是防止缓存穿透）
     * 如果缓存中没有去数据库中查找。
     * 数据库中没有则根据查询token为缓存添加数据，“”。
     *  StrUtil.isBlank()//可检验：null  ，“”  ，“   ”。
     * @param id
     * @return
     */
    @Transactional
    public Result queryWithPassThrough(Long id){


        String key=CACHE_SHOP_KEY+id;
        //存储hash数据结构更好，为什么存String类型
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        Shop shop=null;
        if(!StrUtil.isBlank(shopJson)){
            shop = JSONUtil.toBean(shopJson, Shop.class);
            return  Result.ok(shop);
        }

        //判断命中值是否为空
        if(shopJson!=null) {
            //return Result.fail("商户不存在");//缓存穿透
            return null;
        }

            shop = getById(id);

            //(int)Math.random()*6
            if(shop!=null)
                stringRedisTemplate.opsForValue()
                        .set(key,JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL+(int)Math.random()*6,TimeUnit.MINUTES);
            else {
                stringRedisTemplate.opsForValue()
                        .set(key, "", CACHE_NULL_TTL + (int) Math.random() * 6, TimeUnit.MINUTES);
return null;
            }


        return  Result.ok(shop);

    }


    /**
     * 缓存击穿：互斥锁
     *
     * 互斥锁，利用redis中setnx命令完成。setnx无法设置已存在的值，再次设置时会返回0
     * ps:在为缓存添加数据时，应该再次检查缓存中是否存在数据。双重检查，多线程。
     * @param id
     * @return
     */
    @Transactional
    public Result queryWithMutex(Long id){


         String key=CACHE_SHOP_KEY+id;
        //存储hash数据结构更好，为什么存String类型
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        Shop shop=null;
        if(!StrUtil.isBlank(shopJson)){
            shop = JSONUtil.toBean(shopJson, Shop.class);
            return  Result.ok(shop);
        }

        //判断命中值是否为空
        if(shopJson!=null) {
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
                shopJson = stringRedisTemplate.opsForValue().get(key);
                shop=null;
                if(!StrUtil.isBlank(shopJson)){
                    shop = JSONUtil.toBean(shopJson, Shop.class);
                    return  Result.ok(shop);
                }
                //判断命中值是否为空
                if(shopJson!=null) {
                    // return Result.fail("商户不存在");//缓存穿透
                    return null;
                }


            }



            //    2.   递归的上锁过程
            /*
            if(! (isLock=tyLock(lockKey))){
                  // 失败，则休眠重试
                Thread.sleep(50);
                return queryWithMutex(id);
            }*/




               // 锁获取完成后再次查看缓存，查看其他线程是否已修改值
            shopJson = stringRedisTemplate.opsForValue().get(key);
            shop=null;
            if(!StrUtil.isBlank(shopJson)){
                shop = JSONUtil.toBean(shopJson, Shop.class);
                return  Result.ok(shop);
            }
            //判断命中值是否为空
            if(shopJson!=null) {
                // return Result.fail("商户不存在");//缓存穿透
                return null;
            }



            shop = getById(id);

            //(int)Math.random()*6
            if(shop!=null) {
                 stringRedisTemplate.opsForValue()
                        .set(key, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL + (int) Math.random() * 6, TimeUnit.MINUTES);
            }
            else {
                stringRedisTemplate.opsForValue()
                        .set(key, "", CACHE_NULL_TTL + (int) Math.random() * 6, TimeUnit.MINUTES);
                        return null;
            }
            // unLock(lockKey);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {

                unLock(lockKey);
        }


        return  Result.ok(shop);

    }





    /**
     *
     *
     * 思路：
     * 防止缓存击穿：逻辑过期
     * 存储信息设置了一个逻辑过期时间。
     * 在原有的防缓存穿透基础上，如果我们查询缓存信息，如果缓存逻辑时间过期了
     * 单独分出一个子线程去更新缓存，原有线程直接返回过期线程。
     *
     *
     * 具体应用场景：热点key问题，应用于特殊节日的某些特价商品，
     *              在缓存中提前加入商品信息以待使用，未加入的商品便不是特价商品。
     *
     *
     * 问题是：子线程如何更新缓存信息。
     *
     */
    @Transactional
    public Result queryWithLogicalExpireTime( Long id ) {
        String key=CACHE_SHOP_KEY+id;
        //存储hash数据结构更好，为什么存String类型
        String json = stringRedisTemplate.opsForValue().get(key);




        //存在
        if(StrUtil.isBlank(json)){
            return null;
         }

        //存在
       // if(StrUtil.isBlank(json)&&json!=null) return null;



        //缓存中得到数据
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        JSONObject jsonObject = (JSONObject) redisData.getData();
        Shop shop = JSONUtil.toBean(jsonObject, Shop.class);
        LocalDateTime expireTime = redisData.getExpireTime();

        //是否过期
        if(expireTime.isAfter(LocalDateTime.now())) return Result.ok(shop);


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
                shop = JSONUtil.toBean(jsonObject, Shop.class);
                expireTime = redisData.getExpireTime();
                if(expireTime.isAfter(LocalDateTime.now())) return Result.ok(shop);


                CACHE_REBUILD_EXECUTOR.submit(()->
                {
                this.resetRedisData(id, 20L);
                });

            }

            // return Result.ok(stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY+id))
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            unLock(lockKey);
        }


        return  Result.ok(shop);
    }

    public void resetRedisData(Long id,long expireTime){
        Shop shop = getById(id);
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireTime));
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY+id, JSONUtil.toJsonStr(redisData));


    }




    private Boolean  tyLock(String key){

        //ifAbsent为redis中setnx方法
        Boolean ifAbsent = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10 + (int) Math.random() * 6, TimeUnit.SECONDS);

        return  BooleanUtil.isTrue(ifAbsent);
    }

    private  void unLock(String key){
        stringRedisTemplate.delete(key);
    }

    @Transactional
    @Override
    public Result update(Shop shop) {

        Long id = shop.getId();
        if (id==null) return Result.fail("商铺id不能为空");

        boolean updateById = updateById(shop);
        return  Result.ok();



    }


    /**
     * 查询离自己近的商铺
     * 利用redis中Geo数据结构查询，
     *
     *      // GEOSEARCH：在指定范围内搜索member，并按照与指定点之间的距离排序后返回。
     *       // 范围可以是圆形或矩形。6.2.新功能
     * @param typeId
     * @param current
     * @param x
     * @param y
     * @return
     */
    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
                    //1.如果不指定距离就从数据库中查询
        if(x==null||y==null){
            com.baomidou.mybatisplus.extension.plugins.pagination.Page<Shop> page =
                    new com.baomidou.mybatisplus.extension.plugins.pagination.Page<>(current, DEFAULT_PAGE_SIZE);
            Page<Shop> shopPage = query().eq("type_id", typeId)
                    .page(page);
            return Result.ok(shopPage.getRecords());

        }

        // GEOSEARCH：在指定范围内搜索member，并按照与指定点之间的距离排序后返回。但是只能从0开始查询，所以就不能分页了。
        // 范围可以是圆形或矩形。6.2.新功能
        String key=SHOP_GEO_KEY+typeId;

        //分页的数据
            Integer from=(current-1)*DEFAULT_PAGE_SIZE;
            Integer end=from+DEFAULT_PAGE_SIZE;


                    //2.从redis中取出数据
        // GEOSEARCH key BYLONLAT x y BYRADIUS 10 WITHDISTANCE   (这个查询不可以指定从哪里查，只能从0开始查)
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo()
                .search(key, GeoReference.fromCoordinate(x, y), new Distance(5000),

                        RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end));

        //RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end)
            //这段代码是 RedisGeoCommands 库中的一个方法，用于构造 Redis 的 GEOSEARCH 命令的参数对象。
            //
            //其中 newGeoSearchArgs() 创建了一个新的参数对象；includeDistance() 表示返回结果中包含每个位置点与中心点的距离信息； limit(end) 表示查询结果最多返回 end 个符合条件的位置点。
            //
            //综合来看，这段代码构造了一个带有距离信息和结果数量限制的 GEOSEARCH 命令参数对象。此参数可以用于向 Redis 发送 GEOSEARCH 命令以获取符合条件的位置点。

            //无数据返空
            if(results==null) return Result.ok(Collections.emptyList());


            //从redis中取出的数据，Geolocation=》member和位置信息

                    //3.分页
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> content = results.getContent().
                stream().skip(from).collect(Collectors.toList());
             //数据已经到底无法分页了
        if(content.size()<=from) return Result.ok(Collections.emptyList());


                    //4.从redis数据中获取店铺id和距离
       List<Long> ids = new ArrayList<>(content.size());
        Map<String, Distance> distanceMap = new HashMap<>();
        for(GeoResult<RedisGeoCommands.GeoLocation<String>> geoResult:content){
                    RedisGeoCommands.GeoLocation<String> geoLocation = geoResult.getContent();
            Long id = Long.valueOf(geoLocation.getName());
            ids.add(id);
            Distance distance = geoResult.getDistance();
            distanceMap.put(id.toString(),distance );
        }



        // 5.根据id查询Shop
        String idStr = StrUtil.join(",", ids);
        List<Shop> shops = query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list();
        for (Shop shop : shops) {
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }
        // 6.返回
        return Result.ok(shops);


    }
}
