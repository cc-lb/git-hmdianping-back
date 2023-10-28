package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.Voucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IVoucherService;
import com.hmdp.utils.RedisIdworker;
import com.hmdp.utils.SimpleRedisLock;
import com.hmdp.utils.UserHolder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author  cc
 * @since 2023-6-24
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {


    @Resource
    ISeckillVoucherService seckillVoucherService;
    @Resource
    RedisIdworker redisIdworker;
    @Resource
    StringRedisTemplate stringRedisTemplate;
   @Resource
   private RedissonClient redissonClient;




   static DefaultRedisScript<Long> SKILL_SCRIPT;
     static {
        SKILL_SCRIPT=new DefaultRedisScript<>();
       SKILL_SCRIPT.setLocation(new ClassPathResource("order02.lua"));

       SKILL_SCRIPT.setResultType(Long.class);
    }

    private static  final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();
  //  private   BlockingQueue<VoucherOrder> blockingQueue=new ArrayBlockingQueue(1024*1024);

    @PostConstruct
    void init(){
        SECKILL_ORDER_EXECUTOR.submit(new Handler());

    }

   private   class Handler implements Runnable{
       //  @SneakyThrows
         @Override
         public void run() {
             while(true){

                 try {
                   //  VoucherOrder voucherOrder = (VoucherOrder) blockingQueue.take();
                //重点来了，redis的阻塞队列

                     //1. 取出消息队列的消息（订单）
                     //2. 处理订单
                     //3，ACK
                     //4.异常处理


                     //1. 取出消息队列的消息（订单）
                     // xredagroup
                     List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream()
                             .read(Consumer.from("g1", "c1")
                                     , StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2))
                                     , StreamOffset.create("stream.orders", ReadOffset.lastConsumed()));
                    //ReadOffset.lastConsumed()这里取>符号,读取消息队列中，下一个未消费的消息开始
                     // 判断订单信息是否为空
                     if(list==null||list.isEmpty()) continue;
                     log.info("队列消息不为空"+list.toString());
                     // 解析数据
                     MapRecord<String, Object, Object> record = list.get(0);
                     Map<Object, Object> value = record.getValue();
                     VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);

                    //2. 处理订单
                     handleVoucherOrder(voucherOrder);
                     //3，ACK
                     stringRedisTemplate.opsForStream().acknowledge("s1", "g1", record.getId());
                 } catch ( Exception e) {
                     log.error("处理订单异常："+e);
                     //4.异常处理
                     handlePendingList();
                 }

             }
         }
     }
    private void handlePendingList() {
        while(true){
            //1.取出消费组中的异常数据
            //2.空则无异常消息
            //3.再次处理消息
            //4.ACK
            //5.异常：循环

                String key="stream.orders";
            try {
                //1.取出消费组中的异常数据

                List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream()
                        .read(Consumer.from("g1", "c1")
                                , StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2))
                                , StreamOffset.create("stream.orders", ReadOffset.from("0")));
                //ReadOffset.lastConsumed()这里取0符号,读取消费组中异常消息中，第一个消息开始


                //2.空则无异常消息
                if(list ==null) break;
                // 解析数据
                MapRecord<String, Object, Object> record = list.get(0);
                Map<Object, Object> value = record.getValue();
                VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);

                //3.再次处理消息
                handleVoucherOrder(voucherOrder);
                //4，ACK
                stringRedisTemplate.opsForStream().acknowledge("stream.orders", "g1", record.getId());
            } catch (Exception e) {
                //5.异常：循环
                log.error("处理pendding订单异常", e);
                try{
                    Thread.sleep(20);
                }catch(Exception w){
                    w.printStackTrace();
                }
            }


        }
    }
        private void handleVoucherOrder(VoucherOrder voucherOrder){
            Long userId = voucherOrder.getUserId();
            RLock redisLock = redissonClient.getLock("lock:order:" + userId);
            boolean isLock = redisLock.tryLock();
            try {


                if (!isLock) {
                    // 获取锁失败，直接返回失败或者重试
                    log.error("不允许重复下单！");
                    return;
                }
                //注意：由于是spring的事务是放在threadLocal中，此时的是多线程，事务会失效
                //所以在将proxy变成成员变量，在主线程初始化，这里直接调用
                proxy.onePersonAndVoucher(voucherOrder);
            } catch (Exception e) {
                log.error(e+"");
            } finally {
                redisLock.unlock();
            }
        }



    //最终版本，利用redission解决一人一单和超卖问题
    //将订单和数据库增改分离化，优惠券下单问题由串行解决改变为异步

    private IVoucherOrderService proxy;
    @Override
    public Result seckillVocher(Long voucherId) {
        //1.查询优惠券
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);

        //2.看优惠券活动是否开始或结束
        if (voucher.getBeginTime().isAfter(LocalDateTime.now()))
            return Result.fail("活动尚未开始。。。");
        if (voucher.getEndTime().isBefore(LocalDateTime.now()))
            return Result.fail("活动已经结束。。");

        Long voucherId1 = voucher.getVoucherId();
        Long userId = UserHolder.getUser().getId();
        long orderId = redisIdworker.nextId("order");

        // 1.执行lua脚本
        Long execute = stringRedisTemplate.execute(SKILL_SCRIPT, Collections.emptyList()
                , voucherId.toString(), userId.toString(),String.valueOf(orderId));
        int r = execute.intValue();
        // 2.判断结果是否为0
        if (r != 0) {
            // 2.1.不为0 ，代表没有购买资格
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }
//        VoucherOrder voucherOrder = new VoucherOrder();
//       // long orderId = redisIdworker.nextId("order");
//        voucherOrder.setId(orderId);
//        voucherOrder.setVoucherId(voucherId);
//        voucherOrder.setUserId(UserHolder.getUser().getId());
      //  blockingQueue.add(voucherOrder);
         proxy = (IVoucherOrderService) AopContext.currentProxy();



        return Result.ok();
    }

    @Transactional
    @Override
    public void onePersonAndVoucher(VoucherOrder voucherOrder){



        //一人一单问题
        Long userId = voucherOrder.getUserId();
        Long voucherId = voucherOrder.getVoucherId();

        // 5.1.查询订单
        int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();

        if(count>0) log.error("用户已购买一次");


        //超卖问题和扣减库存
        boolean update = seckillVoucherService.update()
                .setSql("stock=stock-1")//set stock=stock-1
                .eq("voucher_id", voucherId)//where id=?
                .gt("stock",0)//and stack=?，防止超卖
                .update();

       if(!update) log.error("库存不足");


        boolean save = save(voucherOrder);
        if(!save)  log.error("订单生成失败");


       // return Result.ok(orderId);



    }
    /**
     *
     * 防止了超卖问题（乐观锁）， 和一人一单（悲观锁）
     * 使用秒杀优惠券
     * @param voucherId
     * @return
     */


    /*
    //版本2：解决一人一单问题和超卖问题
    @Override
    public Result seckillVocher(Long voucherId) {
        //1.查询优惠券
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);

        //2.看优惠券活动是否开始或结束
        if (voucher.getBeginTime().isAfter(LocalDateTime.now()))
            return Result.fail("活动尚未开始。。。");
        if (voucher.getEndTime().isBefore(LocalDateTime.now()))
            return Result.fail("活动已经结束。。");

        //3.扣除库存优惠券

        // 关于使用字段的可移植性？？？字段名会变BlueUtil.columnToUnderline(Voucher::getStock)
        //setSQL可以用lambda表达式。
        // 例如：
        // voucherService.update(new LambdaUpdateWrapper<Voucher>()
        //                .setSql()
        //                .eq(Voucher::getStock,"stock"))

        //一人一单问题 加超卖问题


        // 单体锁
        //Long userId = UserHolder.getUser().getId();
                 //关于上锁问题，上锁上的是每一个对象,
              // 如何为每一个用户上锁，当然是用户id，
        // 让用户id相同的为同一个对象，这就设计到String的创建原理了
        //intern()返回规范表示，就是去字符串常量池里面找原始字符串
       // synchronized (userId.toString().intern()){

            //这里是获取代理对象，事务是由spring代理AOP调用，自己调用方法是没有事物的，为了使用方法时有事务
            // ，
         //   IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
         //   return proxy.onePersonAndVoucher(voucherId);
       // }


        //分布式锁
        Long userId
                = UserHolder.getUser().getId();
        SimpleRedisLock simpleRedisLock = new SimpleRedisLock(stringRedisTemplate,
                "order:" + userId);
        Boolean isLock = simpleRedisLock.tryLock(1200);

            try {
                if (isLock) {
                    IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
                    return proxy.onePersonAndVoucher(voucherId);
                }
            } catch (IllegalStateException e) {
                e.printStackTrace();
            } finally {
                simpleRedisLock.unLock();
            }
        if (!isLock) return Result.fail("不允许重复下单");

            return Result.fail("下单失败");
        }






//     * 1.优惠券的售卖每人一张，判断了此人未买过才售卖，因此数据库售卖-1的操作应在未买过的条件下
//     * 也就是说加锁应持续到数据库-1操作之后。
//     *
//     *2. 同时事务也应在锁的范围内
//     * @param voucherId
//     * @return

    @Transactional
    public Result onePersonAndVoucher(Long voucherId){



        //一人一单问题
        Long userId = UserHolder.getUser().getId();

        // 5.1.查询订单
        int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();

        if(count>0) return Result.fail("用户已购买一次");


        //超卖问题和扣减库存
        boolean update = seckillVoucherService.update()
                .setSql("stock=stock-1")//set stock=stock-1
                .eq("voucher_id", voucherId)//where id=?
                .gt("stock",0)//and stack=?，防止超卖
                .update();

        if(!update) Result.fail("库存不足");

        //4.生成订单
        VoucherOrder voucherOrder = new VoucherOrder();
        long orderId = redisIdworker.nextId("order");
        voucherOrder.setId(orderId);
        voucherOrder.setVoucherId(voucherId);
        voucherOrder.setUserId(UserHolder.getUser().getId());
      boolean save = save(voucherOrder);
       if(!save) return Result.fail("订单生成失败");


        return Result.ok(orderId);



    }

*/




    /*
    //版本1：解决超卖问题吗，未解决一人一单问题
    public Result seckillVocher(Long voucherId) {
        //1.查询优惠券
  SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        //2.看优惠券活动是否开始或结束
        if(voucher.getBeginTime().isAfter(LocalDateTime.now()))
            return Result.fail("活动尚未开始。。。");
        if (voucher.getEndTime().isBefore(LocalDateTime.now()))
            return Result.fail("活动已经结束。。");

        //3.扣除库存优惠券

       // 关于使用字段的可移植性？？？字段名会变BlueUtil.columnToUnderline(Voucher::getStock)
         //setSQL可以用lambda表达式。
        // 例如：
            // voucherService.update(new LambdaUpdateWrapper<Voucher>()
        //                .setSql()
        //                .eq(Voucher::getStock,"stock"))

        //一人一单问题尚未解决

        //超卖问题
        boolean update =  seckillVoucherService.update()
                .setSql("stock=stock-1")//set stock=stock-1
                .eq("voucher_id", voucherId)//where id=?
                .gt("stock",0)//and stack=?
                .update();

        if(!update) Result.fail("库存不足");

        //4.生成订单
        VoucherOrder voucherOrder = new VoucherOrder();
        long orderId = redisIdworker.nextId("order");
        voucherOrder.setId(orderId);
        voucherOrder.setVoucherId(voucherId);
        voucherOrder.setUserId(UserHolder.getUser().getId());

        return Result.ok(orderId);

    }
    */

}
