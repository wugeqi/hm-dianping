package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIdWorker redisIdworker;
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedissonClient redissonClient;

    private static final DefaultRedisScript<Long> SECKILL_SCIPT;
    static {
        SECKILL_SCIPT = new DefaultRedisScript<>();
        SECKILL_SCIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCIPT.setResultType(Long.class);
    }
    //private BlockingQueue<VoucherOrder> orderTask=new ArrayBlockingQueue<>(1024*1024);
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();
    @PostConstruct
    public void init(){
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());

    }
    private class VoucherOrderHandler implements Runnable{
        String queueName = "stream.orders";
        @Override
        public void run() {
            while (true){
                try {
                    //1. 获取消息队列中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS streams.order >
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );
                    //2. 判断消息获取是否成功
                    if (list == null || list.isEmpty()){
                        //2.1 如果获取失败，说明没有消息，继续下一次循环
                        continue;
                    }
                    //3. 解析消息中的订单信息
                    MapRecord<String,Object,Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    //3. 如果获取成功，可以下单
                    createVoucherOrder(voucherOrder);
                    //2. ACK确认  SACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",record.getId());
                } catch (Exception e) {
                    log.error("处理订单异常",e);
                    handlePendingList();
                }
            }
        }

        private void handlePendingList() {
            while (true){
                try {
                    //1. 获取pending-list中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1  STREAMS streams.order 0
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queueName, ReadOffset.from("0"))
                    );
                    //2. 判断消息获取是否成功
                    if (list == null || list.isEmpty()){
                        //2.1 如果获取失败，说明pending-list没有消息，结束循环
                        break;
                    }
                    //3. 解析消息中的订单信息
                    MapRecord<String,Object,Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    //3. 如果获取成功，可以下单
                    createVoucherOrder(voucherOrder);
                    //2. ACK确认  SACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",record.getId());
                } catch (Exception e) {
                    log.error("处理pending-list订单异常",e);
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException interruptedException) {
                        interruptedException.printStackTrace();
                    }
                }
            }
        }
    }

    //    private class VoucherOrderHandler implements Runnable {
//        @Override
//        public void run() {
//            while(true){
//                try{
//                    VoucherOrder voucherOrder=orderTask.take();
//                    handleVoucherOrder(voucherOrder);
//                }catch(Exception e){
//                    log.error("处理订单异常",e);
//                }
//            }
//        }
//    }
//    private void handleVoucherOrder(VoucherOrder voucherOrder){
//        Long useId=voucherOrder.getVoucherId();
//        RLock lock=redissonClient.getLock("lock:order:"+useId);
//        boolean isLock=lock.tryLock();
//        if(!isLock){
//            log.error("不允许重复下单");
//            return ;
//        }
//        try{
//            proxy.createVoucherOrder(voucherOrder);
//
//
//        }
//        finally {
//            lock.unlock();
//
//        }
//    }
//    private IVoucherOrderService proxy;
    @Override
    public Result seckillVoucher(Long voucherId) {
        Long userId=UserHolder.getUser().getId();
        long orderId=redisIdworker.nextId("order");
        Long result=stringRedisTemplate.execute(
                SECKILL_SCIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString(),
                String.valueOf(orderId)
        );
        int r=result.intValue();
        if(r!=0){
            return Result.fail(r==1?"库存不足":"不能重复下单");

        }
//        VoucherOrder voucherOrder = new VoucherOrder();
//        long orderId=redisIdworker.nextId("order");
//        voucherOrder.setId(orderId);
//        //Long userId = UserHolder.getUser().getId();
//        voucherOrder.setUserId(userId);
//        voucherOrder.setVoucherId(voucherId);
//        orderTask.add(voucherOrder);
       //proxy=(IVoucherOrderService)AopContext.currentProxy();




        return Result.ok(orderId);
    }

//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
//        if(voucher == null){
//            return Result.fail("秒杀券不存在，请联系管理员处理");
//        }
//        if(voucher.getBeginTime().isAfter(LocalDateTime.now())){
//            return Result.fail("秒杀还未开始");
//        }
//        if(voucher.getEndTime().isBefore(LocalDateTime.now())){
//            return Result.fail("秒杀已结束");
//        }
//        if(voucher.getStock()<1){
//            return Result.fail("库存不足！");
//
//        }
//        Long userId=UserHolder.getUser().getId();
//       // synchronized (userId.toString().intern()) {
//        //SimpleRedisLock lock=new SimpleRedisLock("order:"+userId,stringRedisTemplate);
//        RLock lock=redissonClient.getLock("lock:order:"+userId);
//
//        boolean isLock=lock.tryLock();
//        if(!isLock){
//            return Result.fail("不能重复下单！");
//        }
//
//
//        try {
//            IVoucherOrderService proxy=(IVoucherOrderService) AopContext.currentProxy();
//            return proxy.createVoucherOrder(voucherId);
//            //}
//        } catch (IllegalStateException e) {
//            throw new RuntimeException(e);
//        } finally {
//            lock.unlock();
//        }
//    }
    @Transactional

    public void createVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        Long voucherId = voucherOrder.getVoucherId();
        // 创建锁对象
        RLock redisLock = redissonClient.getLock("lock:order:" + userId);
        // 尝试获取锁
        boolean isLock = redisLock.tryLock();
        // 判断
        if (!isLock) {
            // 获取锁失败，直接返回失败或者重试
            log.error("不允许重复下单！");
            return;
        }

        try {
            // 5.1.查询订单
            int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
            // 5.2.判断是否存在
            if (count > 0) {
                // 用户已经购买过了
                log.error("不允许重复下单！");
                return;
            }

            // 6.扣减库存
            boolean success = seckillVoucherService.update()
                    .setSql("stock = stock - 1") // set stock = stock - 1
                    .eq("voucher_id", voucherId).gt("stock", 0) // where id = ? and stock > 0
                    .update();
            if (!success) {
                // 扣减失败
                log.error("库存不足！");
                return;
            }

            // 7.创建订单
            save(voucherOrder);
        } finally {
            // 释放锁
            redisLock.unlock();
        }



    }
}
