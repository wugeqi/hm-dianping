package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;
import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;

@Component
@Slf4j
public class CacheClient {
    private final StringRedisTemplate stringRedisTemplate;
    public CacheClient(StringRedisTemplate stringRedisTemplate){
        this.stringRedisTemplate = stringRedisTemplate;
    }
    public void set(String key, Object value, Long time , TimeUnit timeUnit){
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, timeUnit);

    }
    public void setWithLogicalExpire(String key, Object value, Long time , TimeUnit timeUnit){

        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(timeUnit.toSeconds(time)));
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    public <R,ID> R queryWithPassThrough(String keyPrefix, ID id, Class<R> type, Function<ID,R> dbFallback, Long time, TimeUnit unit){
        String key = keyPrefix + id.toString();
        String json=stringRedisTemplate.opsForValue().get(key);
        if(StrUtil.isNotBlank(json)){
            return JSONUtil.toBean(json,type);
        }
        if(json!=null){
            return null;
        }
        R r=dbFallback.apply(id);
        if(r==null){
            stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL,TimeUnit.MINUTES);
            return null;
        }
        this.set(key,r,time,unit);
       // stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(r),time,unit);
        return r;
    }


    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);
    //这里不考虑缓存穿透
    public <R,ID> R queryWithLogicalExpire(String keyPrefix,ID id,Class<R> type, Function<ID,R> dbFallback, Long time, TimeUnit unit) {
        //1.从redis查缓存
        String key = keyPrefix + id.toString();
        String Json=stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY+id);

        //2.判断是否存在
        if(StrUtil.isBlank(Json)){
            //Shop shop= JSONUtil.toBean(shopJson,Shop.class);
            return null;
        }//如果缓存不为空的话，直接返回店铺信息
        RedisData redisData=JSONUtil.toBean(Json,RedisData.class);
        R r=JSONUtil.toBean((JSONObject) redisData.getData(),type);
        LocalDateTime expireTime=redisData.getExpireTime();
        if(expireTime.isAfter(LocalDateTime.now())){
            return r;
        }
        String lockKey=LOCK_SHOP_KEY+id;
        boolean isLock=tryLockShop(lockKey);
        if(isLock){

            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    Thread.sleep(100);
                    R r1=dbFallback.apply(id);
                    //this.saveShop2Redis(id,30L);
                    this.setWithLogicalExpire(key,r1,time,unit);

                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    tryUnLockShop(lockKey);
                }
            });

        }
        return r;





    }

    private boolean tryLockShop(String key){
        Boolean flag=stringRedisTemplate.opsForValue().setIfAbsent(key,"1",10,TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }
    private boolean tryUnLockShop(String key){
        Boolean flag=stringRedisTemplate.delete(key);
        return BooleanUtil.isTrue(flag);
    }


}
