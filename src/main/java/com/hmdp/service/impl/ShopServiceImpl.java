package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.RandomUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisData;
import org.springframework.boot.autoconfigure.cache.CacheProperties;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private CacheClient cacheClient;

    @Override
    public Result queryById(Long id) {
        //Shop shop = queryWithMutex(id);
        //Shop shop=queryWithLogicalExpire(id);
        Shop shop=cacheClient.queryWithLogicalExpire(CACHE_SHOP_KEY,id,Shop.class,this::getById,CACHE_SHOP_TTL,TimeUnit.SECONDS);
        if(shop==null){
            return Result.fail("店铺不存在");
        }
        return Result.ok(shop);
    }

    //@Override
    public Shop queryWithMutex(Long id) {
        //1.从redis查缓存
        String shopJson=stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY+id);

        //2.判断是否存在
        if(StrUtil.isNotBlank(shopJson)){
            //Shop shop= JSONUtil.toBean(shopJson,Shop.class);
            return JSONUtil.toBean(shopJson,Shop.class);
        }//如果缓存不为空的话，直接返回店铺信息
        //如果有这个缓存，但是为空
        if(shopJson!=null){
            return null;
        }


        //3.存在是否返回
        String lockKey=LOCK_SHOP_KEY+id;
        Shop shop= null;
        try {
            Boolean isLock=tryLockShop(lockKey);

            if(!isLock){
                Thread.sleep(50);
                return queryWithMutex(id);

            }

            //4.不存在，根据id查数据库
            shop = getById(id);


            //模拟延时
            Thread.sleep(200);
            Long cache_shop_ttl = RandomUtil.randomLong(30L);

            //5.不存在错误
            if(shop==null){
                stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY+id,"",CACHE_NULL_TTL,TimeUnit.MINUTES);
                return null;
            }


            //6.存在，写入redis
            stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY+id,JSONUtil.toJsonStr(shop),cache_shop_ttl, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            tryUnLockShop(lockKey);
        }
       // tryUnLockShop(lockKey);
        return shop;
    }

private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);
//这里不考虑缓存穿透
    public Shop queryWithLogicalExpire(Long id) {
        //1.从redis查缓存
        String shopJson=stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY+id);

        //2.判断是否存在
        if(StrUtil.isBlank(shopJson)){
            //Shop shop= JSONUtil.toBean(shopJson,Shop.class);
            return null;
        }//如果缓存不为空的话，直接返回店铺信息
        RedisData redisData=JSONUtil.toBean(shopJson,RedisData.class);
        Shop shop=JSONUtil.toBean((JSONObject) redisData.getData(),Shop.class);
       LocalDateTime expireTime=redisData.getExpireTime();
       if(expireTime.isAfter(LocalDateTime.now())){
           return shop;
       }
       String lockKey=LOCK_SHOP_KEY+id;
       boolean isLock=tryLockShop(lockKey);
       if(isLock){

           CACHE_REBUILD_EXECUTOR.submit(() -> {
               try {
                   Thread.sleep(100);
                   this.saveShop2Redis(id,30L);

               } catch (Exception e) {
                   throw new RuntimeException(e);
               } finally {
                   tryUnLockShop(lockKey);
               }
           });

       }
       return shop;





    }

    private boolean tryLockShop(String key){
        Boolean flag=stringRedisTemplate.opsForValue().setIfAbsent(key,"1",10,TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }
    private boolean tryUnLockShop(String key){
        Boolean flag=stringRedisTemplate.delete(key);
        return BooleanUtil.isTrue(flag);
    }
    public void saveShop2Redis(Long id,Long expirSeconds){
        Shop shop=getById(id);
        RedisData redisData=new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expirSeconds));
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY+id,JSONUtil.toJsonStr(redisData));

    }

    @Override
    public Result upadte(Shop shop) {
        //更新数据库
        Long id= shop.getId();
        if(id==null){
            return Result.fail("店铺id不能为空");
        }
        updateById(shop);



        //删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY+id);
        return Result.ok();
    }
}
