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
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import org.springframework.boot.autoconfigure.cache.CacheProperties;

import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoSearchCommandArgs;

import org.springframework.data.redis.connection.RedisGeoCommands;

//import org.springframework.data.redis.connection.RedisGeoCommands.GeoSearchCommandArgs;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;


import javax.annotation.Resource;

import java.time.LocalDateTime;
import java.util.*;
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
    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        // 1.判断是否需要根据坐标查询
        if (x == null || y == null) {
            // 不需要坐标查询，按数据库查询
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            // 返回数据
            return Result.ok(page.getRecords());
        }

        // 2.计算分页参数
        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;

        // 3.查询redis、按照距离排序、分页。结果：shopId、distance
        String key = SHOP_GEO_KEY + typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo() // GEOSEARCH key BYLONLAT x y BYRADIUS 10 WITHDISTANCE
                .search(
                        key,
                        GeoReference.fromCoordinate(x, y),
                        new Distance(5000),
                        RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end)
                );
        // 4.解析出id
        if (results == null) {
            return Result.ok(Collections.emptyList());
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
        if (list.size() <= from) {
            // 没有下一页了，结束
            return Result.ok(Collections.emptyList());
        }
        // 4.1.截取 from ~ end的部分
        List<Long> ids = new ArrayList<>(list.size());
        Map<String, Distance> distanceMap = new HashMap<>(list.size());
        list.stream().skip(from).forEach(result -> {
            // 4.2.获取店铺id
            String shopIdStr = result.getContent().getName();
            ids.add(Long.valueOf(shopIdStr));
            // 4.3.获取距离
            Distance distance = result.getDistance();
            distanceMap.put(shopIdStr, distance);
        });
        // 5.根据id查询Shop
        String idStr = StrUtil.join(",", ids);
        List<Shop> shops = query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list();
        for (Shop shop : shops) {
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }
        // 6.返回
        return Result.ok(shops);
    }


//    @Override
//    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
//        //判断是否需要坐标查询
//        if (x == null || y == null) {
//            //不需要坐标查询
//            Page<Shop> page=lambdaQuery()
//                    .eq(Shop::getTypeId,typeId)
//                    .page(new Page<>(current,SystemConstants.MAX_PAGE_SIZE));
//            return Result.ok(page.getRecords());
//        }
//        //计算分页参数
//        int from = (current - 1) * SystemConstants.MAX_PAGE_SIZE;
//        int end = current * SystemConstants.MAX_PAGE_SIZE;
//        //查询redis 距离排序 分页
//        String key= SHOP_GEO_KEY+typeId;
//        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo()
//                .search(key
//                        , GeoReference.fromCoordinate(x, y)
//                        , new Distance(5000)
//                        ,RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end)
//                );
////        RedisGeoCommands.GeoSearchCommandArgs geoSearchArgs = RedisGeoCommands.GeoSearchCommandArgs
////                .newGeoSearchArgs()
////                .includeDistance()  // 包括距离
////                .limit(end);
////        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo()
////                .search(key, GeoReference.fromCoordinate(x, y), new RedisGeoCommands.Distance(5000, RedisGeoCommands.DistanceUnit.METERS), geoSearchArgs);
//        //解析出id
//        if (results==null){
//            return Result.ok(Collections.emptyList());
//        }
//        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> content = results.getContent();
//        if (content.size()<from){
//            //没有下一页
//            return Result.ok();
//        }
//        //截取
//        List<Long> ids=new ArrayList<>(content.size());
//        Map<String,Distance> distanceMap=new HashMap<>();
//        content.stream().skip(from).forEach(result->{
//            //店铺id
//            String shopId = result.getContent().getName();
//            ids.add(Long.valueOf(shopId));
//            //距离
//            Distance distance = result.getDistance();
//            distanceMap.put(shopId,distance);
//        });
//        //根据id查询shop
//        String join = StrUtil.join(",", ids);
//        List<Shop> shopList = lambdaQuery().in(Shop::getId, ids).last("order by field(id,"+join+")").list();
//        for (Shop shop : shopList) {
//            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
//        }
//        return Result.ok(shopList);
//
//    }
}