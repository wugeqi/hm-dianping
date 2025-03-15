package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

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

    @Override
    public Result queryById(Long id) {
        //1.从redis查缓存
        String shopJson=stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY+id);

        //2.判断是否存在
        if(StrUtil.isNotBlank(shopJson)){
            Shop shop= JSONUtil.toBean(shopJson,Shop.class);
            return Result.ok(shop);
        }//如果缓存不为空的话，直接返回店铺信息
        //如果有这个缓存，但是为空
        if(shopJson!=null){
            return Result.fail("店铺信息不存在");
        }


        //3.存在是否返回


        //4.不存在，根据id查数据库
        Shop shop=getById(id);

        //5.不存在错误
        if(shop==null){
            stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY+id,"",CACHE_NULL_TTL,TimeUnit.MINUTES);
            return Result.fail("店铺信息不存在");
        }


        //6.存在，写入redis
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY+id,JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);
        return Result.ok(shop);
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
