package com.hmdp.service.impl;

import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.ResourceTransactionManager;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TYPE_KEY;
import static com.hmdp.utils.RedisConstants.LOCK_SHOP_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Autowired
    private ResourceTransactionManager resourceTransactionManager;

    @Override
    public Result queryShopList() {
        //得到数据

        //查询缓存
        List<String> JsonList=stringRedisTemplate.opsForList().range(CACHE_SHOP_TYPE_KEY, 0, -1);



        //缓存有，输出
        if(!JsonList.isEmpty()){
            List<ShopType> typeList = new ArrayList<>();

            // 遍历 JsonList 中的每个 JSON 字符串，并将其反序列化为 ShopType 对象
            for (String json : JsonList) {
                ShopType shop = JSONUtil.toBean(json, ShopType.class); // 将每个 JSON 字符串转换为 ShopType 对象
                typeList.add(shop); // 将 ShopType 对象添加到 List 中
            }
            return Result.ok(typeList);

        }

        //缓存没有，查询数据库
        List<ShopType> typeList = query().orderByAsc("sort").list();


        //数据库没有，返回错误
        if(typeList.isEmpty()){
            return Result.fail("品类不存在");
        }

        List<String> cacheList = new ArrayList<>();
        for (ShopType shop : typeList) {
            String json = JSONUtil.toJsonStr(shop);  // 将 ShopType 转换为 JSON 字符串
            cacheList.add(json);  // 添加到缓存列表中
        }

// 将缓存写入 Redis
        stringRedisTemplate.opsForList().rightPushAll(CACHE_SHOP_TYPE_KEY, cacheList);
        //List<ShopType> typeList = typeService.query().orderByAsc("sort").list();
        return Result.ok(typeList);
    }
}
