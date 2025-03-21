package com.hmdp.utils;

import cn.hutool.core.lang.UUID;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;


import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class SimpleRedisLock implements ILock {
    private StringRedisTemplate stringRedisTemplate;
    private String name;

    public SimpleRedisLock( String name,StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
        this.name = name;
    }
    private static final String KEY_PREFIX = "lock:";
    private static final String ID_PREFIX = UUID.randomUUID().toString(true)+"-";
    private static final DefaultRedisScript<Long> UNLOCK_SCIPT;
    static {
        UNLOCK_SCIPT = new DefaultRedisScript<>();
        UNLOCK_SCIPT.setLocation(new ClassPathResource("unlock.lua"));
        UNLOCK_SCIPT.setResultType(Long.class);
    }


    @Override
    public boolean tryLock(long timeoutSec){
        String threadID = ID_PREFIX+Thread.currentThread().getId();
        Boolean success=stringRedisTemplate.opsForValue()
                .setIfAbsent(KEY_PREFIX+name,threadID+"",timeoutSec, TimeUnit.SECONDS);


        return Boolean.TRUE.equals(success);

    }

    @Override
    public void unlock(){
//        String threadID = ID_PREFIX+Thread.currentThread().getId();
//        String id=stringRedisTemplate.opsForValue().get(KEY_PREFIX+name);
//        if(threadID.equals(id)){
//            stringRedisTemplate.delete(KEY_PREFIX+name);
//        }
        stringRedisTemplate.execute(
                UNLOCK_SCIPT,
                Collections.singletonList(KEY_PREFIX+name),
                ID_PREFIX+Thread.currentThread().getId()
        );

    }

}
