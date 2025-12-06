package com.redis.spring.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class FibService {

    // have a strategy for cache evict
    @Cacheable(value = "math:fib", key = "#index")
    public int getFib(int index){
        log.info("calculating fib for {}", index);
        return this.fib(index);
    }

    // PUT / POST / PATCH / DELETE
    @CacheEvict(value = "math:fib", key = "#index")
    public void clearCache(int index){
        log.info("clearing hash key");
    }

    //  @Scheduled(fixedRate = 10_000)
    @CacheEvict(value = "math:fib", allEntries = true)
    public void clearCache(){
        log.info("clearing all fib keys");
    }

    private int fib(int index){
        if(index < 2)
            return index;
        return fib(index - 1) + fib(index - 2);
    }

}
