package com.redis;

import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RAtomicLongReactive;
import org.redisson.api.RedissonReactiveClient;
import reactor.core.publisher.Flux;

import java.time.Duration;

@Slf4j
public class NumberBucket {
    private static final RedissionConfig redissionConfig = new RedissionConfig();
    private static final RedissonReactiveClient client = redissionConfig.getReactiveClient();

    public static void main(String[] args) {
        r1();
    }

    private static void r1() {
        RAtomicLongReactive atomicLong = client.getAtomicLong("user:1:visit");
        Flux.range(1, 30)
                .delayElements(Duration.ofSeconds(1))
                .flatMap(i -> atomicLong.incrementAndGet())
                .then()
                .doOnTerminate(() -> redissionConfig.getClient().shutdown())
                .subscribe();
    }
}
