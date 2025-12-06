package com.redis;

import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RHyperLogLogReactive;
import org.redisson.api.RedissonReactiveClient;
import org.redisson.client.codec.LongCodec;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.stream.LongStream;

@Slf4j
public class RedisHyperLogLog {
    private static final RedissionConfig redissionConfig = new RedissionConfig();
    private static final RedissonReactiveClient client = redissionConfig.getReactiveClient();

    public static void main(String[] args) {
        RHyperLogLogReactive<Long> hyperLogLog = client.getHyperLogLog("user:visits", LongCodec.INSTANCE);
        List<Long> list = LongStream.rangeClosed(1, 25000)
                .boxed()
                .toList();
        List<Long> list2 = LongStream.rangeClosed(25001, 50000)
                .boxed()
                .toList();
        List<Long> list3 = LongStream.rangeClosed(1, 75000)
                .boxed()
                .toList();
        List<Long> list4 = LongStream.rangeClosed(50000, 100_000)
                .boxed()
                .toList();

        Flux.just(list, list2, list3, list4)
                .flatMap(hyperLogLog::addAll)
                .then(hyperLogLog.count()
                        .doOnNext(aLong -> log.info("Count: {}", aLong)))
                .subscribe();
    }
}
