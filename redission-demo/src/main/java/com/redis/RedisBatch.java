package com.redis;

import lombok.extern.slf4j.Slf4j;
import org.redisson.api.*;
import org.redisson.client.codec.LongCodec;

@Slf4j
public class RedisBatch {
    private static final RedissionConfig redissionConfig = new RedissionConfig();
    private static final RedissonReactiveClient client = redissionConfig.getReactiveClient();

    public static void main(String[] args) {
        RBatchReactive batch = client.createBatch(BatchOptions.defaults());
        RListReactive<Long> list = batch.getList("number-list", LongCodec.INSTANCE);
        RSetReactive<Long> set = batch.getSet("number-set", LongCodec.INSTANCE);

        for (long i = 0; i < 500_000; i++) {
            list.add(i);
            set.add(i);
        }
        batch.execute()
                .doOnNext(e -> log.info("Completed"))
                .subscribe();
    }
}
