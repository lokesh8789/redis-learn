package com.redis;

import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBucketReactive;
import org.redisson.api.RedissonReactiveClient;

@Slf4j
public class Bucket {

    private static final RedissionConfig redissionConfig = new RedissionConfig();
    private static final RedissonReactiveClient client = redissionConfig.getReactiveClient();

    public static void main(String[] args) {

        RBucketReactive<String> bucket = client.getBucket("user:1:name");
        bucket.set("Lokesh")
                .concatWith(bucket.get()
                        .doOnNext(s -> log.info("Stored Value: {}",s))
                        .then())
                .doOnTerminate(() -> redissionConfig.getClient().shutdown())
                .subscribe();
    }
}