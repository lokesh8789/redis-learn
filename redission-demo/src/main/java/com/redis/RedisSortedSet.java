package com.redis;

import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RScoredSortedSetReactive;
import org.redisson.api.RedissonReactiveClient;
import org.redisson.client.codec.StringCodec;
import reactor.core.publisher.Mono;

import java.util.function.Function;

@Slf4j
public class RedisSortedSet {
    private static final RedissionConfig redissionConfig = new RedissionConfig();
    private static final RedissonReactiveClient client = redissionConfig.getReactiveClient();

    public static void main(String[] args){
        RScoredSortedSetReactive<String> sortedSet = client.getScoredSortedSet("student:score", StringCodec.INSTANCE);

        sortedSet.addScore("sam", 12.25)
                .then(sortedSet.add(23.25, "mike"))
                .then(sortedSet.addScore("jake", 7))
                .then(sortedSet.entryRange(0, 1)
                        .flatMapIterable(Function.identity()) // flux
                        .map(se -> se.getScore() + " : " + se.getValue())
                        .doOnNext(e -> log.info("{}", e))
                        .then())
                .subscribe();
    }
}
