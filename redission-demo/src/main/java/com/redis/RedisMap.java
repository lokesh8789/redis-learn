package com.redis;

import lombok.extern.slf4j.Slf4j;
import org.redisson.api.*;
import org.redisson.api.options.LocalCachedMapOptions;
import org.redisson.client.codec.StringCodec;
import org.redisson.codec.TypedJsonJacksonCodec;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Slf4j
public class RedisMap {
    private static final RedissionConfig redissionConfig = new RedissionConfig();
    private static final RedissonReactiveClient client = redissionConfig.getReactiveClient();

    public static void main(String[] args) {
//        r1();
//        r2();
//        r3();
//        r4();
    }

    private static void r4() {
        TypedJsonJacksonCodec codec = new TypedJsonJacksonCodec(Integer.class, Student.class);
        RMapCacheReactive<Integer, Student> map = client.getMapCache("stu", codec);

        map.put(1, new Student(21, "Lokesh"), 5, TimeUnit.SECONDS)
                .then(map.put(2, new Student(22, "Ankit")))
                .then(map.put(3, new Student(23, "Subham")))
                .then(map.readAllMap()
                        .doOnNext(s -> log.info("{}", s))
                        .then()
                )
                .then(Mono.delay(Duration.ofSeconds(7)))
                .then(map.readAllMap()
                        .doOnNext(s -> log.info("{}", s))
                        .then()
                )
                .doOnTerminate(() -> redissionConfig.getClient().shutdown())
                .subscribe();
    }

    private static void r3() {
        TypedJsonJacksonCodec codec = new TypedJsonJacksonCodec(Integer.class, Student.class);
        RMapReactive<Integer, Student> map = client.getMap("users", codec);
        Map<Integer, Student> javaMap = Map.of(
                1, new Student(21, "Lokesh"),
                2, new Student(22, "Ankit"),
                3, new Student(23, "Subham")
        );
        map.putAll(javaMap)
                .concatWith(map.readAllMap()
                        .doOnNext(s -> log.info("{}", s))
                        .then()
                )
                .doOnTerminate(() -> redissionConfig.getClient().shutdown())
                .subscribe();
    }

    private static void r2() {
        RMapReactive<String, String> map = client.getMap("user:2", StringCodec.INSTANCE);
        Map<String, String> javaMap = Map.of(
                "name", "Lokesh",
                "age", "25"
        );
        map.putAll(javaMap)
                .concatWith(map.readAllMap()
                        .doOnNext(s -> log.info("{}", s))
                        .then()
                )
                .doOnTerminate(() -> redissionConfig.getClient().shutdown())
                .subscribe();
    }

    private static void r1() {
        RMapReactive<String, String> map = client.getMap("user:1", StringCodec.INSTANCE);
        map.put("name", "Lokesh")
                .then(map.put("age", "25"))
                .then(map.put("id", "345"))
                .then(map.readAllMap()
                        .doOnNext(s -> log.info("{}", s))
                        .then()
                )
                .doOnTerminate(() -> redissionConfig.getClient().shutdown())
                .subscribe();
    }
}
