package com.redis;

import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLocalCachedMapReactive;
import org.redisson.api.RedissonReactiveClient;
import org.redisson.api.options.LocalCachedMapOptions;
import org.redisson.codec.TypedJsonJacksonCodec;
import reactor.core.publisher.Flux;

import java.time.Duration;

@Slf4j
public class LocalCacheMap {

    public static class Server1 {
        private static final RedissionConfig redissionConfig = new RedissionConfig();
        private static final RedissonReactiveClient client = redissionConfig.getReactiveClient();

        public static void main(String[] args) throws InterruptedException {
            LocalCachedMapOptions<Integer, Student> mapOptions = LocalCachedMapOptions.<Integer, Student>name("students")
                    .codec(new TypedJsonJacksonCodec(Integer.class, Student.class))
                    .syncStrategy(LocalCachedMapOptions.SyncStrategy.UPDATE)
                    .reconnectionStrategy(LocalCachedMapOptions.ReconnectionStrategy.CLEAR);

            RLocalCachedMapReactive<Integer, Student> map = client.getLocalCachedMap(mapOptions);
            r1(map);
            Thread.sleep(Duration.ofMinutes(2));
        }

        private static void r1(RLocalCachedMapReactive<Integer, Student> map) {
            map.put(1, new Student(21, "Lokesh"))
                    .thenMany(Flux.interval(Duration.ofSeconds(1))
                            .flatMap(aLong -> map.get(1))
                            .doOnNext(student -> log.info("{}", student)))
                    .subscribe();
        }
    }

    public static class Server2 {
        private static final RedissionConfig redissionConfig = new RedissionConfig();
        private static final RedissonReactiveClient client = redissionConfig.getReactiveClient();

        public static void main(String[] args) throws InterruptedException {
            LocalCachedMapOptions<Integer, Student> mapOptions = LocalCachedMapOptions.<Integer, Student>name("students")
                    .codec(new TypedJsonJacksonCodec(Integer.class, Student.class))
                    .syncStrategy(LocalCachedMapOptions.SyncStrategy.UPDATE)
                    .reconnectionStrategy(LocalCachedMapOptions.ReconnectionStrategy.CLEAR);

            RLocalCachedMapReactive<Integer, Student> map = client.getLocalCachedMap(mapOptions);
            r2(map);
            Thread.sleep(Duration.ofMinutes(2));
        }

        private static void r2(RLocalCachedMapReactive<Integer, Student> map) {
            map.put(1, new Student(21, "Lokesh Updated"))
                    .thenMany(Flux.interval(Duration.ofSeconds(1))
                            .flatMap(aLong -> map.get(1))
                            .doOnNext(student -> log.info("{}", student)))
                    .subscribe();
        }
    }
}
