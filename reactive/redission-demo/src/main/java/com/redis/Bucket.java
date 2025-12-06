package com.redis;

import lombok.extern.slf4j.Slf4j;
import org.redisson.api.DeletedObjectListener;
import org.redisson.api.ExpiredObjectListener;
import org.redisson.api.RBucketReactive;
import org.redisson.api.RedissonReactiveClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.codec.JsonJacksonCodec;
import org.redisson.codec.TypedJsonJacksonCodec;
import reactor.core.publisher.Mono;

import java.time.Duration;

@Slf4j
public class Bucket {

    private static final RedissionConfig redissionConfig = new RedissionConfig();
    private static final RedissonReactiveClient client = redissionConfig.getReactiveClient();

    public static void main(String[] args) {
//        r1();
//        r2();
//        r3();
//        r4();
//        r5();
//        r6();
//        r7();
//        r8();
//        r9();
        r10();
    }

    private static void r10() {
        RBucketReactive<String> bucket = client.getBucket("user:1:name", StringCodec.INSTANCE);
        bucket.set("Lokesh")
                .concatWith(bucket.get()
                        .doOnNext(s -> log.info("{}",s))
                        .then())
                .concatWith(bucket.addListener(new DeletedObjectListener() {
                    @Override
                    public void onDeleted(String name) {
                        log.info("Key Deleted: {}", name);
                    }
                }).then())
                .subscribe();
    }

    private static void r9() {
        RBucketReactive<String> bucket = client.getBucket("user:1:name", StringCodec.INSTANCE);
        bucket.set("Lokesh", Duration.ofSeconds(3))
                .concatWith(bucket.get()
                        .doOnNext(s -> log.info("{}",s))
                        .then())
                .concatWith(bucket.addListener(new ExpiredObjectListener() {
                    @Override
                    public void onExpired(String name) {
                        log.info("Key Expired: {}", name);
                    }
                }).then())
                .subscribe();
    }

    private static void r8() {
        client.getBuckets(StringCodec.INSTANCE)
                .get("user:1:visit", "user:2:visit")
                .doOnNext(map -> log.info("{}", map))
                .doOnTerminate(() -> redissionConfig.getClient().shutdown())
                .subscribe();
    }

    private static void r7() {
        RBucketReactive<Student> bucket = client.getBucket("student:1", new TypedJsonJacksonCodec(Student.class));
        bucket.set(new Student(12, "Lokesh"))
                .concatWith(bucket.get()
                        .doOnNext(s -> log.info("{}",s))
                        .then())
                .doOnTerminate(() -> redissionConfig.getClient().shutdown())
                .subscribe();
    }

    private static void r6() {
        RBucketReactive<Student> bucket = client.getBucket("student:1", JsonJacksonCodec.INSTANCE);
        bucket.set(new Student(12, "Lokesh"))
                .concatWith(bucket.get()
                        .doOnNext(s -> log.info("{}",s))
                        .then())
                .doOnTerminate(() -> redissionConfig.getClient().shutdown())
                .subscribe();
    }

    private static void r5() {
        RBucketReactive<Student> bucket = client.getBucket("student:1");
        bucket.set(new Student(12, "Lokesh"))
                .concatWith(bucket.get()
                        .doOnNext(s -> log.info("{}",s))
                        .then())
                .doOnTerminate(() -> redissionConfig.getClient().shutdown())
                .subscribe();
    }

    private static void r4() {
        RBucketReactive<String> bucket = client.getBucket("user:1:name", StringCodec.INSTANCE);
        bucket.set("Lokesh", Duration.ofSeconds(10))
                .concatWith(bucket.expire(Duration.ofSeconds(40)).then())
                .concatWith(bucket.get()
                        .doOnNext(s -> log.info("{}",s))
                        .then())
                .concatWith(bucket.remainTimeToLive()
                        .doOnNext(aLong -> log.info("Time Remaining: {}", aLong))
                        .then())
                .doOnTerminate(() -> redissionConfig.getClient().shutdown())
                .subscribe();
    }

    private static void r3() {
        RBucketReactive<String> bucket = client.getBucket("user:1:name", StringCodec.INSTANCE);
        bucket.set("Lokesh", Duration.ofSeconds(10))
                .concatWith(Mono.delay(Duration.ofSeconds(12))
                        .doOnNext(aLong -> log.info("Completed"))
                        .then())
                .concatWith(bucket.get()
                        .doOnNext(s -> log.info("{}",s))
                        .then())
                .doOnTerminate(() -> redissionConfig.getClient().shutdown())
                .subscribe();
    }

    private static void r2() {
        RBucketReactive<String> bucket = client.getBucket("user:1:name", StringCodec.INSTANCE);
        bucket.set("Lokesh")
                .concatWith(bucket.get()
                        .doOnNext(s -> log.info("{}",s))
                        .then())
                .doOnTerminate(() -> redissionConfig.getClient().shutdown())
                .subscribe();
    }

    private static void r1() {
        RBucketReactive<String> bucket = client.getBucket("user:1:name");
        bucket.set("Lokesh")
                .concatWith(bucket.get()
                        .doOnNext(s -> log.info("{}",s))
                        .then())
                .doOnTerminate(() -> redissionConfig.getClient().shutdown())
                .subscribe();
    }
}