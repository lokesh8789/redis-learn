package com.redis;

import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RDequeReactive;
import org.redisson.api.RListReactive;
import org.redisson.api.RQueueReactive;
import org.redisson.api.RedissonReactiveClient;
import org.redisson.client.codec.LongCodec;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.stream.LongStream;

@Slf4j
public class RedisListQueueStack {
    private static final RedissionConfig redissionConfig = new RedissionConfig();
    private static final RedissonReactiveClient client = redissionConfig.getReactiveClient();

    public static void main(String[] args) {
//        r1();
//        r2();
//        r3();
//        r4();
        r5();
    }

    private static void r5() {
        RDequeReactive<Long> stack = client.getDeque("numbers", LongCodec.INSTANCE);

        stack.pollLast()
                .repeat(3)
                .doOnNext(aLong -> log.info("Removed From Stack: {}", aLong))
                .then(stack.readAll().doOnNext(d -> log.info("{}", d)))
                .subscribe();
    }

    private static void r4() {
        RQueueReactive<Long> queue = client.getQueue("numbers", LongCodec.INSTANCE);

        queue.poll()
                .repeat(3)
                .doOnNext(aLong -> log.info("Removed From Queue: {}", aLong))
                .then(queue.readAll().doOnNext(d -> log.info("{}", d)))
                .subscribe();
    }

    private static void r3() {
        RListReactive<Long> list = client.getList("numbers", LongCodec.INSTANCE);

        List<Long> longList = LongStream.rangeClosed(1, 10)
                .boxed()
                .toList();
        list.addAll(longList)
                .then(list.readAll().doOnNext(d -> log.info("{}", d)))
                .subscribe();
    }

    private static void r2() {
        RListReactive<Long> list = client.getList("numbers", LongCodec.INSTANCE);

        Flux.range(1,10)
                .map(Long::valueOf)
                .concatMap(list::add)
                .then(list.readAll().doOnNext(d -> log.info("{}", d)))
                .subscribe();
    }

    private static void r1() {
        RListReactive<Long> list = client.getList("numbers", LongCodec.INSTANCE);

        Flux.range(1,10)
                .map(Long::valueOf)
                .flatMap(list::add)
                .then(list.readAll().doOnNext(d -> log.info("{}", d)))
                .subscribe();
    }
}
