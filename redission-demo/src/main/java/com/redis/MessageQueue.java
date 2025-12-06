package com.redis;

import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBlockingDequeReactive;
import org.redisson.api.RedissonReactiveClient;
import org.redisson.client.codec.LongCodec;
import reactor.core.publisher.Flux;

import java.time.Duration;

@Slf4j
public class MessageQueue {
    public static class Producer {
        private static final RedissionConfig redissionConfig = new RedissionConfig();
        private static final RedissonReactiveClient client = redissionConfig.getReactiveClient();

        public static void main(String[] args) {
            RBlockingDequeReactive<Long> queue = client.getBlockingDeque("message-queue", LongCodec.INSTANCE);
            Flux.range(1, 30)
                    .delayElements(Duration.ofMillis(500))
                    .map(Long::valueOf)
                    .doOnNext(i -> log.info("Going to add: {}", i))
                    .flatMap(queue::add)
                    .subscribe();
        }
    }

    public static class Consumer1 {
        private static final RedissionConfig redissionConfig = new RedissionConfig();
        private static final RedissonReactiveClient client = redissionConfig.getReactiveClient();

        public static void main(String[] args) {
            RBlockingDequeReactive<Long> queue = client.getBlockingDeque("message-queue", LongCodec.INSTANCE);
            queue.takeElements()
                    .doOnNext(o -> log.info("Consumer1: {}", o))
                    .doOnError(ex -> log.info(ex.getMessage()))
                    .subscribe();
        }
    }

    public static class Consumer2 {
        private static final RedissionConfig redissionConfig = new RedissionConfig();
        private static final RedissonReactiveClient client = redissionConfig.getReactiveClient();

        public static void main(String[] args) {
            RBlockingDequeReactive<Long> queue = client.getBlockingDeque("message-queue", LongCodec.INSTANCE);
            queue.takeElements()
                    .doOnNext(o -> log.info("Consumer2: {}", o))
                    .doOnError(ex -> log.info(ex.getMessage()))
                    .subscribe();
        }
    }
}
