package com.redis;

import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RTopicReactive;
import org.redisson.api.RedissonReactiveClient;
import org.redisson.client.codec.StringCodec;
import reactor.core.publisher.Flux;

import java.time.Duration;

@Slf4j
public class RedisPubSub {
    public static class Publisher {
        private static final RedissionConfig redissionConfig = new RedissionConfig();
        private static final RedissonReactiveClient client = redissionConfig.getReactiveClient();

        public static void main(String[] args) {
            RTopicReactive topic = client.getTopic("slack-room", StringCodec.INSTANCE);
            Flux.interval(Duration.ofMillis(500))
                    .take(50)
                    .doOnNext(aLong -> log.info("Publishing: {}", aLong))
                    .concatMap(aLong -> topic.publish(String.valueOf(aLong)))
                    .subscribe();
        }
    }

    public static class Subscriber1 {
        private static final RedissionConfig redissionConfig = new RedissionConfig();
        private static final RedissonReactiveClient client = redissionConfig.getReactiveClient();

        public static void main(String[] args) {
            RTopicReactive topic = client.getTopic("slack-room", StringCodec.INSTANCE);
            topic.getMessages(String.class)
                    .doOnNext(m -> log.info("{}", m))
                    .doOnError(ex -> log.error(ex.getMessage()))
                    .subscribe();
        }
    }

    public static class Subscriber2 {
        private static final RedissionConfig redissionConfig = new RedissionConfig();
        private static final RedissonReactiveClient client = redissionConfig.getReactiveClient();

        public static void main(String[] args) {
            RTopicReactive topic = client.getTopic("slack-room", StringCodec.INSTANCE);
            topic.getMessages(String.class)
                    .doOnNext(m -> log.info("{}", m))
                    .doOnError(ex -> log.error(ex.getMessage()))
                    .subscribe();
        }
    }
}
