package com.redis;

import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RScoredSortedSetReactive;
import org.redisson.api.RedissonReactiveClient;
import org.redisson.codec.TypedJsonJacksonCodec;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

@Slf4j
public class PriorityQueueWay {
    private static final RedissionConfig redissionConfig = new RedissionConfig();
    private static final RedissonReactiveClient client = redissionConfig.getReactiveClient();
    public enum Category {
        PRIME,
        STD,
        GUEST;
    }
    public record UserOrder(int id, Category category) {}

    public static void main(String[] args) throws InterruptedException {
        RScoredSortedSetReactive<UserOrder> priorityQueue = client.getScoredSortedSet("user:order:queue", new TypedJsonJacksonCodec(UserOrder.class));
        producer(priorityQueue);
        Thread.sleep(Duration.ofSeconds(20));
        consumer(priorityQueue);
    }

    private static double getScore(Category category){
        return category.ordinal() + Double.parseDouble("0." + System.nanoTime());
    }

    public static void producer(RScoredSortedSetReactive<UserOrder> priorityQueue) {
        Flux.interval(Duration.ofSeconds(1))
                .take(5)
                .map(l -> (l.intValue() * 5))
                .flatMap(i -> {
                    UserOrder u1 = new UserOrder(i + 1, Category.GUEST);
                    UserOrder u2 = new UserOrder(i + 2, Category.STD);
                    UserOrder u3 = new UserOrder(i + 3, Category.PRIME);
                    UserOrder u4 = new UserOrder(i + 4, Category.STD);
                    UserOrder u5 = new UserOrder(i + 5, Category.GUEST);
                    return Flux.just(u1, u2, u3, u4, u5)
                            .flatMap(u -> priorityQueue.add(getScore(u.category()), u)
                                    .then()
                            )
                            .then();
                }).subscribe();
    }

    public static void consumer(RScoredSortedSetReactive<UserOrder> priorityQueue) {
        priorityQueue.takeFirstElements()
                .limitRate(1)
                .delayElements(Duration.ofMillis(500))
                .doOnNext(e -> log.info("{}", e))
                .subscribe();
    }
}
