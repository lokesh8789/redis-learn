package com.redis;

import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBucketReactive;
import org.redisson.api.RTransactionReactive;
import org.redisson.api.RedissonReactiveClient;
import org.redisson.api.TransactionOptions;
import org.redisson.client.codec.LongCodec;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

@Slf4j
public class RedisTransaction {
    private static final RedissionConfig redissionConfig = new RedissionConfig();
    private static final RedissonReactiveClient client = redissionConfig.getReactiveClient();

    public static void main(String[] args) throws InterruptedException {
        RBucketReactive<Long> user1Balance = client.getBucket("user:1:balance", LongCodec.INSTANCE);
        RBucketReactive<Long> user2Balance = client.getBucket("user:2:balance", LongCodec.INSTANCE);
        accountSetup(user1Balance, user2Balance);
        Thread.sleep(Duration.ofSeconds(3));
        accountStatus(user1Balance, user2Balance);
        Thread.sleep(Duration.ofSeconds(3));
//        nonTransaction(user1Balance, user2Balance);
//        Thread.sleep(Duration.ofSeconds(5));
//        accountStatus(user1Balance, user2Balance);
        transaction();
        Thread.sleep(Duration.ofSeconds(5));
        accountStatus(user1Balance, user2Balance);
    }

    public static void transaction() {
        RTransactionReactive transaction = client.createTransaction(TransactionOptions.defaults());
        RBucketReactive<Long> user1Balance = transaction.getBucket("user:1:balance", LongCodec.INSTANCE);
        RBucketReactive<Long> user2Balance = transaction.getBucket("user:2:balance", LongCodec.INSTANCE);
        transfer(user1Balance, user2Balance, 50)
                .thenReturn(0)
                .map(i -> (5 / i)) // some error
                .then(transaction.commit())
                .doOnError(ex -> log.error(ex.getMessage()))
                .onErrorResume(ex -> transaction.rollback())
                .subscribe();
    }

    private static void nonTransaction(RBucketReactive<Long> user1Balance, RBucketReactive<Long> user2Balance) {
        try {
            transfer(user1Balance, user2Balance, 50)
                    .thenReturn(0)
                    .map(i -> (5 / i)) // some error
                    .doOnError(e -> log.error(e.getMessage()))
                    .subscribe();
        } catch (Exception ignored) {

        }
    }

    private static Mono<Void> transfer(RBucketReactive<Long> fromAccount, RBucketReactive<Long> toAccount, int amount){
        return Flux.zip(fromAccount.get(), toAccount.get())
                .filter(t -> t.getT1() >= amount)
                .flatMap(t -> fromAccount.set(t.getT1() - amount).thenReturn(t))
                .flatMap(t -> toAccount.set(t.getT2() + amount))
                .then();
    }

    private static void accountSetup(RBucketReactive<Long> user1Balance, RBucketReactive<Long> user2Balance) {
        user1Balance.set(100L)
                .then(user2Balance.set(0L))
                .then()
                .subscribe();
    }

    private static void accountStatus(RBucketReactive<Long> user1Balance, RBucketReactive<Long> user2Balance) {
        Flux.zip(user1Balance.get(), user2Balance.get())
                .doOnNext(e -> log.info("{}", e))
                .then()
                .subscribe();
    }
}
