package com.redis.spring.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RListReactive;
import org.redisson.api.RTopicReactive;
import org.redisson.api.RedissonReactiveClient;
import org.redisson.client.codec.StringCodec;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;

@Slf4j
@Service
@RequiredArgsConstructor
public class ChatService implements WebSocketHandler {

    private final RedissonReactiveClient client;

    @Override
    @NonNull
    public Mono<Void> handle(@NonNull WebSocketSession webSocketSession) {
        String room = getChatRoomName(webSocketSession);
        RTopicReactive topic = client.getTopic(room, StringCodec.INSTANCE);
        RListReactive<String> list = client.getList("history:" + room, StringCodec.INSTANCE);

        // subscribe
        webSocketSession.receive()
                .map(WebSocketMessage::getPayloadAsText)
                .flatMap(msg -> list.add(msg).then(topic.publish(msg)))
                .doOnError(ex -> log.error(ex.getMessage()))
                .doFinally(s -> log.info("Subscriber finally {}", s))
                .subscribe();

        // publisher
        Flux<WebSocketMessage> flux = topic.getMessages(String.class)
                .startWith(list.iterator())
                .map(webSocketSession::textMessage)
                .doOnError(ex -> log.error(ex.getMessage()))
                .doFinally(s -> log.info("Publisher finally {}", s));

        return webSocketSession.send(flux);
    }

    private String getChatRoomName(WebSocketSession socketSession){
        URI uri = socketSession.getHandshakeInfo().getUri();
        return UriComponentsBuilder.fromUri(uri)
                .build()
                .getQueryParams()
                .toSingleValueMap()
                .getOrDefault("room", "default");
    }
}
