package com.redis.spring.service;

import com.redis.spring.dto.City;
import lombok.RequiredArgsConstructor;
import org.redisson.api.RMapReactive;
import org.redisson.api.RedissonReactiveClient;
import org.redisson.codec.TypedJsonJacksonCodec;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.PathVariable;
import reactor.core.publisher.Mono;

@Service
public class CityService {

    private final CityClient cityClient;
    private final RMapReactive<String, City> cityCache;

    public CityService(CityClient cityClient, RedissonReactiveClient client) {
        this.cityClient = cityClient;
        this.cityCache = client.getMap("cities", new TypedJsonJacksonCodec(String.class, City.class));
    }

    public Mono<City> getCity(@PathVariable String zipCode) {
        return cityCache.get(zipCode)
                .switchIfEmpty(cityClient.getCity(zipCode)
                        .flatMap(city -> cityCache.fastPut(zipCode, city).thenReturn(city))
                );
    }
}
