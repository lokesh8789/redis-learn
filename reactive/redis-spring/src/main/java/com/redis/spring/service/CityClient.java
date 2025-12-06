package com.redis.spring.service;

import com.redis.spring.dto.City;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;

@Component
public class CityClient {
    private final List<City> cities = List.of(
            new City("1001", "Delhi"),
            new City("1002", "Mumbai"),
            new City("1003", "Patna"),
            new City("1004", "Kolkata"),
            new City("1005", "Kanpur"),
            new City("1006", "Ranchi")
    );

    public Mono<City> getCity(String zipCode){
        return Flux.fromIterable(cities)
                .filter(c -> c.zip().equals(zipCode))
                .next()
                .delayElement(Duration.ofMillis(3000));
    }
}
