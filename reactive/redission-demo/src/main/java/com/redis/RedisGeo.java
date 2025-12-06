package com.redis;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.GeoUnit;
import org.redisson.api.RGeoReactive;
import org.redisson.api.RMapReactive;
import org.redisson.api.RedissonReactiveClient;
import org.redisson.api.geo.GeoSearchArgs;
import org.redisson.api.geo.OptionalGeoSearch;
import org.redisson.codec.TypedJsonJacksonCodec;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

@Slf4j
public class RedisGeo {
    private static final RedissionConfig redissionConfig = new RedissionConfig();
    private static final RedissonReactiveClient client = redissionConfig.getReactiveClient();

    public record Restaurant(
            String id,
            String city,
            double latitude,
            double longitude,
            String name,
            String zip
    ) {}

    public record GeoLocation (double longitude, double latitude) {}

    public static List<Restaurant> getRestaurants(){
        ObjectMapper mapper = new ObjectMapper();
        InputStream stream = RedisGeo.class.getClassLoader().getResourceAsStream("restaurant.json");
        try {
            return mapper.readValue(stream, new TypeReference<>() {});
        } catch (IOException e) {
            log.info(e.getMessage());
        }
        return Collections.emptyList();
    }

    public static void addData(RGeoReactive<Restaurant> geo, RMapReactive<String, GeoLocation> map) {
        Flux.fromIterable(getRestaurants())
                .flatMap(r -> geo.add(r.longitude(), r.latitude(), r).thenReturn(r))
                .flatMap(r -> map.fastPut(r.zip(), new GeoLocation(r.longitude(), r.latitude())))
                .then()
                .doOnSuccess(unused -> log.info("Completed"))
                .subscribe();
    }

    public static void main(String[] args) {
        RGeoReactive<Restaurant> geo = client.getGeo("restaurants", new TypedJsonJacksonCodec(Restaurant.class));
        RMapReactive<String, GeoLocation> map = client.getMap("us:texas", new TypedJsonJacksonCodec(String.class, GeoLocation.class));
//        addData(geo, map);
//        r1(geo);
        r2(geo, map);
    }

    private static void r2(RGeoReactive<Restaurant> geo, RMapReactive<String, GeoLocation> map) {
        map.get("75224")
                .map(gl -> GeoSearchArgs.from(gl.longitude(), gl.latitude()).radius(5, GeoUnit.MILES))
                .flatMap(geo::search)
                .flatMapIterable(Function.identity())
                .doOnNext(r -> log.info("{}", r))
                .then()
                .subscribe();
    }

    private static void r1(RGeoReactive<Restaurant> geo) {
        OptionalGeoSearch radius = GeoSearchArgs.from(-96.80539, 32.78136).radius(3, GeoUnit.MILES);
        geo.search(radius)
                .flatMapIterable(Function.identity())
                .doOnNext(r -> log.info("{}", r))
                .subscribe();
    }
}
