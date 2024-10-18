package com.learnkafkastreams.service;

import com.learnkafkastreams.domain.OrdersCountPerStoreByWindowsDTO;
import com.learnkafkastreams.topology.OrdersTopology;
import com.learnkafkastreams.util.OrdersUtil;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.stream.StreamSupport;

@AllArgsConstructor
@Service
@Slf4j
public class OrderWindowsService {
    private final OrderStoreService orderStoreService;

    public List<OrdersCountPerStoreByWindowsDTO> getWindowedOrdersCounts(String orderType, String locationId) {
        return getWindowedOrdersCounts(orderType, locationId, (Instant) null, null);
    }

    public List<OrdersCountPerStoreByWindowsDTO> getWindowedOrdersCounts(String orderType, String locationId, LocalDateTime timeFrom, LocalDateTime timeTo) {
        return getWindowedOrdersCounts(orderType, locationId, timeFrom.toInstant(ZoneOffset.UTC), timeTo.toInstant(ZoneOffset.UTC));
    }

    public List<OrdersCountPerStoreByWindowsDTO> getWindowedOrdersCounts(String orderType, String locationId, Instant timeFrom, Instant timeTo) {
        ReadOnlyWindowStore<String, Long> orderCountWindowStore = orderStoreService.getOrderCountWindowStore(orderType);

        KeyValueIterator<Windowed<String>, Long> windowedOrdersCount = Objects.isNull(timeFrom) ? orderCountWindowStore.all() : orderCountWindowStore.fetchAll(timeFrom, timeTo);

        Spliterator<KeyValue<Windowed<String>, Long>> spliterator = Spliterators.spliteratorUnknownSize(windowedOrdersCount,0);

        return StreamSupport.stream(spliterator,false)
                .filter(keyValue -> Optional.ofNullable(locationId).map(l->keyValue.key.key().equals(l)).orElse(true))
                .map((keyValue) -> buildDto(keyValue, orderType))
                .toList();
    }

    public List<OrdersCountPerStoreByWindowsDTO> getAllWindowOrdersCountsByRange(LocalDateTime fromTime, LocalDateTime toTime) {
        return List.of(OrdersTopology.GENERAL_ORDERS, OrdersTopology.RESTAURANT_ORDERS)
                .stream()
                .map(orderType -> getWindowedOrdersCounts(orderType, null, fromTime, toTime).stream().toList())
                .flatMap(List::stream)
                .toList();
    }

    public List<OrdersCountPerStoreByWindowsDTO> getAllWindowOrdersCounts(String locationId) {
        return List.of(OrdersTopology.GENERAL_ORDERS, OrdersTopology.RESTAURANT_ORDERS)
                .stream()
                .map(orderType -> getWindowedOrdersCounts(orderType, locationId).stream().toList())
                .flatMap(List::stream)
                .toList();
    }

    private OrdersCountPerStoreByWindowsDTO buildDto(KeyValue<Windowed<String>, Long> keyValue, String orderType) {
        LocalDateTime start = LocalDateTime.ofInstant(keyValue.key.window().startTime(), ZoneOffset.UTC);
        LocalDateTime end = LocalDateTime.ofInstant(keyValue.key.window().endTime(), ZoneOffset.UTC);
        return new OrdersCountPerStoreByWindowsDTO(keyValue.key.key(), keyValue.value, OrdersUtil.getOrderTypeFromTopology(orderType), start, end);
    }
}
