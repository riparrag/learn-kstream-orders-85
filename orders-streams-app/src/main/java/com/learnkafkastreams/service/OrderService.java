package com.learnkafkastreams.service;

import com.learnkafkastreams.domain.*;
import com.learnkafkastreams.topology.OrdersTopology;
import io.micrometer.common.util.StringUtils;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.stream.StreamSupport;

@AllArgsConstructor
@Service
@Slf4j
public class OrderService {
    private final OrderStoreService orderStoreService;

    public List<OrderCountPerStoreDTO> getOrdersCountPerStore(String orderType) {
        return this.getOrdersCountPerStore(orderType, null);
    }

    public List<OrderCountPerStoreDTO> getOrdersCountPerStore(String orderType, String locationId) {
        ReadOnlyKeyValueStore<String, Long> orderCountStore = orderStoreService.getOrderCountStore(orderType);
        KeyValueIterator<String, Long> ordersCountsPerStore = orderCountStore.all();
        Spliterator<KeyValue<String, Long>> spliterator = Spliterators.spliteratorUnknownSize(ordersCountsPerStore,0);

        return StreamSupport.stream(spliterator,false)
                            .filter(keyValue -> Optional.ofNullable(locationId).map(l->keyValue.key.equals(l)).orElse(true))
                            .map((keyValue) -> new OrderCountPerStoreDTO(keyValue.key, keyValue.value))
                            .toList();
    }

    public List<OrderRevenueDTO> getOrdersRevenuesPerStore(String orderType, String locationId) {
        ReadOnlyKeyValueStore<String, TotalRevenue> orderRevenueStore = orderStoreService.getOrderRevenueStore(orderType);
        KeyValueIterator<String, TotalRevenue> ordersRevenuesPerStore = orderRevenueStore.all();
        Spliterator<KeyValue<String, TotalRevenue>> spliterator = Spliterators.spliteratorUnknownSize(ordersRevenuesPerStore,0);

        return StreamSupport.stream(spliterator,false)
                .filter(keyValue -> Optional.ofNullable(locationId).map(l->keyValue.key.equals(l)).orElse(true))
                .map((keyValue) -> new OrderRevenueDTO(keyValue.key, getOrderTypeFromTopology(orderType), keyValue.value))
                .toList();
    }

    public List<AllOrdersCountPerStoreDTO> getAllOrdersCountPerStore() {
        return List.of(OrdersTopology.GENERAL_ORDERS, OrdersTopology.RESTAURANT_ORDERS)
                   .stream()
                   .map(orderType -> getOrdersCountPerStore(orderType).stream()
                                                                      .map(o -> new AllOrdersCountPerStoreDTO(o.locationId(), o.orderCount(), getOrderTypeFromTopology(orderType)))
                                                                      .toList())
                   .flatMap(List::stream)
                   .toList();
    }

    public OrderCountPerStoreDTO getOrdersCountByLocationId(String orderType, String locationId) {
        if (StringUtils.isBlank(locationId)) {
            return null;
        }
        ReadOnlyKeyValueStore<String, Long> orderCountStore = orderStoreService.getOrderCountStore(orderType);
        Long ordersCounts = orderCountStore.get(locationId);
        return new OrderCountPerStoreDTO(locationId, ordersCounts);
    }

    private OrderType getOrderTypeFromTopology(String orderTopologyType) {
        return switch (orderTopologyType) {
            case OrdersTopology.GENERAL_ORDERS -> OrderType.GENERAL;
            case OrdersTopology.RESTAURANT_ORDERS -> OrderType.RESTAURANT;
            default -> null;
        };
    }
}
