package com.learnkafkastreams.service;

import com.learnkafkastreams.domain.OrdersCountPerStoreByWindowsDTO;
import com.learnkafkastreams.util.OrdersUtil;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

@AllArgsConstructor
@Service
@Slf4j
public class OrderWindowsService {
    private final OrderStoreService orderStoreService;

    public List<OrdersCountPerStoreByWindowsDTO> getWindowedOrdersCounts(String orderType, String locationId) {
        ReadOnlyWindowStore<String, Long> orderCountWindowStore = orderStoreService.getOrderCountWindowStore(orderType);

        KeyValueIterator<Windowed<String>, Long> windowedOrdersCount = orderCountWindowStore.all();

        Spliterator<KeyValue<Windowed<String>, Long>> spliterator = Spliterators.spliteratorUnknownSize(windowedOrdersCount,0);

        return StreamSupport.stream(spliterator,false)
                .filter(keyValue -> Optional.ofNullable(locationId).map(l->keyValue.key.key().equals(l)).orElse(true))
                .map((keyValue) -> buildDto(keyValue, orderType))
                .toList();

    }

    private OrdersCountPerStoreByWindowsDTO buildDto(KeyValue<Windowed<String>, Long> keyValue, String orderType) {
        LocalDateTime start = LocalDateTime.ofInstant(keyValue.key.window().startTime(), ZoneOffset.UTC);
        LocalDateTime end = LocalDateTime.ofInstant(keyValue.key.window().endTime(), ZoneOffset.UTC);
        return new OrdersCountPerStoreByWindowsDTO(keyValue.key.key(), keyValue.value, OrdersUtil.getOrderTypeFromTopology(orderType), start, end);
    }
}
