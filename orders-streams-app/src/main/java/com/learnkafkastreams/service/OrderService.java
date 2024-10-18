package com.learnkafkastreams.service;

import com.learnkafkastreams.domain.AllOrdersCountPerStoreDTO;
import com.learnkafkastreams.domain.OrderCountPerStoreDTO;
import com.learnkafkastreams.domain.OrderType;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

@AllArgsConstructor
@Service
@Slf4j
public class OrderService {
    private final OrderStoreService orderStoreService;

    public List<OrderCountPerStoreDTO> getOrdersCountPerStore(String orderType, String locationId) {


        var orderCountStore = orderStoreService.getOrderCountStore(orderType);

        var ordersCountsPerStore = orderCountStore.all();

        var spliterator = Spliterators.spliteratorUnknownSize(ordersCountsPerStore,0);

        return StreamSupport.stream(spliterator,false)
                            .map((keyValue) -> new OrderCountPerStoreDTO(keyValue.key, keyValue.value))
                            .toList();
    }

    public List<AllOrdersCountPerStoreDTO> getAllOrdersCountPerStore() {
        //return getAllOrderCountStore();
        return null;
    }
}
