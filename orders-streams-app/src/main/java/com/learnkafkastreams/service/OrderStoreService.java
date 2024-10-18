package com.learnkafkastreams.service;

import com.learnkafkastreams.domain.TotalRevenue;
import lombok.AllArgsConstructor;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Spliterators;

import static com.learnkafkastreams.topology.OrdersTopology.*;

@AllArgsConstructor
@Service
public class OrderStoreService {

    private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    public ReadOnlyKeyValueStore<String, Long> getOrderCountStore(String orderType) {
        return switch (orderType) {
            case GENERAL_ORDERS -> getKeyValueStore(GENERAL_ORDERS_COUNT);
            case RESTAURANT_ORDERS -> getKeyValueStore(RESTAURANT_ORDERS_COUNT);
            default -> throw new IllegalArgumentException("Not valid order type, you sent: "+orderType+ ", possibles values: "+ GENERAL_ORDERS +", "+ RESTAURANT_ORDERS);
        };
    }

    public ReadOnlyKeyValueStore<String, TotalRevenue> getOrderRevenueStore(String orderType) {
        return switch (orderType) {
            case GENERAL_ORDERS -> getKeyValueStore(GENERAL_ORDERS_REVENUE);
            case RESTAURANT_ORDERS -> getKeyValueStore(RESTAURANT_ORDERS_REVENUE);
            default -> throw new IllegalArgumentException("Not valid order type, you sent: "+orderType+ ", possibles values: "+ GENERAL_ORDERS +", "+ RESTAURANT_ORDERS);
        };
    }

    private <T> ReadOnlyKeyValueStore<String,T> getKeyValueStore(String storeName) {
        return streamsBuilderFactoryBean.getKafkaStreams()
                                        .store(StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.keyValueStore()));
    }
}
