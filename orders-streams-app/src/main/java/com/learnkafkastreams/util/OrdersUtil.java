package com.learnkafkastreams.util;

import com.learnkafkastreams.domain.OrderType;
import com.learnkafkastreams.topology.OrdersTopology;

public class OrdersUtil {
    public static final OrderType getOrderTypeFromTopology(String orderTopologyType) {
        return switch (orderTopologyType) {
            case OrdersTopology.GENERAL_ORDERS -> OrderType.GENERAL;
            case OrdersTopology.RESTAURANT_ORDERS -> OrderType.RESTAURANT;
            default -> null;
        };
    }
}
