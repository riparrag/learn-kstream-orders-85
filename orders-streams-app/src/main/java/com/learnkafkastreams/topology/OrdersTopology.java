package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.*;
import com.learnkafkastreams.util.OrderTimeStampExtractor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class OrdersTopology {
    public static final String ORDERS = "orders";
    public static final String STORES = "stores";

    public static final String GENERAL_ORDERS = "general_orders";
    public static final String GENERAL_ORDERS_COUNT = "general_orders_count";
    public static final String GENERAL_ORDERS_COUNT_WINDOWS = "general_orders_count_window";
    public static final String GENERAL_ORDERS_REVENUE = "general_orders_revenue";
    public static final String GENERAL_ORDERS_REVENUE_WINDOWS = "general_orders_revenue_window";

    public static final String RESTAURANT_ORDERS = "restaurant_orders";
    public static final String RESTAURANT_ORDERS_COUNT = "restaurant_orders_count";
    public static final String RESTAURANT_ORDERS_REVENUE = "restaurant_orders_revenue";
    public static final String RESTAURANT_ORDERS_COUNT_WINDOWS = "restaurant_orders_count_window";
    public static final String RESTAURANT_ORDERS_REVENUE_WINDOWS = "restaurant_orders_revenue_window";

    private static final Predicate<String, Order> GENERAL_BRANCH_PREDICATE = (key, order) -> order.orderType().equals(OrderType.GENERAL);
    private static final Predicate<String, Order> RESTAURANT_BRANCH_PREDICATE = (key, order) -> order.orderType().equals(OrderType.RESTAURANT);

    private static final ValueMapper<Order, Revenue> ORDER_TO_REVENUE_VALUE_MAPPER = order -> new Revenue(order.locationId(), order.finalAmount());

    @Autowired
    public void process(StreamsBuilder streamsBuilder) {
        orderTopology(streamsBuilder);
    }

    private static void orderTopology(StreamsBuilder streamsBuilder) {
        KStream<String,Order> orderStreams = buildOrderStreams(streamsBuilder);

        KTable<String,Store> storesTable = buildStoreTable(streamsBuilder);

        branchOrdersIntoTopics(orderStreams, storesTable);
    }

    private static KStream<String,Order> buildOrderStreams(StreamsBuilder streamsBuilder) {
        var orderStreams = streamsBuilder
                .stream(ORDERS,
                        Consumed.with(Serdes.String(), new JsonSerde<>(Order.class))
                                .withTimestampExtractor(new OrderTimeStampExtractor())
                )
                .selectKey((key, value) -> value.locationId());

        orderStreams.print(Printed.<String, Order>toSysOut().withLabel(ORDERS));

        return orderStreams;
    }

    private static KTable<String,Store> buildStoreTable(StreamsBuilder streamsBuilder) {
        var storesTable = streamsBuilder.table(STORES, Consumed.with(Serdes.String(), new JsonSerde<>(Store.class)));

        storesTable.toStream()
                   .print(Printed.<String,Store>toSysOut().withLabel(STORES));

        return storesTable;
    }

    private static void branchOrdersIntoTopics(KStream<String, Order> orderStreams, KTable<String, Store> storesTable) {
        orderStreams.split(Named.as("all-orders"))
                .branch(GENERAL_BRANCH_PREDICATE,
                        Branched.withConsumer(generalOrdersStreams -> {
                            generalOrdersStreams.print(Printed.<String,Order>toSysOut().withLabel("general-orders-stream"));
                            generalOrdersStreams.mapValues(ORDER_TO_REVENUE_VALUE_MAPPER)
                                                .to(GENERAL_ORDERS, Produced.with(Serdes.String(), new JsonSerde<>(Revenue.class)));

                            aggregateOrdersByCount(generalOrdersStreams, GENERAL_ORDERS_COUNT, storesTable);
                            aggregateTotalRevenueByLocationId(generalOrdersStreams, GENERAL_ORDERS_REVENUE);
                        })
                )
                .branch(RESTAURANT_BRANCH_PREDICATE,
                        Branched.withConsumer(restaurantOrdersStreams -> {
                            restaurantOrdersStreams.print(Printed.<String,Order>toSysOut().withLabel("restaurant-orders-stream"));
                            restaurantOrdersStreams.mapValues(ORDER_TO_REVENUE_VALUE_MAPPER)
                                                   .to(RESTAURANT_ORDERS, Produced.with(Serdes.String(), new JsonSerde<>(Revenue.class)));

                            aggregateOrdersByCount(restaurantOrdersStreams, RESTAURANT_ORDERS_COUNT, storesTable);
                            aggregateTotalRevenueByLocationId(restaurantOrdersStreams, RESTAURANT_ORDERS_REVENUE);
                        })
                );
    }

    private static void aggregateOrdersByCount(KStream<String, Order> ordersStreamsBranch, String ordersCountName, KTable<String, Store> storesTable) {
        KTable<String,Long> ordersCountByStoreId = ordersStreamsBranch.map((key, value) -> KeyValue.pair(value.locationId(), value))
                                                                      .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(Order.class)))
                                                                      .count(Named.as(ordersCountName), Materialized.as(ordersCountName));
        ordersCountByStoreId.toStream()
                            .print(Printed.<String,Long>toSysOut().withLabel(ordersCountName));

        ValueJoiner<Long, Store, TotalCountWithAddress> valueJoiner = TotalCountWithAddress::new;

        ordersCountByStoreId.join(storesTable, valueJoiner)
                            .toStream()
                            .print(Printed.<String, TotalCountWithAddress>toSysOut().withLabel(ordersCountName+"-by-store-id"));
    }

    private static void aggregateTotalRevenueByLocationId(KStream<String, Order> generalOrdersStreams, String storeName) {
        Initializer<TotalRevenue> totalRevenueInitializer = TotalRevenue::new;

        Aggregator<String, Order, TotalRevenue> totalRevenueAggregator = (key, order, aggregate) -> aggregate.updateRunningRevenue(key, order);

        generalOrdersStreams.map((key, value) -> KeyValue.pair(value.locationId(), value))
                            .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(Order.class)))
                            .aggregate(totalRevenueInitializer, totalRevenueAggregator, Materialized.<String, TotalRevenue, KeyValueStore<Bytes, byte[]>>as(storeName)
                                                                                                    .withKeySerde(Serdes.String())
                                                                                                    .withValueSerde(new JsonSerde<>(TotalRevenue.class)))
                            .toStream()
                            .print(Printed.<String, TotalRevenue>toSysOut().withLabel(storeName));
    }
}