package com.learnkafkastreams.util;

import com.learnkafkastreams.domain.Order;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

@Slf4j
public class OrderTimeStampExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        var orderRecord = (Order) record.value();
        if(orderRecord!=null && orderRecord.orderedDateTime()!=null){
            var timeStamp = orderRecord.orderedDateTime();

            var instant = timeStamp.toInstant(ZoneOffset.UTC).toEpochMilli();

            log.info("Received timestamp is: {} and after extractor is: {}", timeStamp, instant);
            return instant;
        }
        //fallback to stream time
        return partitionTime;
    }

    public static void printLocalDateTime(Windowed<String> key, Object value) {
        Instant startTime = key.window().startTime();
        Instant endTime = key.window().endTime();
        log.info("startTime: {}, endTime: {}, key {}, count {}", startTime, endTime, key.key(), value);

        LocalDateTime startLDT = LocalDateTime.ofInstant(startTime, ZoneOffset.UTC.normalized());
        LocalDateTime endLDT = LocalDateTime.ofInstant(endTime, ZoneOffset.UTC.normalized());
        log.info("startLDT: {}, endLDT: {}, key {}, count {}", startLDT, endLDT, key.key(), value);
    }
}
