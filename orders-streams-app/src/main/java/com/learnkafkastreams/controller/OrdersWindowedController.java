package com.learnkafkastreams.controller;

import com.learnkafkastreams.domain.OrdersCountPerStoreByWindowsDTO;
import com.learnkafkastreams.service.OrderWindowsService;
import lombok.AllArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Objects;

@AllArgsConstructor
@RestController
@RequestMapping("/v1/orders")
public class OrdersWindowedController {
    private final OrderWindowsService orderWindowsService;
    /**
     * @return All Orders Count for  Windows by Order Type
     */
    @GetMapping({"/windows/count", "/windows/count/{orderType}", "/windows/count/{orderType}/{location_id}"})
    public ResponseEntity<List<OrdersCountPerStoreByWindowsDTO>> getWindowedOrdersCountsByLocationId(@PathVariable(value="orderType", required=false) String orderType, @PathVariable(value="location_id", required=false) String locationId) {
        if (Objects.isNull(orderType)) {
            return ResponseEntity.ok(orderWindowsService.getAllWindowOrdersCounts(locationId));
        }
        return ResponseEntity.ok(orderWindowsService.getWindowedOrdersCounts(orderType, locationId));
    }
}
