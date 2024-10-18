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

@AllArgsConstructor
@RestController
@RequestMapping("/v1/orders")
public class OrdersWindowedController {
    private final OrderWindowsService orderWindowsService;
    /**
     * @return All Orders Count for  Windows by Order Type
     */
    @GetMapping("/windows/count/{orderType}")
    public ResponseEntity<List<OrdersCountPerStoreByWindowsDTO>> getWindowedOrdersCountsByLocationId(@PathVariable("orderType") String orderType, @PathVariable(value = "location_id", required=false) String locationId) {
        return ResponseEntity.ok(orderWindowsService.getWindowedOrdersCounts(orderType, locationId));
    }
}
