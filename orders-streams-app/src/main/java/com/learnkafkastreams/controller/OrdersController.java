package com.learnkafkastreams.controller;

import com.learnkafkastreams.domain.AllOrdersCountPerStoreDTO;
import com.learnkafkastreams.domain.OrderCountPerStoreDTO;
import com.learnkafkastreams.domain.OrderRevenueDTO;
import com.learnkafkastreams.domain.OrdersCountPerStoreByWindowsDTO;
import com.learnkafkastreams.service.OrderService;
import lombok.AllArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@AllArgsConstructor
@RestController
@RequestMapping("/v1/orders")
public class OrdersController {
    private final OrderService orderService;
    /**
     * @return Retrieve All Orders Count for All Types
     */
    @GetMapping("/count")
    public ResponseEntity<List<AllOrdersCountPerStoreDTO>> getAllOrdersCounts() {
        return ResponseEntity.ok(orderService.getAllOrdersCountPerStore());
    }

    /**
     * @return Retrieve Orders Count By Order Type & Location Id
     */
    @GetMapping("/count/{orderType}/{location_id}")
    public ResponseEntity<OrderCountPerStoreDTO> getOrdersCountByLocationId(@PathVariable("orderType") String orderType, @PathVariable("location_id") String locationId) {
        return ResponseEntity.ok(orderService.getOrdersCountByLocationId(orderType, locationId));
    }

    /**
     * @return Retrieve Orders Count By Order Type & Location Id
     */
    @GetMapping("/count/{orderType}")
    public ResponseEntity<List<OrderCountPerStoreDTO>> getOrdersCountPerStore(@PathVariable("orderType") String orderType, @RequestParam(value = "location_id", required=false) String locationId) {
        return ResponseEntity.ok(orderService.getOrdersCountPerStore(orderType, locationId));
    }

    /**
     * @return All Orders Count for  Windows by Order Type
     */
    @GetMapping("/windows/count/{orderType}/{location_id}")
    public ResponseEntity<List<OrdersCountPerStoreByWindowsDTO>> getWindowedOrdersCountPerStore(@PathVariable("orderType") String orderType, @PathVariable(value="location_id", required=false) String locationId) {

        return ResponseEntity.ok(null);
    }

    /**
     * @return Retrieve Revenue for All Types
     */
    @GetMapping("/revenue")
    public ResponseEntity<List<OrderRevenueDTO>> getAllRevenues() {

        return ResponseEntity.ok(null);
    }

    /**
     * @return Retrieve Revenue By Order Type
     */
    @GetMapping("/revenue/{orderType}")
    public ResponseEntity<List<OrderRevenueDTO>> getOrdersRevenue(@PathVariable("orderType") String orderType, @PathVariable(value="location_id", required=false) String locationId) {
        return ResponseEntity.ok(orderService.getOrdersRevenuesPerStore(orderType, locationId));
    }
}