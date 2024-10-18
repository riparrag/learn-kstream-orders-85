package com.learnkafkastreams.controller;

import com.learnkafkastreams.domain.AllOrdersCountPerStoreDTO;
import com.learnkafkastreams.domain.OrderCountPerStoreDTO;
import com.learnkafkastreams.domain.OrderRevenueDTO;
import com.learnkafkastreams.domain.OrdersCountPerStoreByWindowsDTO;
import com.learnkafkastreams.service.OrderService;
import lombok.AllArgsConstructor;
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
    public List<AllOrdersCountPerStoreDTO> getAllOrdersCounts() {

        return orderService.getAllOrdersCountPerStore();
    }

    /**
     * @return Retrieve Orders Count By Order Type & Location Id
     */
    @GetMapping("/count/{orderType}")
    public List<OrderCountPerStoreDTO> getOrdersCountPerStore(@PathVariable("orderType") String orderType, @RequestParam(value = "location_id", required=false) String locationId) {

        return orderService.getOrdersCountPerStore(orderType, locationId);
    }

    /**
     * @return All Orders Count for  Windows by Order Type
     */
    @GetMapping("/windows/count/{orderType}")
    public List<OrdersCountPerStoreByWindowsDTO> getWindowedOrdersCountPerStore(@PathVariable("orderType") String orderType, @RequestParam("location_id") String locationId) {

        return null;
    }

    /**
     * @return Retrieve Revenue for All Types
     */
    @GetMapping("/revenue")
    public List<OrderRevenueDTO> getAllRevenues() {

        return null;
    }

    /**
     * @return Retrieve Revenue By Order Type
     */
    @GetMapping("/revenue/{orderType}")
    public List<OrderRevenueDTO> getOrdersRevenue(@PathVariable("orderType") String orderType) {

        return null;
    }
}