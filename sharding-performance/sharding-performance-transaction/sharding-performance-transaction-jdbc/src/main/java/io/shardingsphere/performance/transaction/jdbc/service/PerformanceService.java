/*
 * Copyright 2016-2018 shardingsphere.io.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */

package io.shardingsphere.performance.transaction.jdbc.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import javax.annotation.Resource;

import org.apache.shardingsphere.transaction.core.TransactionType;
import org.apache.shardingsphere.transaction.core.TransactionTypeHolder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import io.shardingsphere.performance.transaction.jdbc.entity.Order;
import io.shardingsphere.performance.transaction.jdbc.repository.OrderRepo;
import io.shardingsphere.transaction.annotation.ShardingTransactionType;

@Service
public class PerformanceService {
    
    @Resource
    private OrderRepo orderRepo;
    
    private List<Long> initOrderIds = new ArrayList<>();
    
    public void initEnvironment() {
        initOrderIds.clear();
        orderRepo.createTableIfNotExists();
        orderRepo.truncateTable();
    }
    
    public void initData(final int dataRows) {
        List<Order> orders = orderRepo.selectAll();
        for (Order order : orders) {
            initOrderIds.add(order.getOrderId());
        }
        if (initOrderIds.size() < dataRows) {
            int restRowNumber = dataRows - initOrderIds.size();
            for (int i = 1; i <= restRowNumber; i++) {
                Order order = new Order();
                order.setUserId(i);
                order.setStatus("INIT");
                orderRepo.insert(order);
                initOrderIds.add(order.getOrderId());
            }
        }
    }
    
    @Transactional
    public void processSuccess(final Integer repeatTimes) {
        Random random = new Random();
        for (int i = 0; i < repeatTimes; i++) {
            randomUpdate(random);
        }
    }
    
    @Transactional
    public void processFailure(final Integer repeatTimes) {
        Random random = new Random();
        for (int i = 0; i < repeatTimes; i++) {
            randomUpdate(random);
        }
//        insertTooLongData();
        throw new RuntimeException("test exception");
    }
    
    private void insertTooLongData() {
        Order order = new Order();
        order.setUserId(1);
        order.setStatus("Too long data !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
        orderRepo.insert(order);
    }
    
    public void processSuccessNonTx(final Integer repeatTimes) {
        Random random = new Random();
        for (int i = 0; i < repeatTimes; i++) {
            randomUpdate(random);
        }
    }
    
    private void randomUpdate(final Random random) {
        Order order = new Order();
        order.setOrderId(initOrderIds.get(random.nextInt(initOrderIds.size())));
        order.setStatus(Thread.currentThread().getName());
        orderRepo.update(order);
    }
    
    @Transactional
    @ShardingTransactionType
    public void processSuccessWithLocal(final Integer repeatTimes) {
        printTransactionType();
        processSuccess(repeatTimes);
    }
    
    @Transactional
    @ShardingTransactionType
    public void processFailureWithLocal(final Integer repeatTimes) {
        printTransactionType();
        processFailure(repeatTimes);
    }
    
    @Transactional
    @ShardingTransactionType(value = TransactionType.XA)
    public void processSuccessWithXA(final Integer repeatTimes) {
        printTransactionType();
        processSuccess(repeatTimes);
    }
    
    @Transactional
    @ShardingTransactionType(value = TransactionType.XA)
    public void processFailureWithXA(final Integer repeatTimes) {
        printTransactionType();
        processFailure(repeatTimes);
    }
    
    @Transactional
    @ShardingTransactionType(value = TransactionType.BASE)
    public void processSuccessWithBase(final Integer repeatTimes) {
        printTransactionType();
        processSuccess(repeatTimes);
    }
    
    @Transactional
    @ShardingTransactionType(value = TransactionType.BASE)
    public void processFailureWithBase(final Integer repeatTimes) {
        printTransactionType();
        processFailure(repeatTimes);
    }
    
    private void printTransactionType() {
//        System.out.println(String.format("-------------- Process With Transaction %s ---------------", TransactionTypeHolder.get()));
    }
}
