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

package io.shardingsphere.example.repository.jdbc.service;

import io.shardingsphere.core.constant.transaction.TransactionType;
import io.shardingsphere.core.transaction.TransactionTypeHolder;
import io.shardingsphere.example.repository.api.entity.Order;
import io.shardingsphere.example.repository.api.entity.OrderItem;
import io.shardingsphere.example.repository.api.repository.OrderItemRepository;
import io.shardingsphere.example.repository.api.repository.OrderRepository;
import io.shardingsphere.example.repository.api.service.CommonServiceImpl;
import io.shardingsphere.example.repository.api.service.TransactionService;
import io.shardingsphere.example.repository.jdbc.repository.JDBCOrderItemTransactionRepositotyImpl;
import io.shardingsphere.example.repository.jdbc.repository.JDBCOrderTransacationRepositoryImpl;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

public final class RawPojoTransactionService extends CommonServiceImpl implements TransactionService {
    
    private final JDBCOrderTransacationRepositoryImpl orderRepository;
    
    private final JDBCOrderItemTransactionRepositotyImpl orderItemRepository;
    
    private final DataSource dataSource;
    
    private Connection insertConnection;
    
    public RawPojoTransactionService(final JDBCOrderTransacationRepositoryImpl orderRepository,
        final JDBCOrderItemTransactionRepositotyImpl orderItemRepository, final DataSource dataSource) throws SQLException {
        this.orderRepository = orderRepository;
        this.orderItemRepository = orderItemRepository;
        this.dataSource = dataSource;
    }
    
    @Override
    public void processFailureWithLocal() {
        TransactionTypeHolder.set(TransactionType.LOCAL);
        printTransactionType();
        processFailure();
    }
    
    @Override
    public void processFailureWithXa() {
        TransactionTypeHolder.set(TransactionType.XA);
        printTransactionType();
        processFailure();
    }
    
    @Override
    public void processFailureWithBase() {
        TransactionTypeHolder.set(TransactionType.BASE);
        printTransactionType();
        processFailure();
    }
    
    @Override
    public void printTransactionType() {
        System.out.println(String.format("-------------- Process With Transaction %s ---------------", TransactionTypeHolder.get()));
    }
    
    @Override
    public void processSuccess(boolean isRangeSharding) {
        try (Connection connection = dataSource.getConnection()) {
            orderRepository.setInsertConnection(connection);
            orderItemRepository.setInsertConnection(connection);
            super.processSuccess(isRangeSharding);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    
    @Override
    public void processFailure() {
        try (Connection connection = dataSource.getConnection()){
            this.insertConnection = connection;
            orderRepository.setInsertConnection(insertConnection);
            orderItemRepository.setInsertConnection(insertConnection);
            beginTransaction();
            super.processFailure();
            commitTransaction();
        } catch (RuntimeException ex) {
            System.out.println(ex.getMessage());
            rollbackTransaction();
            super.printData(false);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    
    @Override
    protected OrderRepository getOrderRepository() {
        return orderRepository;
    }
    
    @Override
    protected OrderItemRepository getOrderItemRepository() {
        return orderItemRepository;
    }
    
    @Override
    protected Order newOrder() {
        return new Order();
    }
    
    @Override
    protected OrderItem newOrderItem() {
        return new OrderItem();
    }
    
    private void beginTransaction() {
        try {
            if (null != this.insertConnection) {
                this.insertConnection.setAutoCommit(false);
            }
        } catch (SQLException ignored) {
        }
    }
    
    private void commitTransaction() {
        try {
            if (null != this.insertConnection) {
                this.insertConnection.commit();
            }
        } catch (SQLException ignored) {
        }
    }
    
    private void rollbackTransaction() {
        try {
            if (null != this.insertConnection) {
                this.insertConnection.rollback();
            }
        } catch (SQLException ignored) {
        }
    }
}
