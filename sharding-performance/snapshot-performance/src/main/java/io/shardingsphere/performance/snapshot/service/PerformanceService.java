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

package io.shardingsphere.performance.snapshot.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.annotation.Resource;
import javax.sql.DataSource;

import org.springframework.stereotype.Service;

import com.google.common.util.concurrent.MoreExecutors;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;

/**
 * ${DESCRIPTION}
 *
 * @author yangyi
 */
@Service
public class PerformanceService {
    
    private static final String INSERT_SQL = "INSERT INTO saga_snapshot (transaction_id, snapshot_id, transaction_context, revert_context) values (?, ?, ?, ?)";
    
    private static final String DELETE_SQL = "DELETE FROM saga_snapshot WHERE transaction_id = ?";
    
    @Resource
    private DataSource dataSource;
    
    private ExecutorService executorService;
    
    public PerformanceService() {
        executorService = Executors.newCachedThreadPool();
        MoreExecutors.addDelayedShutdownHook(executorService, 60, TimeUnit.SECONDS);
    }
    
    @SneakyThrows
    public void business() {
        String transactionId = UUID.randomUUID().toString();
        Future<Object> future = executorService.submit(new TestInsert(transactionId));
        doInsert(transactionId);
        future.get();
        doDelete(transactionId);
    }
    
    @SneakyThrows
    private void doInsert(String transactionId) {
        Snapshot snapshot = new Snapshot(transactionId);
        try (Connection connection = dataSource.getConnection();
            PreparedStatement statement = connection.prepareStatement(INSERT_SQL)) {
            statement.setObject(1, snapshot.getTransactionId());
            statement.setObject(2, snapshot.getSnapshotId());
            statement.setObject(3, snapshot.getTransactionContext());
            statement.setObject(4, snapshot.getRevertContext());
            statement.executeUpdate();
        }
    }
    
    @SneakyThrows
    private void doDelete(String transactionId) {
        try (Connection connection = dataSource.getConnection();
            PreparedStatement statement = connection.prepareStatement(DELETE_SQL)) {
            statement.setObject(1, transactionId);
            statement.executeUpdate();
        }
    }
    
    @RequiredArgsConstructor
    @Getter
    private class Snapshot {
        private final String transactionId;
        
        private final int snapshotId = new Random().nextInt();
    
        private String revertContext = "SQLRevertResult(sql=UPDATE xxx SET xxx=? WHERE id=?, parameterSets=[[xxx, 1111111]])";
        
        private String transactionContext = "SagaBranchTransaction(dataSourceName=ds_1, sql=UPDATE xxx set xxx=? WHERE id=?, parameterSets=[[xxx, 1111111]])";
    }
    
    @RequiredArgsConstructor
    private class TestInsert implements Callable<Object> {
        
        private final String transactionId;
    
        @Override
        public Object call() throws Exception {
            doInsert(transactionId);
            return "ok";
        }
    }
}
