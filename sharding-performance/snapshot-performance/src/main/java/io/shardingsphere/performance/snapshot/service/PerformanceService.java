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
import java.util.Collection;
import java.util.List;
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

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/**
 * ${DESCRIPTION}
 *
 * @author yangyi
 */
@Service
@Slf4j
public class PerformanceService {
    
    private static final String INSERT_SQL = "INSERT INTO saga_snapshot2 (id, transaction_id, snapshot_id, transaction_context, revert_context) values (?, ?, ?, ?, ?)";
    
    private static final String DELETE_SQL = "DELETE FROM saga_snapshot2 WHERE id = ?";
    
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
        long start = System.currentTimeMillis();
        Future<Object> future = executorService.submit(new TestInsert(transactionId));
        Snapshot snapshot1 = doInsert(transactionId);
        Snapshot snapshot2 = (Snapshot) future.get();
        doDelete(Lists.newArrayList(snapshot1, snapshot2));
        log.info("txId {} whole tx cost time {}", transactionId, System.currentTimeMillis() - start);
    }
    
    @SneakyThrows
    private Snapshot doInsert(String transactionId) {
        Snapshot snapshot = new Snapshot(transactionId);
        log.info("txId {} start to get insert connection", transactionId);
        long start = System.currentTimeMillis();
        try (Connection connection = dataSource.getConnection();
            PreparedStatement statement = connection.prepareStatement(INSERT_SQL)) {
            log.info("txId {} get insert connection cost time {}", transactionId, System.currentTimeMillis() - start);
            statement.setObject(1, snapshot.getUniformId());
            statement.setObject(2, snapshot.getTransactionId());
            statement.setObject(3, snapshot.getSnapshotId());
            statement.setObject(4, snapshot.getTransactionContext());
            statement.setObject(5, snapshot.getRevertContext());
            long executeStart = System.currentTimeMillis();
            statement.executeUpdate();
            log.info("txId {} execute insert cost time {}", transactionId, System.currentTimeMillis() - executeStart);
        }
        log.info("txId {} execute insert whole cost time {}", transactionId, System.currentTimeMillis() - start);
        return snapshot;
    }
    
    @SneakyThrows
    private void doDelete(List<Snapshot> snapshots) {
        long start = System.currentTimeMillis();
        try (Connection connection = dataSource.getConnection();
            PreparedStatement statement = connection.prepareStatement(DELETE_SQL)) {
            log.info("txId {} get delete connection cost time {}", snapshots.get(0).getTransactionId(), System.currentTimeMillis() - start);
            for (Snapshot snapshot : snapshots) {
                statement.setObject(1, snapshot.getUniformId());
                statement.addBatch();
            }
            long executeStart = System.currentTimeMillis();
            statement.executeBatch();
            log.info("txId {} execute delete cost time {}", snapshots.get(0).getTransactionId(), System.currentTimeMillis() - executeStart);
        }
        log.info("txId {} execute delete whole cost time {}", snapshots.get(0).getTransactionId(), System.currentTimeMillis() - start);
    }
    
    @RequiredArgsConstructor
    @Getter
    private class Snapshot {
        private final String transactionId;
        
        private final int snapshotId = new Random().nextInt();
    
        private String revertContext = "SQLRevertResult(sql=UPDATE xxx SET xxx=? WHERE id=?, parameterSets=[[xxx, 1111111]])";
        
        private String transactionContext = "SagaBranchTransaction(dataSourceName=ds_1, sql=UPDATE xxx set xxx=? WHERE id=?, parameterSets=[[xxx, 1111111]])";
        
        public String getUniformId() {
            return transactionId + snapshotId;
        }
    }
    
    @RequiredArgsConstructor
    private class TestInsert implements Callable<Object> {
        
        private final String transactionId;
    
        @Override
        public Object call() throws Exception {
            return doInsert(transactionId);
        }
    }
}
