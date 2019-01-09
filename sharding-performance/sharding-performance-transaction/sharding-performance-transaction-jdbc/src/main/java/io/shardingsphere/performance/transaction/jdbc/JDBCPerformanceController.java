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

package io.shardingsphere.performance.transaction.jdbc;

import io.shardingsphere.core.exception.ShardingException;
import io.shardingsphere.example.repository.mybatis.service.SpringPojoTransactionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value = "/jdbc")
public final class JDBCPerformanceController {
    
    @Autowired
    @Qualifier("jdbcTransactionService")
    private SpringPojoTransactionService springPojoTransactionService;
    
    @RequestMapping(value = "/init")
    public String init() {
        springPojoTransactionService.initEnvironment();
        return "ok";
    }
    
    @RequestMapping(value = "/insert")
    public String insert() {
        springPojoTransactionService.processSuccess(false, 1);
        return "ok";
    }
    
    @RequestMapping(value = "/commit/auto")
    public String autoCommit(@RequestParam(name = "number") Integer number) {
        springPojoTransactionService.processSuccessNonTx(false, number);
        return "ok";
    }
    
    @RequestMapping(value = "/commit/local")
    public String localCommit(@RequestParam(name = "number") Integer number) {
        springPojoTransactionService.processSuccessWithLocal(number);
        return "ok";
    }
    
    @RequestMapping(value = "/rollback/local")
    public String localRollback(@RequestParam(name = "number") Integer number) {
        try {
            springPojoTransactionService.processFailureWithLocal(number);
        } catch (final ShardingException ignore) {
        }
        return "ok";
    }
    
    @RequestMapping(value = "/commit/xa")
    public String xaCommit(@RequestParam(name = "number") Integer number) {
        springPojoTransactionService.processSuccessWithXA(number);
        return "ok";
    }
    
    @RequestMapping(value = "/rollback/xa")
    public String xaRollback(@RequestParam(name = "number") Integer number) {
        try {
            springPojoTransactionService.processFailureWithXA(number);
        } catch (final ShardingException ignore) {
        }
        return "ok";
    }
    
    @RequestMapping(value = "/commit/base")
    public String baseCommit(@RequestParam(name = "number") Integer number) {
        springPojoTransactionService.processSuccessWithBase(number);
        return "ok";
    }
    
    @RequestMapping(value = "/rollback/base")
    public String baseRollback(@RequestParam(name = "number") Integer number) {
        try {
            springPojoTransactionService.processFailureWithBase(number);
        } catch (final ShardingException ignore) {
        }
        return "ok";
    }
}
