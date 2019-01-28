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

package io.shardingsphere.performance.transaction.jdbc.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import io.shardingsphere.performance.transaction.jdbc.service.PerformanceService;

@RestController
@RequestMapping(value = "/jdbc")
public final class JDBCPerformanceController {
    
    @Autowired
    private PerformanceService performanceService;
    
    @RequestMapping(value = "/init")
    public String init() {
        performanceService.initEnvironment();
        return "ok";
    }
    
    @RequestMapping(value = "/init/data")
    public String initData(@RequestParam(name = "row") Integer row) {
        performanceService.initData(row);
        return "ok";
    }
    
    @RequestMapping(value = "/commit/auto")
    public String autoCommit(@RequestParam(name = "number") Integer number) {
        performanceService.processSuccessNonTx(number);
        return "ok";
    }
    
    @RequestMapping(value = "/commit/local")
    public String localCommit(@RequestParam(name = "number") Integer number) {
        performanceService.processSuccessWithLocal(number);
        return "ok";
    }
    
    @RequestMapping(value = "/rollback/local")
    public String localRollback(@RequestParam(name = "number") Integer number) {
        try {
            performanceService.processFailureWithLocal(number);
        } catch (final RuntimeException ignore) {
        }
        return "ok";
    }
    
    @RequestMapping(value = "/commit/xa")
    public String xaCommit(@RequestParam(name = "number") Integer number) {
        performanceService.processSuccessWithXA(number);
        return "ok";
    }
    
    @RequestMapping(value = "/rollback/xa")
    public String xaRollback(@RequestParam(name = "number") Integer number) {
        try {
            performanceService.processFailureWithXA(number);
        } catch (final RuntimeException ignore) {
        }
        return "ok";
    }
    
    @RequestMapping(value = "/commit/base")
    public String baseCommit(@RequestParam(name = "number") Integer number) {
        performanceService.processSuccessWithBase(number);
        return "ok";
    }
    
    @RequestMapping(value = "/rollback/base")
    public String baseRollback(@RequestParam(name = "number") Integer number) {
        try {
            performanceService.processFailureWithBase(number);
        } catch (final RuntimeException ignore) {
        }
        return "ok";
    }
}
