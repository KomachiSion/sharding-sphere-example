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

package io.shardingsphere.example.jdbc.nodep.config;

import io.shardingsphere.api.config.SagaConfiguration;
import io.shardingsphere.api.config.rule.ShardingRuleConfiguration;
import io.shardingsphere.api.config.rule.TableRuleConfiguration;
import io.shardingsphere.api.config.strategy.StandardShardingStrategyConfiguration;
import io.shardingsphere.example.algorithm.PreciseModuloShardingTableAlgorithm;
import io.shardingsphere.example.config.DataSourceUtil;
import io.shardingsphere.example.config.ExampleConfiguration;
import io.shardingsphere.shardingjdbc.api.ShardingDataSourceFactory;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public final class ShardingTablesConfigurationPrecise implements ExampleConfiguration {
    
    @Override
    public DataSource getDataSource() throws SQLException {
        ShardingRuleConfiguration shardingRuleConfig = new ShardingRuleConfiguration();
        shardingRuleConfig.getTableRuleConfigs().add(getOrderTableRuleConfiguration());
        shardingRuleConfig.getTableRuleConfigs().add(getOrderItemTableRuleConfiguration());
        shardingRuleConfig.getBindingTableGroups().add("t_order, t_order_item");
        shardingRuleConfig.setDefaultTableShardingStrategyConfig(new StandardShardingStrategyConfiguration("order_id", new PreciseModuloShardingTableAlgorithm()));
        return ShardingDataSourceFactory.createDataSource(createDataSourceMap(), shardingRuleConfig, new HashMap<String, Object>(), new Properties(), new SagaConfiguration());
    }
    
    private static TableRuleConfiguration getOrderTableRuleConfiguration() {
        TableRuleConfiguration result = new TableRuleConfiguration();
        result.setLogicTable("t_order");
        result.setActualDataNodes("demo_ds.t_order_${[0, 1]}");
        result.setKeyGeneratorColumnName("order_id");
        return result;
    }
    
    private static TableRuleConfiguration getOrderItemTableRuleConfiguration() {
        TableRuleConfiguration result = new TableRuleConfiguration();
        result.setLogicTable("t_order_item");
        result.setActualDataNodes("demo_ds.t_order_item_${[0, 1]}");
        return result;
    }
    
    private static Map<String, DataSource> createDataSourceMap() {
        Map<String, DataSource> result = new HashMap<>();
        result.put("demo_ds", DataSourceUtil.createDataSource("demo_ds"));
        return result;
    }
}
