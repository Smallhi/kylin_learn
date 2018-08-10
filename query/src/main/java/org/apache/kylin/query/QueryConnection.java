/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.apache.kylin.query;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.calcite.jdbc.Driver;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.query.schema.OLAPSchemaFactory;

public class QueryConnection {
    
    private static Boolean isRegister = false;

    public static Connection getConnection(String project) throws SQLException {
        if (!isRegister) {
            try {
                Class<?> aClass = Thread.currentThread().getContextClassLoader()
                        .loadClass("org.apache.calcite.jdbc.Driver");
                Driver o = (Driver) aClass.newInstance();
                DriverManager.registerDriver(o);
            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                e.printStackTrace();
            }
            isRegister = true;
        }
        // Calcite 并没有建立一个服务的引擎等待查询。 而是通过对外暴露接口，界面model 的形式提供连接。
        // 然后calcite 将SQL翻译成逻辑计划，并最终解析成用户指定的物理计划
        // 实际的执行计划的逻辑是在org.apache.calcite.jdbc.Driver 中实现，所以创建连接的时候，需要指明模型（这里是个JSON文件）
        // 由Driver调起连接，连接之后由calcite 负责解析执行和结果返回，物理计划和优化规则通过接口的方式实现
        File olapTmp = OLAPSchemaFactory.createTempOLAPJson(project, KylinConfig.getInstanceFromEnv());
        Properties info = new Properties();
        info.putAll(KylinConfig.getInstanceFromEnv().getCalciteExtrasProperties());
        info.put("model", olapTmp.getAbsolutePath());
        info.put("typeSystem", "org.apache.kylin.query.calcite.KylinRelDataTypeSystem");
        return DriverManager.getConnection("jdbc:calcite:", info);
    }
}
