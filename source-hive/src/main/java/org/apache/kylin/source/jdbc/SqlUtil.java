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

package org.apache.kylin.source.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Random;

import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.source.hive.DBConnConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqlUtil {
    private static final Logger logger = LoggerFactory.getLogger(SqlUtil.class);

    public static void closeResources(Connection con, Statement statement) {
        try {
            if (statement != null && !statement.isClosed()) {
                statement.close();
            }
        } catch (Exception e) {
            logger.error("", e);
        }

        try {
            if (con != null && !con.isClosed()) {
                con.close();
            }
        } catch (Exception e) {
            logger.error("", e);
        }
    }

    public static void execUpdateSQL(Connection db, String sql) throws SQLException {
        try (Statement statement = db.createStatement()) {
            statement.executeUpdate(sql);
        }
    }

    public static int tryTimes = 5;

    public static Connection getConnection(DBConnConf dbconf) {
        if (dbconf.getUrl() == null)
            return null;
        Connection con = null;
        try {
            Class.forName(dbconf.getDriver());
        } catch (Exception e) {
            logger.error("", e);
        }
        boolean got = false;
        int times = 0;
        Random r = new Random();
        while (!got && times < tryTimes) {
            times++;
            try {
                con = DriverManager.getConnection(dbconf.getUrl(), dbconf.getUser(), dbconf.getPass());
                got = true;
            } catch (Exception e) {
                logger.warn("while use:" + dbconf, e);
                try {
                    int rt = r.nextInt(10);
                    Thread.sleep(rt * 1000);
                } catch (InterruptedException e1) {
                }
            }
        }
        if (null == con) {
            throw new RuntimeException("Can not connect to the data source.");
        }
        return con;
    }

    public static String jdbcTypeToKylinDataType(int sqlType) {
        String result = "any";

        switch (sqlType) {
        case Types.CHAR:
            result = "char";
            break;
        case Types.VARCHAR:
        case Types.NVARCHAR:
        case Types.LONGVARCHAR:
            result = "varchar";
            break;
        case Types.NUMERIC:
        case Types.DECIMAL:
            result = "decimal";
            break;
        case Types.BIT:
        case Types.BOOLEAN:
            result = "boolean";
            break;
        case Types.TINYINT:
            result = "tinyint";
            break;
        case Types.SMALLINT:
            result = "smallint";
            break;
        case Types.INTEGER:
            result = "integer";
            break;
        case Types.BIGINT:
            result = "bigint";
            break;
        case Types.REAL:
        case Types.FLOAT:
        case Types.DOUBLE:
            result = "double";
            break;
        case Types.BINARY:
        case Types.VARBINARY:
        case Types.LONGVARBINARY:
            result = "byte";
            break;
        case Types.DATE:
            result = "date";
            break;
        case Types.TIME:
            result = "time";
            break;
        case Types.TIMESTAMP:
            result = "timestamp";
            break;
        default:
            //do nothing
            break;
        }

        return result;
    }

    public static boolean isPrecisionApplicable(String typeName) {
        return isScaleApplicable(typeName) || DataType.STRING_FAMILY.contains(typeName);
    }

    public static boolean isScaleApplicable(String typeName) {
        return DataType.NUMBER_FAMILY.contains(typeName) && !DataType.INTEGER_FAMILY.contains(typeName);
    }
}
