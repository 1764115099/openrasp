/*
 * Copyright 2017-2021 Baidu Inc.
 *
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
 */

package com.baidu.openrasp.hook.sql;

import com.baidu.openrasp.HookHandler;
import com.baidu.openrasp.plugin.checker.CheckParameter;
import com.baidu.openrasp.tool.annotation.HookAnnotation;
import javassist.CannotCompileException;
import javassist.CtClass;
import javassist.NotFoundException;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.ResultSet;
import java.util.HashMap;

/**
 * Created by ldx on 22-10-20.
 * sql查询结果hook点
 */
@HookAnnotation
public class SQLResultHook extends AbstractSqlHook {
    private static final Logger LOGGER = Logger.getLogger(SQLResultHook.class.getName());

    @Override
    public boolean isClassMatched(String className) {
         /* MySQL */
        if ("com/mysql/jdbc/ResultSetImpl".equals(className)
                || "com/mysql/cj/jdbc/result/ResultSetImpl".equals(className)) {
            this.type = SqlType.MYSQL;
            this.exceptions = new String[]{"java/sql/SQLException"};
            LOGGER.debug("----- hook msyql: "+ className);
            return true;
        }

        /* SQLite */
        if ("org/sqlite/RS".equals(className)
                || "org/sqlite/jdbc3/JDBC3ResultSet".equals(className)) {
            this.type = SqlType.SQLITE;
            this.exceptions = new String[]{"java/sql/SQLException"};
            return true;
        }

       /* Oracle */
        if ("oracle/jdbc/driver/OracleResultSetImpl".equals(className)) {
            this.type = SqlType.ORACLE;
            this.exceptions = new String[]{"java/sql/SQLException"};
            return true;
        }

        /* SQL Server */
        if ("com/microsoft/sqlserver/jdbc/SQLServerResultSet".equals(className)) {
            this.type = SqlType.SQLSERVER;
            this.exceptions = new String[]{"com/microsoft/sqlserver/jdbc/SQLServerException"};
            return true;
        }

        /* PostgreSQL */
        if ("org/postgresql/jdbc/PgResultSet".equals(className)
                || "org/postgresql/jdbc1/AbstractJdbc1ResultSet".equals(className)
                || "org/postgresql/jdbc2/AbstractJdbc2ResultSet".equals(className)
                || "org/postgresql/jdbc3/AbstractJdbc3ResultSet".equals(className)
                || "org/postgresql/jdbc3g/AbstractJdbc3gResultSet".equals(className)
                || "org/postgresql/jdbc4/AbstractJdbc4ResultSet".equals(className)) {
            this.type = SqlType.PGSQL;
            this.exceptions = new String[]{"java/sql/SQLException"};
            return true;
        }

        /* DB2 */
        if (className.startsWith("com/ibm/db2/jcc/am")) {
            this.type = SqlType.DB2;
            this.exceptions = new String[]{"java/sql/SQLException"};
            return true;
        }

        return false;
    }

    @Override
    public String getType() {
        return "sqlResult";
    }

    /**
     * hook 目标类的函数
     */
    @Override
    protected void hookMethod(CtClass ctClass) throws IOException, CannotCompileException, NotFoundException {
        CtClass[] interfaces = ctClass.getInterfaces();
        if (this.type.equals(SqlType.DB2) && interfaces != null) {
            for (CtClass inter : interfaces) {
                if (inter.getName().equals("com.ibm.db2.jcc.DB2ResultSet")) {
                    if (interfaces.length > 3) {
                        hookSqlResultMethod(ctClass);
                    }
                }
            }
        }else {
            hookSqlResultMethod(ctClass);
        }
    }

    /**
     * 用于 hook Sql 检测结果的 next 方法
     *
     * @param ctClass sql 检测结果类
     */
    private void hookSqlResultMethod(CtClass ctClass) throws NotFoundException, CannotCompileException {
        LOGGER.debug("--------- in hookSqlResultMethod Hook");
        String src = getInvokeStaticSrc(SQLResultHook.class, "checkSqlResult",
                "\"" + type.name + "\"" + ",$0", String.class, Object.class);
        insertBefore(ctClass, "next", "()Z", src);
    }

    /**
     * 获取数据库查询结果
     *
     * @param sqlResultSet 数据库查询结果
     */
    public static void checkSqlResult(String server, Object sqlResultSet) {
        LOGGER.debug("----------in SQLResultHook checkSqlResult,result: "+sqlResultSet.toString());
        HashMap<String, Object> params = new HashMap<String, Object>();
        try {
            ResultSet resultSet = (ResultSet) sqlResultSet;
            int queryCount = resultSet.getRow();
            params.put("query_count", queryCount);
            params.put("server", server);
            int rows = resultSet.getMetaData().getColumnCount();
            HashMap<String, Object> rowData = new HashMap<String, Object>();
            for (int i=1;i<=rows;i++){
                rowData.put(resultSet.getMetaData().getColumnName(i),resultSet.getObject(i));
            }
            params.put("result", rowData.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
        HookHandler.doCheck(CheckParameter.Type.SQLResult, params);
    }
}
