/*
 * Copyright 2017-2018 Baidu Inc.
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
import com.baidu.openrasp.hook.AbstractClassHook;
import com.baidu.openrasp.messaging.LogTool;
import com.baidu.openrasp.plugin.checker.CheckParameter;
import com.baidu.openrasp.tool.Reflection;
import com.baidu.openrasp.tool.annotation.HookAnnotation;
import com.google.gson.Gson;
import javassist.*;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.log4j.helpers.LogLog;

import java.io.IOException;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;

/**
 * @description: Hbase 查询Hook点
 * @author: ldx
 * @create: 2023/03/16 14:21
 */
@HookAnnotation
public class HbaseSQLResultHook extends AbstractClassHook {
    private static final Logger LOGGER = Logger.getLogger(HbaseSQLResultHook.class.getName());
    private static final String SQL_TYPE_HBASE = "hbase";
    private String className;
    private String type;
    private String resultType;

    @Override
    public boolean isClassMatched(String className) {
        if ("org/apache/hadoop/hbase/client/ResultScanner".equals(className)) {
            this.type = SQL_TYPE_HBASE;
            this.className = className;
            this.resultType = "ResultScanner";
            return true;
        }

        if ("org/apache/hadoop/hbase/client/Table".equals(className)) {
            this.type = SQL_TYPE_HBASE;
            this.className = className;
            this.resultType = "Result";
            return true;
        }

        return false;
    }

    @Override
    public String getType() {
        return "hbaseSqlResult";
    }

    @Override
    protected void hookMethod(CtClass ctClass) throws IOException, CannotCompileException, NotFoundException {
        if (this.resultType.equals("ResultScanner")) {
            LOGGER.debug("--------- in hbaseResultScanner Hook");
//            CtField field = CtField.make("public static boolean hookFirstRow = true;", ctClass);
//            ctClass.addField(field);

//            CtMethod iteratorMethod = ctClass.getDeclaredMethod("iterator");
//            CtMethod nextMethod = iteratorMethod.getReturnType().getDeclaredMethod("next");


            String getScannerNextMethodDesc = "()Lorg/apache/hadoop/hbase/client/Result;";
            String getScannerSrc = getInvokeStaticSrc(HbaseSQLResultHook.class, "checkSqlResult",
                    "\"" + type + "\"" + ",$_", String.class, Object.class);
            insertBefore(ctClass, "next", getScannerNextMethodDesc, getScannerSrc);
        }else if (this.resultType.equals("Result")){
//            LogLog.debug("--------- in hbaseResult Hook");
            String getMethodDesc = "()Lorg/apache/hadoop/hbase/client/Result;";
            String getSrc = getInvokeStaticSrc(HbaseSQLResultHook.class, "checkSqlResult",
                    "\"" + type + "\"" + ",$_", String.class, Object.class);
            insertAfter(ctClass, "get", getMethodDesc, getSrc);
        }
    }

//    public static boolean hookFirstRow = true;
//    public static void checkSqlAllResult(String server, Object scannerResult) {
//        HashMap<String, Object> params = new HashMap<String, Object>();
//        try {
//            LogLog.debug("--------- in checkSqlAllResult,hookFirstRow= "+hookFirstRow);
//            Result r = (Result) scannerResult;
//            HashMap<String, String> results = new HashMap<String, String>();
//            if (hookFirstRow == true) {
//                hookFirstRow = false;
//                List<Cell> cells = r.listCells();
//                // 遍历 KeyValue 实例
//                for (Cell cell : cells) {
//                    // 获取列限定符
//                    byte[] qualifierBytes = CellUtil.cloneQualifier(cell);
//                    String qualifier = Bytes.toString(qualifierBytes);
//
//                    // 获取值
//                    byte[] valueBytes = CellUtil.cloneValue(cell);
//                    String value = Bytes.toString(valueBytes);
//
//                    results.put(qualifier, value);
//                }
//            }
//            params.put("server", server);
//            params.put("result", results);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//        HookHandler.doCheck(CheckParameter.Type.HbaseSQLResult, params);
//    }

    public static void checkSqlResult(String server, Object scannerResult) {
        LOGGER.info("--------------in checkSqlResult,server= "+server+"scannerResult: "+scannerResult);
        HashMap<String, Object> params = new HashMap<String, Object>();
        try {
            Result r = (Result) scannerResult;
            HashMap<String, String> result = new HashMap<String, String>();

            List<Cell> cells = r.listCells();
            // 遍历 KeyValue 实例
            for (Cell cell : cells) {
                // 获取列限定符
                byte[] qualifierBytes = CellUtil.cloneQualifier(cell);
                String qualifier = Bytes.toString(qualifierBytes);

                // 获取值
                byte[] valueBytes = CellUtil.cloneValue(cell);
                String value = Bytes.toString(valueBytes);

                result.put(qualifier, value);
            }
            params.put("server", server);
            params.put("result", result);
            LOGGER.info("----------in checkSqlResult,result: "+result);
        } catch (Exception e) {
            e.printStackTrace();
        }

        HookHandler.doCheck(CheckParameter.Type.HbaseSQLResult, params);
    }


}
