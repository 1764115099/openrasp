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
import com.baidu.openrasp.plugin.checker.CheckParameter;
import com.baidu.openrasp.tool.annotation.HookAnnotation;
import javassist.*;
import org.apache.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;

/**
 * @description: Hbase 查询Hook点
 * @author: ldx
 * @create: 2023/03/16 14:21
 */
@HookAnnotation
public class HbaseSQLResultHook extends AbstractClassHook {
    private static final Logger LOGGER = Logger.getLogger("com.iie.rasp.hook.sql.HbaseSQLResultHook");
    private static final String SQL_TYPE_HBASE = "hbase";
    private String className;
    private String type;
    private String resultType;

    /**
     * (none-javadoc)
     *
     * @see com.baidu.openrasp.hook.AbstractClassHook#isClassMatched(String)
     */
    @Override
    public boolean isClassMatched(String className) {
        LOGGER.debug("########### in isClassMatched Hook className: "+className);
        if ("org/apache/hadoop/hbase/client/HTable".equals(className)) {
            this.type = SQL_TYPE_HBASE;
            this.className = className;
            this.resultType = "Result";
            LOGGER.debug("----------- hook HTable");
            return true;
        }

        if ("org/apache/hadoop/hbase/client/CompleteScanResultCache".equals(className)) {
            this.type = SQL_TYPE_HBASE;
            this.className = className;
            this.resultType = "ResultScanner";
            LOGGER.debug("----------- hook CompleteScanResultCache");
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
            CtMethod loadResultsToCacheMethod=null;
            CtMethod addAndGetMethod=null;
            //Hbase1.x为loadResultsToCache方法
            try {
                loadResultsToCacheMethod = ctClass.getDeclaredMethod("loadResultsToCache");
            }catch (NotFoundException e){
                LOGGER.debug("--------- in hbaseResultScanner 不存在 loadResultsToCacheMethod 方法，应该为Hbase2！");
            }
            if(loadResultsToCacheMethod != null){
                String getScannerResultCacheMethodDesc1 = "([Lorg/apache/hadoop/hbase/client/Result;Z)V";
                String getScannerSrc1 = getInvokeStaticSrc(HbaseSQLResultHook.class, "getSqlResult",
                        "\"" + type + "\"" + ",$1", String.class, Object.class, Object.class);
                insertAfter(ctClass, "loadResultsToCache", getScannerResultCacheMethodDesc1, getScannerSrc1);
            }

            //Hbase2.x为addAndGet方法
            try {
                addAndGetMethod = ctClass.getDeclaredMethod("addAndGet");
            }catch (NotFoundException e){
                LOGGER.debug("--------- in hbaseResultScanner 不存在 addAndGetMethod 方法，应该为Hbase1！");
            }
            if(addAndGetMethod != null){
                String getScannerResultCacheMethodDesc2 = "([Lorg/apache/hadoop/hbase/client/Result;Z)[Lorg/apache/hadoop/hbase/client/Result;";
                String getScannerSrc2 = getInvokeStaticSrc(HbaseSQLResultHook.class, "getSqlResult",
                        "\"" + type + "\"" + ",$1", String.class, Object.class, Object.class);
                insertAfter(ctClass, "addAndGet", getScannerResultCacheMethodDesc2, getScannerSrc2);
            }

        }else if (this.resultType.equals("Result")){
            LOGGER.debug("--------- in hbaseResult Hook");
            String getMethodDesc = "(Lorg/apache/hadoop/hbase/client/Get;)Lorg/apache/hadoop/hbase/client/Result;";
            String getSrc = getInvokeStaticSrc(HbaseSQLResultHook.class, "checkSqlResult",
                    "\"" + type + "\"" + ",$_", String.class, Object.class);
            insertAfter(ctClass, "get", getMethodDesc, getSrc);
        }
    }

    public static void getSqlResult(String server, Object[] hookResults) {
        LOGGER.debug("--------------in HbaseSQLResultHook getSqlResult, server= " + server + "result= " + hookResults[0].toString());
        HashMap<String, Object> params = new HashMap<String, Object>();
        try {
            if(!hookResults[0].toString().contains("info:seqnumDuringOpen")){

//              强制类型转换为Result
                Class<?> iieResultClass = Class.forName("org.apache.hadoop.hbase.client.Result");
                Object iieResultObj = iieResultClass.cast(hookResults[0]);
                LOGGER.info("--------------in HbaseSQLResultHook getSqlResult, iieResultObj= " + iieResultObj.toString());

//				获取Cells
                Method iieListCellsMethod = iieResultClass.getMethod("listCells");
                List<Object> iieListCells = (List<Object>) iieListCellsMethod.invoke(iieResultObj);
                LOGGER.info("--------------in HbaseSQLResultHook getSqlResult, iieListCells= " + iieListCells.toString()+", cell1: "+iieListCells.get(0));

//				将cell强制类型转换
                Class<?> iieCellClass = Class.forName("org.apache.hadoop.hbase.Cell");
                Object iieCellObj = iieCellClass.cast(iieListCells.get(0));
                LOGGER.info("--------------in HbaseSQLResultHook getSqlResult, iieCellObj= " + iieCellObj.toString());

//              获取value对象
                Class<?> iieCellUtilClass = Class.forName("org.apache.hadoop.hbase.CellUtil");
                Method iieCloneValueMethod = iieCellUtilClass.getMethod("cloneValue", iieCellClass);
                Object iieValue = iieCloneValueMethod.invoke(iieCellUtilClass, iieCellObj);
                LOGGER.info("--------------in HbaseSQLResultHook getSqlResult, iieValue= " + iieValue.toString());

//				转换成byte[]
                // 创建字节输出流和对象输出流
                ByteArrayOutputStream iieByteOut = new ByteArrayOutputStream();
                ObjectOutputStream iieObjOut = new ObjectOutputStream(iieByteOut);

                // 将对象写入对象输出流
                iieObjOut.writeObject(iieValue);
                iieObjOut.flush();

                // 获取字节数组
                byte[] iieByteArray = iieByteOut.toByteArray();

                // 关闭流
                iieObjOut.close();
                iieByteOut.close();

//              字节转换方法
				Class<?> iieBytesClass = Class.forName("org.apache.hadoop.hbase.util.Bytes");
				Method iieToString = iieBytesClass.getMethod("toString",byte[].class);
				Object iieHbaseResult = iieToString.invoke(iieBytesClass, iieByteArray);
                LOGGER.info("--------------in HbaseSQLResultHook getSqlResult, iieHbaseResult= " + iieHbaseResult.toString());
                params.put("server", server);
                params.put("result", hookResults[0].toString());
                params.put("resultValue", iieHbaseResult.toString());
            } else {
                params.put("result", "iieIgnore");
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        HookHandler.doCheck(CheckParameter.Type.HbaseSQLResult, params);
    }

    public static void checkSqlResult(String server, Object hookResult) {
        LOGGER.debug("--------------in HbaseSQLResultHook checkSqlResult,server= " + server + ", scannerResult: " + hookResult.toString());
        HashMap<String, Object> params = new HashMap<String, Object>();
        try {
            params.put("server", server);
            params.put("result", hookResult.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }

        HookHandler.doCheck(CheckParameter.Type.HbaseSQLResult, params);
    }


}
