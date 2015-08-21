package com.gome.bigdata.utils;


import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSONArray;
import com.gome.bigdata.attr.CanalClientConf;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.SystemUtils;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;

public class AnalysisBinLog {
    private static Logger log = LoggerFactory.getLogger(AnalysisBinLog.class);

    public static void main(String[] args) {

        String destination = "example";
        CanalConnector connector = CanalConnectors.newSingleConnector(
                new InetSocketAddress("10.126.53.216", 11111), destination, "",
                "");
        int batchSize = 1 * 1024;
        try {
            MDC.put("destination", destination);
            connector.connect();
            connector.subscribe();
            while (true) {
                Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据
                long batchId = message.getId();
                int size = message.getEntries().size();
                if (batchId == -1 || size == 0) {
                    // System.out.println("fail");
                } else {
                    Map oneMap = new HashMap();
                    Map twoMap = null;
                    try {
                        // twoMap = new HashMap();
                        twoMap = analysisBinLog(message.getEntries());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    if (twoMap != null && twoMap.size() != 0) {
                        oneMap.put("total", twoMap);
                        log.info("Map: " + twoMap.size());
                        String JsonString = JSONArray.toJSON(oneMap)
                                .toString();
                        String JsonStringFinally = JsonString.substring(1,
                                JsonString.length() - 1);

                        log.info("JsonStringFinally:" + JsonStringFinally);
                    }

                }
                connector.ack(batchId); // 提交确认
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            connector.disconnect();
            MDC.remove("destination");
        }
    }

    public static Map analysisBinLog(List<Entry> entrys) throws Exception {
        Map twoMap = new HashMap();
        Integer i = 1;
        String sql = "";
        for (Entry entry : entrys) {
            if (entry.getEntryType() == EntryType.ROWDATA) {
                RowChange rowChage = null;
                rowChage = RowChange.parseFrom(entry.getStoreValue());
                EventType eventType = rowChage.getEventType();

//                log.info("--T E S T--");
//                log.info("Entry: " + entry.toString());
//                log.info("EventType: " + eventType);
//                log.info("DDL: " + rowChage.getIsDdl());
//                log.info("Row Change: " + rowChage.toString());

//                if (eventType == EventType.QUERY || rowChage.getIsDdl()) {
//                    sql = rowChage.getSql() + SystemUtils.LINE_SEPARATOR;
//                    log.info("query or ddl sql --> : " + sql);
//                    continue;
//                }
                if (rowChage.getIsDdl() || (eventType != EventType.INSERT && eventType != EventType.DELETE && eventType != EventType.UPDATE)) {
                    sql = rowChage.getSql() + SystemUtils.LINE_SEPARATOR;
                    log.info("Unacceptable operation: " + eventType);
                    continue;
                }

                String tableName = entry.getHeader().getTableName();
                String databases = entry.getHeader().getSchemaName();
                boolean isTotalImport = CanalClientConf.IS_TOTAL_IMPORT;
                String includeTable = CanalClientConf.INCLUDE_TABLES;
                String includeDataBase = CanalClientConf.INCLUDE_DATABASE;

                if (!includeDataBase.contains(databases)) {
                    log.info("*********************");
                    log.info("ignore_sql : " + sql);
                    log.info("*********************");
                    continue;
                }

                if (isTotalImport == false && !includeTable.contains(tableName)) {
                    log.info("*********************");
                    log.info("ignore_sql : " + sql);
                    log.info("*********************");
                    continue;
                }

                try {
                    for (RowData rowData : rowChage.getRowDatasList()) {
                        Map threeMap = initcol(tableName, eventType.name(), databases);
                        if (eventType == EventType.DELETE) {
                            log.info("Event Type: DELETE");
                            createDeleteMap(rowData, threeMap);
                        } else if (eventType == EventType.INSERT) {
                            log.info("Event Type: INSERT");
                            createInsertMap(rowData, threeMap);
                        } else if (eventType == EventType.UPDATE) {
                            log.info("Event Type: UPDATE");
                            createUpdateMap(rowData, threeMap);
                        } else {

                        }
                        if (threeMap != null && threeMap.size() > 5) {
                            twoMap.put(i.toString(), threeMap);
                            i++;
                        }
                    }
                    log.info("*********************");
                    log.info("sucess_sql : " + sql);
                    log.info("*********************");
                } catch (Exception e) {
                    log.info("*********************");
                    log.info("fail_sql : " + sql);
                    log.info("*********************");
//                    RecordLog.writeLog(GetConfig.getConfigInfomation("SendFailPath"), sql);
                    continue;
                }
            }
        }

        return twoMap;
    }

    public static Map createDeleteMap(RowData rowData, Map threeMap)
            throws Exception {
        Map fourMap = new HashMap();
        List<Column> columns = rowData.getBeforeColumnsList();
        String primarykeys = "";
        for (Column column : columns) {
            String name = column.getName();
            String value = column.getValue();
//            if (StringUtils.isEmpty(value)) {
//                value = " ";
//            }
            boolean updated = column.getUpdated();
            boolean isKey = column.getIsKey();
            if (isKey == true) {
                primarykeys = primarykeys + name + ",";
                fourMap.put(name, value);
            }
        }

        if (primarykeys.equals("") || fourMap == null || fourMap.size() == 0) {
            return null;
        }

        threeMap.put("META-PRIMARYKEY",
                primarykeys.substring(0, primarykeys.length() - 1));
        threeMap.put("META-FILEDVALUE", fourMap);
        return threeMap;
    }

    public static Map createInsertMap(RowData rowData, Map threeMap)
            throws Exception {
        Map fourMap = new HashMap();
        List<Column> columns = rowData.getAfterColumnsList();
        String primarykeys = "";
        for (Column column : columns) {
            String name = column.getName();
            String value = column.getValue();
            //lujia delete
//            if (StringUtils.isEmpty(value)) {
//                value = " ";
//            }
            boolean updated = column.getUpdated();
            boolean isKey = column.getIsKey();
            if (isKey == true) {
                primarykeys = primarykeys + name + ",";
            }
            fourMap.put(name, value);
        }

        if (primarykeys.equals("") || fourMap == null || fourMap.size() == 0) {
            return null;
        }

        threeMap.put("META-PRIMARYKEY",
                primarykeys.substring(0, primarykeys.length() - 1));
        threeMap.put("META-FILEDVALUE", fourMap);
        return threeMap;
    }

    public static Map createUpdateMap(RowData rowData, Map threeMap)
            throws Exception {
        boolean isPk = false;
        Map fourMap = new HashMap();
        List<Column> columnsBefore = rowData.getBeforeColumnsList();
        List<Column> columnsAfter = rowData.getAfterColumnsList();
        String primarykeys = "";
        for (Column column : columnsAfter) {
            String name = column.getName();
            String value = column.getValue();
            //lujia delete
//            if (StringUtils.isEmpty(value)) {
//                value = " ";
//            }
            boolean updated = column.getUpdated();
            boolean isKey = column.getIsKey();
            if (isKey == true) {
                primarykeys = primarykeys + name + ",";
                fourMap.put(name, value);
                if (updated == true && isPk == false) {
                    isPk = true;
                }
            } else {
                if (updated == true) {
                    fourMap.put(name, value);
                }
            }
        }

        if (isPk == true) {
            List keyList = Arrays.asList(primarykeys.split(","));
            for (Column column : columnsBefore) {
                String name = column.getName();
                String value = column.getValue();
                //lujia deleted
//                if (StringUtils.isEmpty(value)) {
//                    value = " ";
//                }
                if (keyList.contains(name)) {
                    fourMap.put(name + "_BEFORE", value);
                }
            }
            threeMap.put("META-PERATEIONTYPE", "UPDATE_FIELDCOMP_PK");
        } else {
            threeMap.put("META-PERATEIONTYPE", "UPDATE_FIELDCOMP");
        }

        if (primarykeys.equals("") || fourMap == null || fourMap.size() == 0) {
            return null;
        }

        threeMap.put("META-PRIMARYKEY",
                primarykeys.substring(0, primarykeys.length() - 1));
        threeMap.put("META-FILEDVALUE", fourMap);
        return threeMap;
    }

    public static Map initcol(String tableName, String type, String databases)
            throws Exception {
        Map threeMap = new HashMap();
        String ids = null;
        threeMap.put("META-OWNER", CanalClientConf.ORACLE_OWNER);
        threeMap.put("META-TABLE", tableName);
        threeMap.put("META-PERATEIONTYPE", type);
        threeMap.put("META-PRIMARYKEY", "");
        threeMap.put("META-DATABASE", databases);
        return threeMap;
    }

}
