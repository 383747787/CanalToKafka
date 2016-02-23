package com.gome.bigdata.utils;


import java.net.InetSocketAddress;
import java.util.*;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.gome.bigdata.attr.CanalClientConf;
import com.gome.bigdata.attr.TableFilterConf;
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

        String destination = "3pp";
        CanalConnector connector = CanalConnectors.newSingleConnector(
                new InetSocketAddress("10.58.47.235", 21111), destination, "",
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
                        System.out.println(twoMap);
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
//                    sql = rowChage.getSql() + SystemUtils.LINE_SEPARATOR;
                    log.info("Unacceptable operation: " + eventType);
                    continue;
                }

                sql = rowChage.getSql() + SystemUtils.LINE_SEPARATOR;

                String tableName = entry.getHeader().getTableName();
                String databases = entry.getHeader().getSchemaName();
                boolean isTotalImport = CanalClientConf.IS_TOTAL_IMPORT;
                String includeTable = CanalClientConf.INCLUDE_TABLES;
                String includeDataBase = CanalClientConf.INCLUDE_DATABASE;

                boolean isInclude = TableFilterConf.IS_FIELD_INCLUDE;
                boolean isExclude = TableFilterConf.IS_FIELD_EXCLUDE;

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
                            createInsertMap(rowData, threeMap, tableName, isInclude, isExclude);
                        } else if (eventType == EventType.UPDATE) {
                            log.info("Event Type: UPDATE");
                            createUpdateMap(rowData, threeMap, tableName, isInclude, isExclude);
                        } else {
                            log.warn("What's this?");
                        }
                        if (threeMap != null && threeMap.size() > 5) {
//                            log.info("---Three Map : " + threeMap.toString());
                            twoMap.put(i.toString(), threeMap);
                            i++;
                        }
                    }
                    log.info("*********************");
                    log.info("sucess_sql : " + sql);
                    log.info("*********************");
                } catch (Exception e) {
                    log.error("*********************");
                    log.error("fail_sql : " + sql);
                    log.error("*********************");
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
            String name = column.getName().toUpperCase();
            String value = column.getValue();
            if (StringUtils.isEmpty(value)) {
                fourMap.put(name, "NULL");
            } else {
                fourMap.put(name, value);
            }
            boolean isKey = column.getIsKey();
            if (isKey == true) {
                primarykeys = primarykeys + name + ",";
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

    public static Map createInsertMap(RowData rowData, Map threeMap, String tableName, boolean isInclude, boolean isExclude)
            throws Exception {
        Map fourMap = new HashMap();
        List<Column> columns = rowData.getAfterColumnsList();
        String primarykeys = "";
        for (Column column : columns) {
            String name = column.getName().toUpperCase();
            String value = column.getValue();
            boolean isKey = column.getIsKey();

            if (isInclude) {
                if (!(TableFilterConf.FIELD_INCLUDE_TABLES_JSON.getString(tableName).contains(name.toLowerCase()) || isKey)) {
                    continue;
                }
            } else if (isExclude) {
                if (TableFilterConf.FIELD_EXCLUDE_TABLES_JSON.getString(tableName).contains(name.toLowerCase())) {
                    continue;
                }
            }

            //lujia modified
            if (StringUtils.isEmpty(value)) {
                fourMap.put(name, "NULL");
            } else {
                fourMap.put(name, value);
            }

            if (isKey == true) {
                primarykeys = primarykeys + name + ",";
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

    public static Map createUpdateMap(RowData rowData, Map threeMap, String tableName, boolean isInclude, boolean isExclude)
            throws Exception {
        boolean isPk = false;
        Map fourMap = new HashMap();
        List<Column> columnsBefore = rowData.getBeforeColumnsList();
        List<Column> columnsAfter = rowData.getAfterColumnsList();
        String primarykeys = "";
        boolean isPkUpdated = false;

        for (Column column : columnsAfter) {
            String name = column.getName().toUpperCase();
            String value = column.getValue();
            boolean updated = column.getUpdated();
            boolean isKey = column.getIsKey();
            if (isInclude) {
                if (!(TableFilterConf.FIELD_INCLUDE_TABLES_JSON.getString(tableName).contains(name.toLowerCase()) || isKey)) {
                    continue;
                }
            } else if (isExclude) {
                if (TableFilterConf.FIELD_EXCLUDE_TABLES_JSON.getString(tableName).contains(name.toLowerCase())) {
                    continue;
                }
            }

            if (StringUtils.isEmpty(value)) {
                fourMap.put(name, "NULL");
            } else {
                fourMap.put(name, value);
            }

            if (isKey) {
                primarykeys = primarykeys + name + ",";
                if (updated) {
                    isPkUpdated = true;
                }
            }
        }

        if (isPkUpdated) {
            threeMap.put("META-PERATEIONTYPE", "UPDATE_FIELDCOMP_PK");
        } else {
            threeMap.put("META-PERATEIONTYPE", "UPDATE_FIELDCOMP");
        }

        for (Column column : columnsBefore) {
            String name = column.getName().toUpperCase();
            String value = column.getValue();
            boolean isKey = column.getIsKey();
            if (isInclude) {
                if (!(TableFilterConf.FIELD_INCLUDE_TABLES_JSON.getString(tableName).contains(name.toLowerCase()) || isKey)) {
                    continue;
                }
            } else if (isExclude) {
                if (TableFilterConf.FIELD_EXCLUDE_TABLES_JSON.getString(tableName).contains(name.toLowerCase())) {
                    continue;
                }
            }

            if (StringUtils.isEmpty(value)) {
                fourMap.put(name + "_BEFORE", "NULL");
            } else {
                fourMap.put(name + "_BEFORE", value);
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

    public static Map initcol(String tableName, String type, String databases)
            throws Exception {
        Map threeMap = new HashMap();
        String ids = null;
        threeMap.put("META-OWNER", CanalClientConf.ORACLE_OWNER);
        threeMap.put("META-TABLE", tableName.toUpperCase());
        threeMap.put("META-PERATEIONTYPE", type.toUpperCase());
        threeMap.put("META-PRIMARYKEY", "");
        threeMap.put("META-DATABASE", databases.toUpperCase());
        return threeMap;
    }

    /**
     * 把原始json解析成单行操作，按照顺序保存
     *
     * @param msg
     * @return 操作顺序
     */
    public static ArrayList<String> parseOperations(String msg) {
        ArrayList<String> jsonList = new ArrayList<String>();
        String obj = "";
        if (msg.trim().startsWith("[")) {
            obj = msg.trim().substring(1, msg.length() - 1);
        } else {
            obj = msg.trim();
        }

        JSONObject jsonTotal = JSON.parseObject(obj);
        JSONObject content = jsonTotal.getJSONObject("total");

        //todo 跟增涛确定是否一定是从"1"开始的
        for (int i = 1; i < content.keySet().size() + 1; i++) {
            jsonList.add(content.getString(String.valueOf(i)));
        }
        return jsonList;
    }

    /**
     * 根据Json数据获取出 数据库名和表面组合成 partitionKey的关键字，发到kafka的不同partition中
     *
     * @param jsonObj josn格式的opt
     * @return database.table
     */
    public static String getPartitionKey(String jsonObj) {
        JSONObject jsonOpt = JSON.parseObject(jsonObj);
        StringBuilder stringOpt = new StringBuilder();
        stringOpt.append(jsonOpt.getString("META-DATABASE")).append(".").append(jsonOpt.getString("META-TABLE"));
        return stringOpt.toString();
    }

}
