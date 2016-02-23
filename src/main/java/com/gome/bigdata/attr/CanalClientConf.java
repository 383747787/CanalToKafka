package com.gome.bigdata.attr;

/**
 * Created by lujia on 2015/6/10.
 */
public class CanalClientConf {

    /**
     * 是否监控所有表操作
     */
    public static boolean IS_TOTAL_IMPORT = true;

    /**
     * 需要监控的mysql schema
     */
    public static String INCLUDE_DATABASE = "bigdata";

    /**
     * 需要监控的表名
     */
    public static String INCLUDE_TABLES = "test";

    /**
     * 保存到Oracle之后的Owner名
     */
    public static String ORACLE_OWNER = "DRG_CORE_PRD";

    /**
     * 部署canal的ip地址
     */
    public static String CANAL_IP = "10.126.53.219";

    /**
     * 部署canal的端口
     */
    public static String CANAL_PORT = "11111";

    /**
     * 部署canal的destination
     */
    public static String CANAL_DESTINATION = "example";

    /**
     * 从canal一次获取数据的量
     */
    public static int FLUME_SOURCE_BATCH_SIZE = 10;

    /**
     * 应用配置文件的地址
     */
    public static String CONFIG_PATH = "config.properties";
}
