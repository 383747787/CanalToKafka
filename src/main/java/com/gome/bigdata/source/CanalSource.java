package com.gome.bigdata.source;


import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.alibaba.fastjson.JSONArray;
import com.gome.bigdata.attr.CanalClientConf;
import com.gome.bigdata.utils.AnalysisBinLog;
import com.gome.bigdata.utils.PropertiesUtil;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;

import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.Message;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CanalSource extends AbstractSource implements Configurable, PollableSource {
    private static Logger log = LoggerFactory.getLogger(CanalSource.class);
    public Context context;
    public Properties parameters;
    public CanalConnector connector;
    private static final String CONFIG_PATH = "config.path";

    @Override
    public Status process() throws EventDeliveryException {
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("timestamp", String.valueOf(System.currentTimeMillis()));
        Event event = new SimpleEvent();
        try {
            Message message = connector.getWithoutAck(CanalClientConf.FLUME_SOURCE_BATCH_SIZE); // 获取指定数量的数据
            long batchId = message.getId();
            int size = message.getEntries().size();
            if (batchId == -1 || size == 0) {
//                log.warn("Get message NULL! ");
//                System.out.println("Get message NULL! ");
            } else {

                Map oneMap = new HashMap();
                Map twoMap = null;
                try {
                    // twoMap = new HashMap();
                    twoMap = AnalysisBinLog.analysisBinLog(message.getEntries());
                } catch (Exception e) {
                    log.error("Analysis ERROR : " + e.getMessage());
                    e.printStackTrace();
                    return Status.BACKOFF;
                }
                if (twoMap != null && twoMap.size() != 0) {
                    log.info("twoMap.size : " + twoMap.size());
                    oneMap.put("total", twoMap);
                    String JsonString = JSONArray.toJSON(oneMap).toString();
//                    String JsonStringFinally = JsonString.substring(1, JsonString.length() - 1);
                    event.setHeaders(headers);
                    event.setBody(JsonString.getBytes());
                    getChannelProcessor().processEvent(event);
                } else {
                    log.warn("twoMap is null OR twoMap size is 0!");

                }
                connector.ack(batchId); // 提交确认
            }
        } catch (Exception e) {
            e.printStackTrace();
            return Status.BACKOFF;
        }
        return Status.READY;

    }

    @Override
    public synchronized void start() {
        log.info("---------------Start flume--------------------");
        String destination = CanalClientConf.CANAL_DESTINATION;
        String ip = CanalClientConf.CANAL_IP;
        String port = CanalClientConf.CANAL_PORT;
        connector = CanalConnectors.newSingleConnector(new InetSocketAddress(
                ip, Integer.parseInt(port)), destination, "", "");
        connector.connect();
        connector.subscribe();
        super.start();
    }

    @Override
    public synchronized void stop() {
        connector.disconnect();
        super.stop();
    }

    @Override
    public void configure(Context context) {
        log.info("---------------Start Configuration------------------");
        this.context = context;
        ImmutableMap<String, String> props = context.getParameters();
        this.parameters = new Properties();
        for (Map.Entry<String, String> entry : props.entrySet()) {
            this.parameters.put(entry.getKey(), entry.getValue());
        }
        CanalClientConf.CONFIG_PATH = Preconditions.checkNotNull((String) this.parameters.get(CONFIG_PATH));

        CanalClientConf.CANAL_IP = PropertiesUtil.getInstance().getProperty("canal_ip");
        CanalClientConf.CANAL_PORT = PropertiesUtil.getInstance().getProperty("canal_port");
        CanalClientConf.CANAL_DESTINATION = PropertiesUtil.getInstance().getProperty("destination");

        CanalClientConf.IS_TOTAL_IMPORT = Boolean.parseBoolean(PropertiesUtil.getInstance().getProperty("is_total_import"));
        CanalClientConf.INCLUDE_DATABASE = PropertiesUtil.getInstance().getProperty("include_database");
        CanalClientConf.INCLUDE_TABLES = PropertiesUtil.getInstance().getProperty("include_tables");
        CanalClientConf.ORACLE_OWNER = PropertiesUtil.getInstance().getProperty("owner");

        CanalClientConf.FLUME_SOURCE_BATCH_SIZE = Integer.parseInt(PropertiesUtil.getInstance().getProperty("batch_size"));

        log.info("------------------Complete Configuration-----------------------");
    }
}
