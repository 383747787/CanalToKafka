import com.alibaba.fastjson.JSONArray;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.Message;
import com.gome.bigdata.utils.AnalysisBinLog;
import org.slf4j.MDC;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by lujia on 2015/6/10.
 */
public class CanalClient {

    public static void main(String[] args) {

        String destination = "example";
        CanalConnector connector = CanalConnectors.newSingleConnector(
                new InetSocketAddress("10.126.53.219", 11112), destination, "",
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
                        twoMap = AnalysisBinLog.analysisBinLog(message.getEntries());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    if (twoMap != null && twoMap.size() != 0) {
                        oneMap.put("total", twoMap);
                        System.out.println(twoMap.size());
                        String JsonString = JSONArray.toJSON(oneMap)
                                .toString();
                        String JsonStringFinally = JsonString.substring(1,
                                JsonString.length() - 1);

                        System.out.println(JsonStringFinally);
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
}
