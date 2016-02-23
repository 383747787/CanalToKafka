package com.gome.bigdata.sink;

import com.gome.bigdata.utils.AnalysisBinLog;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;

/**
 * Created by lujia on 2015/6/19.
 */
public class GomeKafkaSink extends AbstractSink implements Configurable {
    private static Logger log = LoggerFactory.getLogger(GomeKafkaSink.class);

    private Context context;
    private Properties parameters;
    private Producer<String, String> producer;

    private static final String PARTITION_KEY_NAME = "partition.key";
    private static final String CUSTOME_TOPIC_KEY_NAME = "topic";
    private static final String DEFAULT_ENCODING = "UTF-8";

    @Override
    public void configure(Context context) {
        log.info("------------Start KafkaSink Configuration--------------------");
        this.context = context;
        ImmutableMap<String, String> props = context.getParameters();
        this.parameters = new Properties();
        for (Map.Entry<String, String> entry : props.entrySet()) {
            this.parameters.put(entry.getKey(), entry.getValue());
        }
        this.parameters.put("partitioner.class", "com.gome.bigdata.kafka.KafkaPartitioner");
    }

    @Override
    public synchronized void start() {
        log.info("------------Start KafkaSink --------------------");
        super.start();
        ProducerConfig config = new ProducerConfig(this.parameters);
        this.producer = new Producer<String, String>(config);
    }

    @Override
    public Status process() throws EventDeliveryException {
        Status status = null;
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        try {
            transaction.begin();
            Event event = channel.take();
            if (event != null) {
                String partitionKey;
                String topic = Preconditions.checkNotNull((String) this.parameters.get(CUSTOME_TOPIC_KEY_NAME),
                        "topic name is required");
                String eventData = new String(event.getBody(), DEFAULT_ENCODING);
                ArrayList<String> dataList = AnalysisBinLog.parseOperations(eventData);
                for (String data : dataList) {
                    partitionKey = AnalysisBinLog.getPartitionKey(data);
                    KeyedMessage<String, String> kafkaData = new KeyedMessage<String, String>(topic, partitionKey, data);
                    producer.send(kafkaData);
                }
//                log.info("Sending Message to Kafka : [" + topic + ":" + eventData + "]");

                transaction.commit();
                log.info("Send message success");
                status = Status.READY;
            } else {
                transaction.rollback();
                status = Status.BACKOFF;
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Send message failed!", e);
            transaction.rollback();
            status = Status.BACKOFF;
        } finally {
            transaction.close();
        }
        return status;
    }

    @Override
    public void stop() {
        producer.close();
    }
}
