package com.gome.bigdata.kafka;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaPartitioner implements Partitioner {

    private static Logger log = LoggerFactory.getLogger(KafkaPartitioner.class);

    public KafkaPartitioner(VerifiableProperties verifiableProperties) {

    }

    @Override
    public int partition(Object key, int numPartitioner) {

        int res = Math.abs(key.hashCode()) % numPartitioner;
        return res;
    }

}
