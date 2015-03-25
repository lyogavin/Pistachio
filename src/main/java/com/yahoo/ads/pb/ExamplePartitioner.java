package com.yahoo.ads.pb;
import org.apache.commons.configuration.Configuration;
import com.yahoo.ads.pb.util.ConfigurationManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExamplePartitioner implements Partitioner {
	private Configuration conf = ConfigurationManager.getConfiguration();
	private static Logger logger = LoggerFactory.getLogger(ExamplePartitioner.class);

    public long getPartition(byte[] id, long totalParition) {

        long shard = id.hashCode()  % totalParition;
        shard = shard < 0 ? shard + totalParition: shard;
        logger.debug("example partitioner shard id {} into parittion {}", id, shard);
        return shard;
    }
}


