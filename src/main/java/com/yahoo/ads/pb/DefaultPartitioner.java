package com.yahoo.ads.pb;
import org.apache.commons.configuration.Configuration;
import com.yahoo.ads.pb.util.ConfigurationManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultPartitioner implements Partitioner {
	private Configuration conf = ConfigurationManager.getConfiguration();
	private static Logger logger = LoggerFactory.getLogger(DefaultPartitioner.class);

    public long getPartition(long id, long totalParition) {

        if (conf.getString("pistachio.partitioner") != null) {
            try {
                Partitioner customizedPartitioner = (Partitioner) Class.forName(conf.getString("pistachio.partitioner")).newInstance();
                return customizedPartitioner.getPartition(id,totalParition);
            } catch (Exception e) {
                logger.debug("error partitioning with customized partitioner, fall back to default", e);
            }
        }

        long shard = id % totalParition;
        shard = shard < 0 ? shard + totalParition: shard;
        return shard;
    }
}

