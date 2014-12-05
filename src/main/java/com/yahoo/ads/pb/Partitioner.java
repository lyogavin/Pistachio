package com.yahoo.ads.pb;

public interface Partitioner {
    public long getPartition(long key, long totalParition);
}
