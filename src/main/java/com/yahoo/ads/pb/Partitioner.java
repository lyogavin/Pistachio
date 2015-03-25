package com.yahoo.ads.pb;

public interface Partitioner {
    public long getPartition(byte[] key, long totalParition);
}
