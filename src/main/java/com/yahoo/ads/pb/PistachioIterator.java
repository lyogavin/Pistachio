package com.yahoo.ads.pb;

import java.util.Random;

import javax.annotation.Nullable;

import com.yahoo.ads.pb.exception.MasterNotFoundException;
import com.yahoo.ads.pb.kafka.KeyValue;

public interface PistachioIterator {

    public KeyValue getNext() throws MasterNotFoundException, Exception;
    public void jump(byte[] key) throws MasterNotFoundException, Exception;

}
