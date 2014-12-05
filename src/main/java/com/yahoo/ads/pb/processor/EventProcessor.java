package com.yahoo.ads.pb.processor;

interface EventProcessor {
    public boolean processEvent(byte[] event);
}
