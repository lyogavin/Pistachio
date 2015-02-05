/*
 * Copyright 2014 Yahoo! Inc. Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or
 * agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package com.yahoo.ads.pb.store;

import com.yahoo.ads.pb.PistachiosServer;
import java.io.IOException;

import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.rocksdb.*;
import org.rocksdb.util.*;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.concurrent.TimeUnit;
import com.yahoo.ads.pb.store.TLongKyotoCabinetStore;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;
import com.yahoo.ads.pb.kafka.KeyValue;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Timer;
import com.codahale.metrics.JmxReporter;
import com.yahoo.ads.pb.util.ConfigurationManager;



public class RDStore implements Store{
	/*
	private static final InflightCounter storeCounter= new InflightCounter(
	        ProfileServerModule.getCountergroupname(), "Store");;
			*/
	private static Logger logger = LoggerFactory.getLogger(RDStore.class);
	private int partitionId;
	private volatile BlockingQueue<DataOffset>[] incomequeues;
	private Thread[] comsumerThreads;
	private static final int QUEUE_SIZE = 3000;
	final static MetricRegistry metrics = new MetricRegistry();
	final static JmxReporter reporter = JmxReporter.forRegistry(metrics).inDomain("pistachio.metrics.RDStore").build();
	private int threadNum = ConfigurationManager.getConfiguration().getInt("Pistachio.Store.ThreadsPerPartition", 4);
	private final static Meter rdStoreFailures = metrics.meter(MetricRegistry.name(RDStore.class, "RDStoreFailureRequests"));

	private final static Timer rdStoreTimer = metrics.timer(MetricRegistry.name(RDStore.class, "RDStoreStoreTimer"));

	//private static  final IncrementCounter failedStoreCounter = new IncrementCounter(
	//        ProfileServerModule.getCountergroupname(), "FailedStore");
	private RocksDBStore RDprofileStore;
	

	class DataOffset {
		public final KeyValue keyValue;
		public final long offset;

		public DataOffset(KeyValue keyValue, long offset) {
			this.keyValue = keyValue;
			this.offset = offset;
		}

	}
	class Consumer extends Thread {
		private final int partition;

		public Consumer(int i) {
			partition = i;
		}

		@Override
		public void run() {

			logger.info("start receiveing {}, partitionId {}", partition, partitionId);
			while (!this.isInterrupted()) {
                try {
                    DataOffset eventOffset = null;
                    long offset = 0;
                    try {
                        eventOffset = incomequeues[partition].poll(10000, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        logger.error("interrupted by stop");
                        break;
                    }
                    if (eventOffset == null || eventOffset.keyValue == null) {
                        continue;
                    }
                    logger.debug("got data {}/{}/{} from incoming queue", eventOffset.keyValue.key, eventOffset.keyValue.value, eventOffset.offset);

                    final Timer.Context context = rdStoreTimer.time();

                    try {
                        PistachiosServer.getInstance().getRDProfileStore().store(eventOffset.keyValue.key, partitionId, eventOffset.keyValue.value);
                        logger.debug("stored data {}/{}/{}/{}", eventOffset.keyValue.key, partitionId, eventOffset.keyValue.value, eventOffset.offset);
                    } catch (Exception e) {
                        logger.info("error storing data {}/{}/{}/{}", eventOffset.keyValue.key, partitionId, eventOffset.keyValue.value, eventOffset.offset, e);
                        rdStoreFailures.mark();
                    } finally {
                        context.stop();
                    }


                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    rdStoreFailures.mark();
                    logger.error("failed to add user event", e);
                    continue;
                }
            }
		}
	}

	@Override
	public boolean add(byte[] msg, long offset) {
        try {       	
            Kryo kryo = new Kryo();
            Input input = new Input(msg);

            KeyValue keyValue = kryo.readObject(input, KeyValue.class);
            input.close();

            int queueNum = (int) ((keyValue.key / 10771) % threadNum);
            queueNum  = queueNum >= 0 ? queueNum : queueNum + threadNum;

            try {
                incomequeues[queueNum].put(new DataOffset(keyValue, offset));
            } catch (InterruptedException e) {
                logger.error("interrupte exception while add to queue, key {} value {}, offset {}", keyValue.key, keyValue.value, offset, e);
            }


            logger.debug("queued {}:{}, seq id:{}", keyValue.key, keyValue.value, keyValue.seqId);
        } catch (Exception e) {
            logger.info("error ", e);
        }
		return true;

	}

	@Override
	public void update(byte[] msg, long offset) {
		throw new UnsupportedOperationException("do not support update");
	}

	@Override
	public byte[] get(byte[] msg) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("do not support get");
	}

	@Override
	public boolean delete(byte[] msg, long offset) {
		throw new UnsupportedOperationException("do not support delete");
	}

	@Override
	public long getCurrentOffset() {
		try {
	        return RDprofileStore.getOffset(partitionId);
        } catch (Exception e) {
        	logger.error("exception while fetching offset", e);
        	//failed to fetch offset, start from zero.
	        return 0;
        }
	}


    class incomequeueSizeGauge implements Gauge<Integer>{
        BlockingQueue queue;
        public incomequeueSizeGauge(BlockingQueue queue) {
            this.queue = queue;
        }
        @Override
            public Integer getValue() {
                return queue.size();
            }

    }

	@Override
	public boolean open(int partitionId) {
		this.partitionId = partitionId;
		RDprofileStore = PistachiosServer.getInstance().getRDProfileStore();
		try {
	       logger.debug("open store for partition {}",partitionId);
	       RDprofileStore.open(partitionId);
        } catch (Exception e) {
	       logger.error("server open error",e);
	       return false;
        }
		if(incomequeues == null)
			incomequeues = new ArrayBlockingQueue[threadNum];
		comsumerThreads = new Thread[threadNum];
		for (int i = 0; i < threadNum; i++) {
			if(incomequeues[i] == null)
				incomequeues[i] = new ArrayBlockingQueue<DataOffset>(QUEUE_SIZE);
			comsumerThreads[i] = new Consumer(i);
			comsumerThreads[i].start();
            metrics.register(MetricRegistry.name(RDStore.class, "RDStore incoming queue" + partitionId + "/" + i, "size"),
                    new incomequeueSizeGauge(incomequeues[i]));
		}
		reporter.start();
		return true;
	}

	@Override
	public boolean close() {
		RDprofileStore.close();
		for (Thread t : comsumerThreads) {
			t.interrupt();
		}
		return true;
	}

	@Override
	public boolean commitOffset(long offset) {
		try {
	        RDprofileStore.storeOffset( offset, partitionId);
	        return true;
        } catch (Exception e) {
	        // TODO Auto-generated catch block
	        logger.error("exception while storing offset", e);
	        return false;
        }
		
	}

	@Override
    public void flush() {
		try {
	        RDprofileStore.forceFlush(partitionId);
        } catch (Exception e) {
	        // TODO Auto-generated catch block
	        e.printStackTrace();
        }
	    
    }
}
