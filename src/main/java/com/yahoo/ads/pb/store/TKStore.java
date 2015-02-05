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

import kyotocabinet.Visitor;

import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//import org.zeromq.ZMQ.Socket;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
//import com.yahoo.ads.pb.exception.AdmovateException;
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


public class TKStore implements Store{
	/*
	private static final InflightCounter storeCounter= new InflightCounter(
	        ProfileServerModule.getCountergroupname(), "Store");;
			*/
	private static Logger logger = LoggerFactory.getLogger(TKStore.class);
	private int partitionId;
	private volatile BlockingQueue<DataOffset>[] incomequeues;
	private Thread[] comsumerThreads;
	private static final int QUEUE_SIZE = 3000;
	final static MetricRegistry metrics = new MetricRegistry();
	final static JmxReporter reporter = JmxReporter.forRegistry(metrics).inDomain("pistachio.metrics.TKStore").build();
	private int threadNum = ConfigurationManager.getConfiguration().getInt("Pistachio.Store.ThreadsPerPartition", 4);
	private final static Meter tkStoreFailures = metrics.meter(MetricRegistry.name(TKStore.class, "TKStoreFailureRequests"));

	private final static Timer tkStoreTimer = metrics.timer(MetricRegistry.name(TKStore.class, "TKStoreStoreTimer"));

	//private static  final IncrementCounter failedStoreCounter = new IncrementCounter(
	//        ProfileServerModule.getCountergroupname(), "FailedStore");
	private TLongKyotoCabinetStore profileStore;
	
	static {
		//storeCounter.register();
		////failedStoreCounter.register();
	}

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

                    final Timer.Context context = tkStoreTimer.time();

                    try {
                        PistachiosServer.getInstance().getTKProfileStore().store(eventOffset.keyValue.key, partitionId, eventOffset.keyValue.value);
                        logger.debug("stored data {}/{}/{}/{}", eventOffset.keyValue.key, partitionId, eventOffset.keyValue.value, eventOffset.offset);
                    } catch (Exception e) {
                        logger.info("error storing data {}/{}/{}/{}", eventOffset.keyValue.key, partitionId, eventOffset.keyValue.value, eventOffset.offset, e);
                        tkStoreFailures.mark();
                    } finally {
                        context.stop();
                    }


                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    tkStoreFailures.mark();
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
		//storeCounter.begin();
		//try {
			/*
			UserEvent userEvent = UserEvent.parseFrom(msg);
			long userId = userEvent.getUserId();
			logger.debug("Received store request for user {}, user event {}",
			        userId, userEvent.toString());
			*/

			// validate user belong to current server
			/*
			if (ProfileUtil.getPartitionFromUserId(userId) != partitionId) {
				failedStoreCounter.increment();
			}
			*/

/*
			byte[] compressedBytes = ProfileServerModule.getInstance().getUserProfile(userId);
			// for offline data loading, ignore user event if user does not
			// exist on profile server
			if (userEvent.getFromOfflineData()
			        && ArrayUtils.isEmpty(compressedBytes)) {
				logger.debug("Ignore non-exist user profile or offline data loading use case");
				return true;
			}

			RuntimeUserProfile profile = new RuntimeUserProfile(compressedBytes);
			if (profile.getProfile() == null || profile.getProfile().getStoreVersion() < offset) {
				if(profile.getProfile() != null && profile.getProfile().getStoreVersion()>0){
					logger.debug("user {} version {}",userId,  profile.getProfile().getStoreVersion());
				}
				// only process user events when user has not been opt out
				if (!profile.isOptOut()) {
					profile.addUserEvent(userEvent);
					profile = new RuntimeUserProfile(profile.getProfile()
					        .toBuilder().setStoreVersion(offset).build());
					//todo byte offset
					logger.debug("Start storing user profile");
					ProfileServerModule.getInstance().storeUserProfile(userId,
					        profile.getCompressedBytes());
					logger.debug("Finished storing user profile");
				} else {
					logger.debug("User opts out, skip storing user profile");
				}
			}else{
				logger.debug("this message has been processed");
			}
			*/


		//} 
		/*
		catch (InvalidProtocolBufferException e) {
			// add some id log
			//failedStoreCounter.increment();
			logger.error("failed to add user event, user id {}, format not correct",e);
			return false;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			//////failedStoreCounter.increment();
			logger.error("failed to add user event",e);
			return false;
		}
		*/
		return true;

	}

	@Override
	public void update(byte[] msg, long offset) {
		throw new UnsupportedOperationException("do not support update");
	}

	@Override
	public byte[] get(byte[] msg) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("do not support update");
	}

	@Override
	public boolean delete(byte[] msg, long offset) {
		throw new UnsupportedOperationException("do not support update");
	}

	@Override
	public long getCurrentOffset() {
		try {
	        return profileStore.getOffset( partitionId);
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
		profileStore = PistachiosServer.getInstance().getTKProfileStore();
		try {
	       logger.debug("open store for partition {}",partitionId);
	        profileStore.open(partitionId);
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
            metrics.register(MetricRegistry.name(TKStore.class, "TKStore incoming queue" + partitionId + "/" + i, "size"),
                    new incomequeueSizeGauge(incomequeues[i]));
		}
		reporter.start();
		return true;
	}

	@Override
	public boolean close() {
		profileStore.close();
		for (Thread t : comsumerThreads) {
			t.interrupt();
		}
		return true;
	}

	@Override
	public boolean commitOffset(long offset) {
		try {
	        profileStore.storeOffset( offset, partitionId);
	        return true;
        } catch (Exception e) {
	        // TODO Auto-generated catch block
	        logger.error("exception while storing offset", e);
	        return false;
        }
		
	}

	/*
	@Override
    public void transfer(final Socket sender) {
		
		class ProfileTransfer implements Visitor {

			@Override
            public byte[] visit_empty(byte[] arg0) {
				return Visitor.NOP;
            }

			@Override
            public byte[] visit_full(byte[] arg0, byte[] arg1) {
				if(!sender.send(arg1, 0)){
					throw new TransferSenderTimeOutException("transfer time out when sending " + Convert.bytesToLong(arg0));
				}
				return Visitor.NOP;
            }
			
		}
		try{
			profileStore.iterateWithReadLock(new ProfileTransfer());
		}catch(TransferSenderTimeOutException e){
			logger.error("transfer failed",e);
		}
    }
	*/

	@Override
    public void flush() {
		try {
	        profileStore.forceFlush(partitionId);
        } catch (Exception e) {
	        // TODO Auto-generated catch block
	        e.printStackTrace();
        }
	    
    }
}
