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

package com.yahoo.ads.pb;

import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;

import java.nio.ByteBuffer;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import com.yahoo.ads.pb.pastperf.PastperfSignalProcess;
import com.yahoo.ads.pb.store.StorePartition;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentHashMap;

import com.yahoo.ads.pb.store.TKStoreFactory;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.codahale.metrics.JmxReporter;

import java.net.InetAddress;

import com.yahoo.ads.pb.store.TLongKyotoCabinetStore;
import com.yahoo.ads.pb.kafka.KeyValue;
import com.yahoo.ads.pb.helix.PartitionHandler;
import com.yahoo.ads.pb.helix.PartitionHandlerFactory;
import com.yahoo.ads.pb.helix.BootstrapOnlineOfflineStateModel;
import com.yahoo.ads.pb.helix.BootstrapOnlineOfflineStateModelFactory;
import com.yahoo.ads.pb.helix.HelixPartitionManager;
import com.yahoo.ads.pb.helix.HelixPartitionSpectator;

import org.apache.helix.HelixManagerFactory;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.controller.GenericHelixController;

//import com.yahoo.ads.pb.platform.perf.IncrementCounter;
//import com.yahoo.ads.pb.platform.perf.InflightCounter;
import com.yahoo.ads.pb.util.ConfigurationManager;
import com.yahoo.ads.pb.util.NativeUtils;
import com.yahoo.ads.pb.util.PistachiosConstants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;

import com.google.common.base.Joiner;

// Generated code

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Properties;

public class PistachiosServer {
	private static Logger logger = LoggerFactory.getLogger(PistachiosServer.class);

	static {
		try {
			NativeUtils.loadLibraryFromJar("/libkyotocabinet.so");
		} catch (Exception e) {
			logger.error("Unable to load libkyotocabinet.so: {}", e);
		}

		try {
			NativeUtils.loadLibraryFromJar("/libjkyotocabinet.so");
		} catch (Exception e) {
			logger.error("Unable to load libjkyotocabinet.so: {}", e);
		}
	}

	final static MetricRegistry metrics = new MetricRegistry();
	final static JmxReporter reporter = JmxReporter.forRegistry(metrics).inDomain("pistachio.metrics").build();

	private final static Meter lookupRequests = metrics.meter(MetricRegistry.name(PistachiosServer.class, "lookupRequests"));
	private final static Meter lookupFailureRequests = metrics.meter(MetricRegistry.name(PistachiosServer.class,
			"lookupFailureRequests"));

	private final static Meter storeRequests = metrics.meter(MetricRegistry.name(PistachiosServer.class, "storeRequests"));
	private final static Meter storeFailureRequests = metrics.meter(MetricRegistry.name(PistachiosServer.class,
			"storeFailureRequests"));

	private final static Meter processRequests = metrics.meter(MetricRegistry.name(PistachiosServer.class, "processRequests"));
	private final static Meter processFailureRequests = metrics.meter(MetricRegistry.name(PistachiosServer.class,
			"processFailureRequests"));

	private final static Meter processBatchRequests = metrics.meter(MetricRegistry.name(PistachiosServer.class,
			"processBatchRequests"));
	private final static Meter processBatchFailureRequests = metrics.meter(MetricRegistry.name(PistachiosServer.class,
			"processBatchFailureRequests"));

	private final static Timer lookupTimer = metrics.timer(MetricRegistry.name(PistachiosServer.class, "lookupTimer"));
	private final static Timer storeTimer = metrics.timer(MetricRegistry.name(PistachiosServer.class, "storeTimer"));
	private final static Timer processTimer = metrics.timer(MetricRegistry.name(PistachiosServer.class, "processTimer"));
	private final static Timer processBatchTimer = metrics.timer(MetricRegistry.name(PistachiosServer.class, "processBatchTimer"));

	static final String PROFILE_BASE_DIR = "Profile.Base";
	static final String PROFILE_NUM_STORE = "Profile.NumStore";
	static final String PROFILE_RECORDS_PER_SERVER = "Profile.RecordsPerServer";
	static final String ZOOKEEPER_SERVER = "Pistachio.ZooKeeper.Server";
	static final String PROFILE_HELIX_INSTANCE_ID = "Profile.Helix.InstanceId";

	private HelixPartitionManager<BootstrapOnlineOfflineStateModel> manager; // for partition management
	private static HelixPartitionSpectator helixPartitionSpectator;

	private TLongKyotoCabinetStore profileStore;

	private static Producer<String, byte[]> kafkaProducer = null;

	private static LinkedBlockingQueue<PistachiosMessage> synchronousQueue = new LinkedBlockingQueue<PistachiosMessage>();

	public PistachiosServer() {
		Configuration conf = ConfigurationManager.getConfiguration();

		int numBatchThread = conf.getInt("Profile.Process.Number.Batch.Thread", 64);

		logger.info("numBatchThread=", numBatchThread);

		ExecutorService executorService = Executors.newFixedThreadPool(numBatchThread);
		executorService.execute(new Runnable() {
			@Override
			public void run() {
				while (true) {
					logger.info("before process synchronousQueue.size()=", synchronousQueue.size());
					try {
						PistachiosMessage message = synchronousQueue.take();
						instance.handler.process(message.partition, ByteBuffer.wrap(message.value));
						logger.info("after process synchronousQueue.size()=", synchronousQueue.size());
					} catch (NoSuchElementException e) {
						logger.info("error: {}", e);
					} catch (InterruptedException e) {
						logger.info("error: {}", e);
					} catch (TException e) {
						logger.info("error: {}", e);
					}
				}
			}
		});

		// executorService.shutdown();
	}

	public static Producer getKafkaProducerInstance() {
		try {
			if (kafkaProducer == null) {
				synchronized (logger) {
					if (kafkaProducer == null) {
						logger.debug("first time using kafka producer, creating it");
						try {
							Properties props = new Properties();
							Configuration conf = ConfigurationManager.getConfiguration();

							@SuppressWarnings("unchecked")
							Iterator<String> iter = conf.getKeys();
							while (iter.hasNext()) {
								String key = iter.next();
								if (key.startsWith("kafka.") == true) {
									String stormSetting = key.substring(6);
									props.put(stormSetting, conf.getString(key));
									logger.info("Strom setting::{}={}", stormSetting, conf.getString(key));
								}
							}

							ProducerConfig kafkaConf = new ProducerConfig(props);

							kafkaProducer = new Producer<String, byte[]>(kafkaConf);
						} catch (Throwable t) {
							logger.error("Exception in creating Producer:", t);
						}
						logger.debug("created kafka producer");
					}
				}
			}
		} catch (Throwable t) {
			logger.error("error creating kafka producer instance", t);
		}

		return kafkaProducer;
	}

	public static class PistachiosHandler implements Pistachios.Iface {
		String storage;

		public ByteBuffer lookup(long partition, long id) throws org.apache.thrift.TException {
			lookupRequests.mark();
			final Timer.Context context = lookupTimer.time();

			try {
				int partitionId = (int) (partition % 256);
				partitionId = partitionId >= 0 ? partitionId : partitionId + 256;
				//return ByteBuffer.wrap(storage.getBytes());

				KeyValue toReturn = null;
				try {
					toReturn = PistachiosServer.storePartitionMap.get(partitionId).getWriteCache().get(id);
				} catch (Exception e) {
					toReturn = null;
				}

				if (toReturn != null) {
					return ByteBuffer.wrap(toReturn.value);
				}

				if (null != PistachiosServer.getInstance().getProfileStore().get(partitionId, id)) {
					return ByteBuffer.wrap(PistachiosServer.getInstance().getProfileStore().get(partitionId, id));
				}

				return ByteBuffer.wrap("".getBytes());
			} catch (Exception e) {
				logger.info("Exception lookup {}", id, e);
				lookupFailureRequests.mark();
				return ByteBuffer.wrap("".getBytes());
			} finally {
				context.stop();
			}
		}

		public boolean store(long partition, long id, ByteBuffer value) throws org.apache.thrift.TException {
			storeRequests.mark();
			final Timer.Context context = storeTimer.time();

			try {
				int partitionId = (int) (partition % 256);
				partitionId = partitionId >= 0 ? partitionId : partitionId + 256;

				long nextSeqId = -1;
				if ((nextSeqId = PistachiosServer.storePartitionMap.get(partitionId).getNextSeqId()) == -1) {
					return false;
				}

				int shard = (int) (partition % 256);
				shard = shard < 0 ? shard + 256 : shard;
				String partitionTopic = ConfigurationManager.getConfiguration().getString("Pistachio.Kafka.TopicPrefix") + (shard);

				KeyValue kv = new KeyValue();
				kv.partition = partitionId;
				kv.key = id;
				kv.seqId = nextSeqId;
				kv.value = value.array();
				KeyedMessage<String, KeyValue> message = new KeyedMessage<String, KeyValue>(partitionTopic, kv);

				getKafkaProducerInstance().send(message);

				logger.info("sent msg {} {} {}, partition current seqid {}", kv.key, new String(kv.value), kv.seqId,
						PistachiosServer.storePartitionMap.get(partitionId).getSeqId());

				if (PistachiosServer.storePartitionMap.get(partitionId) != null
						&& PistachiosServer.storePartitionMap.get(partitionId).getWriteCache() != null) {
					// store
					PistachiosServer.storePartitionMap.get(partitionId).getWriteCache().put(id, kv);

					logger.info("waiting for change to catch up {} {} within gap 200",
							PistachiosServer.storePartitionMap.get(partitionId).getSeqId(), kv.seqId);
					while (kv.seqId - PistachiosServer.storePartitionMap.get(partitionId).getSeqId() > 200) {
						logger.info("waiting for change to catch up {} {} within gap 200",
								PistachiosServer.storePartitionMap.get(partitionId).getSeqId(), kv.seqId);
						Thread.sleep(30);
					}
					return true;
				} else {
					// XXX: what if there is no partition is created
					logger.error("partition was not created", PistachiosServer.storePartitionMap.get(partitionId).getSeqId(),
							kv.seqId);

					return false;
				}

				//PistachiosServer.getInstance().getProfileStore().store(id, value.array());
			} catch (Exception e) {
				logger.info("error storing {} {}", id, value, e);
				storeFailureRequests.mark();
				return false;
			} finally {
				context.stop();
			}
		}

		@SuppressWarnings("unused")
		@Override
		public boolean process(long partition, ByteBuffer value) throws org.apache.thrift.TException {
			processRequests.mark();
			final Timer.Context context = processTimer.time();

			try {
				int partitionId = (int) (partition % 256);
				partitionId = partitionId >= 0 ? partitionId : partitionId + 256;

				PastperfSignalProcess processPartition = null;

				String message = new String(value.array());
				synchronized (logger) {
					processPartition = PistachiosServer.processPartitionMap.get(partitionId);
					if (processPartition == null) {
						try {
							processPartition = new PastperfSignalProcess();
							PistachiosServer.processPartitionMap.put(partitionId, processPartition);
							logger.debug("processPartition init: {}", partitionId);
						} catch (ConfigurationException e) {
							e.printStackTrace();
							return false;
						}
					}
				}

				if (processPartition == null) {
					logger.error("Ubable to get process partition instance: orderId={} partition={}", partition, partitionId);
					return false;
				}

				logger.debug("PistachiosServer.process.processMessage - before");
				logger.debug("message - [{}]", message);
				processPartition.processMessage(message);
				logger.debug("PistachiosServer.process.processMessage - after");

				return true;
			} catch (Exception e) {
				logger.info("error processing {}", value, e);
				processFailureRequests.mark();
				return false;
			} finally {
				context.stop();
			}
		}

		@Override
		public boolean process_batch(ByteBuffer value) {
			logger.info("process_batch");
			processBatchRequests.mark();
			final Timer.Context context = processBatchTimer.time();

			try {
				String input = new String(value.array());

				String[] messages = input.split("" + PistachiosConnectionInfo.CTRL_b);

				for (String message : messages) {
					String[] items = message.split("" + PistachiosConnectionInfo.CTRL_a);
					if (items.length == 2) {
						PistachiosMessage event = new PistachiosMessage(PistachiosMessage.MESSAGE_PROCESS,
								Long.parseLong(items[0]), 0, items[1].getBytes());

						try {
							PistachiosServer.synchronousQueue.put(event);
						} catch (java.lang.InterruptedException e) {
							logger.info("error: {}", e);
						}
					} else {
						logger.info("invalid input: ", message);
					}
				}
				logger.info("PistachiosServer.synchronousQueue.size()=" + PistachiosServer.synchronousQueue.size());

				return true;
			} catch (Exception e) {
				logger.info("error processing {}", value, e);
				processBatchFailureRequests.mark();
				return false;
			} finally {
				context.stop();
			}
		}
	}

	public static PistachiosHandler handler;

	public static Pistachios.Processor processor;

	private static PistachiosServer instance;

	private static HelixManager helixManager;

	private static GenericHelixController controller;// = new GenericHelixController();

	public static ConcurrentHashMap<Integer, StorePartition> storePartitionMap = new ConcurrentHashMap<Integer, StorePartition>();
	public static ConcurrentHashMap<Integer, PastperfSignalProcess> processPartitionMap = new ConcurrentHashMap<Integer, PastperfSignalProcess>();

	public static PistachiosServer getInstance() {
		return instance;
	}

	public static void main(String[] args) {
		try {
			reporter.start();

			// embed helix controller
			Configuration conf = ConfigurationManager.getConfiguration();
			helixManager = HelixManagerFactory.getZKHelixManager("PistachiosCluster", InetAddress.getLocalHost().getHostName(), //conf.getString(PROFILE_HELIX_INSTANCE_ID),
					InstanceType.CONTROLLER, conf.getString(ZOOKEEPER_SERVER));
			helixManager.connect();
			controller = new GenericHelixController();
			helixManager.addConfigChangeListener(controller);
			helixManager.addLiveInstanceChangeListener(controller);
			helixManager.addIdealStateChangeListener(controller);
			helixManager.addExternalViewChangeListener(controller);
			helixManager.addControllerListener(controller);

			instance = new PistachiosServer();
			instance.init();
			handler = new PistachiosHandler();
			processor = new Pistachios.Processor(handler);

			Runnable simple = new Runnable() {
				public void run() {
					simple(processor);
				}
			};

			new Thread(simple).start();
		} catch (Exception x) {
			x.printStackTrace();
		}
	}

	public static void simple(Pistachios.Processor processor) {
		try {
			Configuration conf = ConfigurationManager.getConfiguration();

			TServerTransport serverTransport = new TServerSocket(9090);
			//TServer server = new TSimpleServer(new Args(serverTransport).processor(processor));

			// Use this for a multithreaded server
			TThreadPoolServer.Args args = new TThreadPoolServer.Args(serverTransport);
			args.processor(processor);
			args.minWorkerThreads = conf.getInt("Pistachio.Thrift.MinWorkerThreads", 50);
			args.maxWorkerThreads = conf.getInt("Pistachio.Thrift.MaxWorkerThreads", 200);
			args.stopTimeoutUnit = TimeUnit.SECONDS;
			args.stopTimeoutVal = conf.getInt("Pistachio.Thrift.StopTimeoutVal", 60);
			TServer server = new TThreadPoolServer(args);

			System.out.println("Starting the simple server...");
			server.serve();
		} catch (Exception e) {
			logger.info("error ", e);
			e.printStackTrace();
		}
	}

	public TLongKyotoCabinetStore getProfileStore() {
		return profileStore;
	}

	public boolean init() {
		boolean initialized = false;

		logger.info("Initializing profile server...........");
		try {
			// open profile store
			Configuration conf = ConfigurationManager.getConfiguration();
			profileStore = new TLongKyotoCabinetStore(conf.getString(PROFILE_BASE_DIR), 0, 8,
					conf.getInt("Profile.RecordsPerServer"), conf.getLong("Profile.MemoryPerServer"));

			/*
			*/
			//profileStore.open();
			/*
			hostname = InetAddress.getLocalHost().getHostName();
			if(conf.getBoolean("Profile.Redesign.Firstload", false)){
				String fileName = "profileMapping.json";
				String refDir = "/home/y/libexec/server/refdata/";
				String fullName = refDir + File.separator + fileName;
				FileReader fr = new FileReader(fullName);
				JSONParser parser = new JSONParser();
				ContainerFactory containerFactory = new ContainerFactory() {
					public List creatArrayContainer() {
						return new LinkedList();
					}

					public Map createObjectContainer() {
						return new LinkedHashMap();
					}

				};
				
				Map<String,List<String>> partition2Server = (Map<String,List<String>>) parser.parse(fr,containerFactory);
				Set<String> keys = partition2Server.keySet();
				Set<Integer> partitions = new TreeSet<Integer>();
				for(String key:keys){
					if(partition2Server.get(key).contains(hostname)){
						partitions.add(Integer.parseInt(key));
					}
				}
				String profilePath = "/home/y/libexec/server/profile/store_";
				String profileFilePrefix = "db_";
				String profileFileSurfix = ".kch";
				String profileWalFileSurfix = ".wal";
				int index = 0;
				for(int partition: partitions){
					File partitionDir = new File(profilePath+index);
					if(partitionDir.isDirectory()){
						logger.info("rename from {} to {}",profilePath+index,profilePath+partition);
						partitionDir.renameTo( new File(profilePath+partition));
					}
					File storeFile = new File(profilePath+partition+File.separator+profileFilePrefix+index+profileFileSurfix);
					if(storeFile.isFile()){
						logger.info("rename from {} to {}",profilePath+partition+File.separator+profileFilePrefix+index+profileFileSurfix, profilePath+partition+File.separator+profileFilePrefix+partition+profileFileSurfix);
						storeFile.renameTo(new File(profilePath+partition+File.separator+profileFilePrefix+partition+profileFileSurfix));
					}
					File storeFileWal =  new File(profilePath+partition+File.separator+profileFilePrefix+index+profileFileSurfix+profileWalFileSurfix);
					if(storeFileWal.isFile()){
						logger.info("rename from {} to {}",profilePath+partition+File.separator+profileFilePrefix+index+profileFileSurfix+profileWalFileSurfix,profilePath+partition+File.separator+profileFilePrefix+partition+profileFileSurfix+profileWalFileSurfix);
						storeFileWal.renameTo(new File(profilePath+partition+File.separator+profileFilePrefix+partition+profileFileSurfix+profileWalFileSurfix));
					}
					
					index++;
				}
				
			}
			*/
			/*
			ModuleManager
				.registerMBean(
						ProfileServer.getInstance(),
						new ObjectName(
							"com.yahoo.ads.pb.platform.profile:name=ProfileServer"));
				*/

			// initialize performance counter
			/*
			lookupCounter = new InflightCounter(counterGroupName, "Lookup");
			lookupCounter.register();
			noDataCounter = new IncrementCounter(counterGroupName, "LookupNoData");
			noDataCounter.register();
			*/

			//			storeCounter = new InflightCounter(counterGroupName, "Store");
			//			storeCounter.register();
			//			failedStoreCounter = new IncrementCounter(counterGroupName,
			//			        "FailedStore");
			//			failedStoreCounter.register();

			//			boolean enableStorePartition = conf.getBoolean(
			//			        Constant.PROFILE_STORING_PARTITION_ENABLE, false);
			//			if (enableStorePartition) {

			logger.info("creating helix partition sepctator {} {} {}", conf.getString(ZOOKEEPER_SERVER, "EMPTY"),
					PistachiosConstants.CLUSTER_NAME, conf.getString(PROFILE_HELIX_INSTANCE_ID, "EMPTY"));
			helixPartitionSpectator = new HelixPartitionSpectator(conf.getString(ZOOKEEPER_SERVER), // zkAddr
					PistachiosConstants.CLUSTER_NAME, InetAddress.getLocalHost().getHostName() //conf.getString(PROFILE_HELIX_INSTANCE_ID) // instanceName
			);
			// Partition Manager for line spending
			manager = new HelixPartitionManager<>(conf.getString(ZOOKEEPER_SERVER), // zkAddr
					PistachiosConstants.CLUSTER_NAME, InetAddress.getLocalHost().getHostName() //conf.getString(PROFILE_HELIX_INSTANCE_ID) // instanceName
			);
			//manager.start("BootstrapOnlineOffline", new BootstrapOnlineOfflineStateModelFactory(new StorePartitionHandlerFactory()));
			manager.start(PistachiosConstants.PARTITION_MODEL, new BootstrapOnlineOfflineStateModelFactory(
					new StorePartitionHandlerFactory()));
			//			}

			initialized = true;
		} catch (Exception e) {
			logger.error("Failed to initialize ProfileServerModule", e);
		}
		logger.info("Finished initializing profile server...........");

		return initialized;
	}

	class StorePartitionHandlerFactory implements PartitionHandlerFactory {

		public PartitionHandler createParitionHandler(int partitionId) {
			StorePartition sp = new StorePartition(partitionId);
			sp.setStoreFactory(new TKStoreFactory());

			PistachiosServer.storePartitionMap.put(partitionId, sp);
			logger.info("creating partition handler........... {}", sp);
			return sp;
			/*
			final int partitionId1 = partitionId;
			return new BootstrapPartitionHandler() {

				int partitionId;
				public void selfBootstraping() {
					logger.info("self bootstrapping {}.", partitionId1);
				}
				public void bootstrapingOthers() {
					logger.info("bootstraping others {}.", partitionId1);
				}

				public void startServing() {
					logger.info("starting serving store {}.", partitionId1);
				}
				public void stopServing() {
					logger.info("stopping serving store {}.", partitionId1);
				}
			};
			*/

		}
	}

}
