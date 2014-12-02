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

import java.nio.ByteBuffer;

import com.yahoo.ads.pb.helix.HelixPartitionSpectator;
import com.yahoo.ads.pb.util.ConfigurationManager;
import com.yahoo.ads.pb.util.PistachiosConstants;

import org.apache.commons.configuration.Configuration;

import java.net.InetAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.client.util.ExponentialBackOff;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.codahale.metrics.JmxReporter;
import com.google.api.client.util.BackOff;

public class PistachiosClient {
	private static Logger logger = LoggerFactory.getLogger(PistachiosClient.class);

	final static MetricRegistry metrics = new MetricRegistry();
	final static JmxReporter reporter = JmxReporter.forRegistry(metrics).inDomain("pistachio.client.metrics").build();

	private final static Meter lookupRequests = metrics.meter(MetricRegistry.name(PistachiosServer.class, "client.lookupRequests"));
	private final static Meter lookupFailureRequests = metrics.meter(MetricRegistry.name(PistachiosServer.class,
			"client.lookupFailureRequests"));

	private final static Meter storeRequests = metrics.meter(MetricRegistry.name(PistachiosServer.class, "client.storeRequests"));
	private final static Meter storeFailureRequests = metrics.meter(MetricRegistry.name(PistachiosServer.class,
			"client.storeFailureRequests"));

	private final static Meter processRequests = metrics.meter(MetricRegistry
			.name(PistachiosServer.class, "client.processRequests"));
	private final static Meter processFailureRequests = metrics.meter(MetricRegistry.name(PistachiosServer.class,
			"client.processFailureRequests"));

	private final static Timer lookupTimer = metrics.timer(MetricRegistry.name(PistachiosServer.class, "client.lookupTimer"));
	private final static Timer storeTimer = metrics.timer(MetricRegistry.name(PistachiosServer.class, "client.storeTimer"));
	private final static Timer processTimer = metrics.timer(MetricRegistry.name(PistachiosServer.class, "client.processTimer"));

	private String localHostAddress = null;

	static {
		reporter.start();
	}

	String ZOOKEEPER_SERVER = "Pistachio.ZooKeeper.Server";
	String PROFILE_HELIX_INSTANCE_ID = "Profile.Helix.InstanceId";
	Configuration conf = ConfigurationManager.getConfiguration();
	private int initialIntervalMillis = conf.getInt("Pistachio.BackOff.InitialIntervalMillis", 100);
	private int maxElapsedTimeMillis = conf.getInt("Pistachio.BackOff.MaxElapsedTimeMillis", 100 * 1000);
	private int maxIntervalMillis = conf.getInt("Pistachio.BackOff.MaxIntervalMillis", 5000);

	HelixPartitionSpectator helixPartitionSpectator;

	ConcurrentHashMap<String, Semaphore> ipToClientMapLock = new ConcurrentHashMap<String, Semaphore>();
	ConcurrentHashMap<String, PistachiosConnectionInfo> ipToClientMap = new ConcurrentHashMap<String, PistachiosConnectionInfo>();

	public PistachiosClient() throws Exception {
		try {
			helixPartitionSpectator = new HelixPartitionSpectator(conf.getString(ZOOKEEPER_SERVER), // zkAddr
					"PistachiosCluster", InetAddress.getLocalHost().getHostName() //conf.getString(PROFILE_HELIX_INSTANCE_ID) // instanceName
			);

		} catch (Exception e) {
			logger.error("Error init HelixPartitionSpectator, are zookeeper and helix installed and configured correctly?", e);
			throw e;
		}

		localHostAddress = InetAddress.getLocalHost().getHostAddress();
	}

	@SuppressWarnings("unused")
	private void closeClient(long id) {
		ipToClientMap.remove(id);
		ipToClientMapLock.remove(id);
	}

	/**
	 * 
	 * @param id
	 * @param reconnect
	 * @return
	 */
	private synchronized PistachiosConnectionInfo getClientInfo(long id, boolean reconnect, String from) {
		String ip = null;
		int count = 0;
		int shard = 0;

		while (ip == null && count++ < 1) {
			shard = (int) id % 256;
			shard = shard < 0 ? shard + 256 : shard;
			ip = helixPartitionSpectator.getOneInstanceForPartition(PistachiosConstants.RESOURCE_NAME, shard,
					PistachiosConstants.PARTITION_MASTER);
		}

		if (ip == null) {
			logger.info("partition not found");
			return null;
		}

		logger.debug("get {} connection for partition id={} shard={} ip={}", from, id, shard, ip);

		PistachiosConnectionInfo connectionPool = ipToClientMap.get(ip);
		if (connectionPool == null) {
			try {
				connectionPool = new PistachiosConnectionInfo(ip);
				ipToClientMap.put(ip, connectionPool);
			} catch (Exception e) {
				connectionPool = null;
				e.printStackTrace();
			}
		}

		return connectionPool;
	}

	private boolean isLocalCall(long partition) {
		boolean directLocalCall = false;

		try {
			int shard = (int) partition % 256;
			shard = shard < 0 ? shard + 256 : shard;
			String ip = helixPartitionSpectator.getOneInstanceForPartition(PistachiosConstants.RESOURCE_NAME, shard,
					PistachiosConstants.PARTITION_MASTER);
			if (ip.equals(localHostAddress) == true) {
				directLocalCall = true;
			}
		} catch (Exception e) {
			logger.info("error:", e);
		}
		return directLocalCall;
	}

	public byte[] lookup(long partition, long id) {
		lookupRequests.mark();
		final Timer.Context context = lookupTimer.time();
		boolean succeeded = false;
		PistachiosConnectionInfo clientInfo;
		Pistachios.Client client = null;
		byte[] ret = null;
		boolean reconnect = false;
		BackOff backoff;
		boolean directLocalCall = isLocalCall(partition);
		logger.info("lookup.directLocalCall={}", directLocalCall);

		backoff = (new ExponentialBackOff.Builder()).setInitialIntervalMillis(initialIntervalMillis)
				.setMaxElapsedTimeMillis(maxElapsedTimeMillis).setMaxIntervalMillis(maxIntervalMillis).build();

		try {
			long backOffMillis = backoff.nextBackOffMillis();
			while (!succeeded && (backOffMillis != BackOff.STOP)) {
				clientInfo = getClientInfo(partition, reconnect, "lookup");
				client = clientInfo.getConnection(reconnect);
				if (client == null) {
					logger.info("failed get client, retry in {} getting client", backOffMillis);
					try {
						Thread.sleep(backOffMillis);
					} catch (Exception e) {
					}
					backOffMillis = backoff.nextBackOffMillis();
					continue;
				}

				int actionRetry = 5;
				while (actionRetry-- > 0) {
					try {
						if (directLocalCall) {
							ret = PistachiosServer.handler.lookup(partition, id).array();
						} else {
							ret = client.lookup(partition, id).array();
						}
						logger.info("retry {} client.lookup(" + id + ")=" + new String(ret), actionRetry);
						succeeded = true;
						break;
					} catch (TException x) {
						logger.info("error: retry {}", actionRetry, x);
						reconnect = true;
					}
				}

				if (!succeeded) {
					//closeClient(id);
					try {
						logger.debug("failed lookup, retry in {}", backOffMillis);
						Thread.sleep(backOffMillis);
					} catch (Exception e) {
					}
				}

				backOffMillis = backoff.nextBackOffMillis();
				clientInfo.releaseConnection(client);
			}

			//perform(store, client);

		} catch (Exception e) {
			logger.info("exception lookup {}", id, e);
		} finally {
			if (!succeeded)
				lookupFailureRequests.mark();
			context.stop();
		}
		return ret;
	}

	public void store(long partition, long id, byte[] value) {
		storeRequests.mark();
		final Timer.Context context = storeTimer.time();
		boolean succeeded = false;
		PistachiosConnectionInfo clientInfo;
		Pistachios.Client client = null;
		@SuppressWarnings("unused")
		byte[] ret = null;
		boolean reconnect = false;
		BackOff backoff;
		boolean directLocalCall = isLocalCall(partition);
		logger.info("store.directLocalCall={}", directLocalCall);

		backoff = (new ExponentialBackOff.Builder()).setInitialIntervalMillis(initialIntervalMillis)
				.setMaxElapsedTimeMillis(maxElapsedTimeMillis).setMaxIntervalMillis(maxIntervalMillis).build();

		try {
			long backOffMillis = backoff.nextBackOffMillis();
			while (!succeeded && (backOffMillis != BackOff.STOP)) {
				clientInfo = getClientInfo(partition, reconnect, "store");
				client = clientInfo.getConnection(reconnect);
				if (client == null) {
					logger.info("failed get client, retry in {} getting client", backOffMillis);
					try {
						Thread.sleep(backOffMillis);
					} catch (Exception e) {
					}
					backOffMillis = backoff.nextBackOffMillis();
					continue;
				}
				int actionRetry = 5;
				while (actionRetry-- > 0) {
					try {
						if (directLocalCall) {
							PistachiosServer.handler.store(partition, id, ByteBuffer.wrap(value));
						} else {
							client.store(partition, id, ByteBuffer.wrap(value));
						}
						logger.debug("client.store(" + id + "," + value + ")");
						succeeded = true;
						break;
					} catch (TException x) {
						logger.info("error: ", x);
						reconnect = true;
					}
				}

				if (!succeeded) {
					logger.info("failed store, retry in {}", backOffMillis);
					//closeClient(id);

					if (!succeeded) {
						logger.info("failed store, retry in {}", backOffMillis);
						//closeClient(id);
						try {
							Thread.sleep(backOffMillis);
						} catch (Exception e) {
						}
					}

					try {
						Thread.sleep(backOffMillis);
					} catch (Exception e) {
					}
				}
				backOffMillis = backoff.nextBackOffMillis();
				clientInfo.releaseConnection(client);
			}

		} catch (Exception e) {
			logger.info("exception store {} {}", id, value, e);
		} finally {
			if (!succeeded)
				storeFailureRequests.mark();
			context.stop();
		}
	}

	/**
	 * 
	 * @param value
	 */
	public void process(byte[] value) {
		processRequests.mark();
		boolean succeeded = false;
		final Timer.Context context = processTimer.time();
		PistachiosConnectionInfo clientInfo;
		Pistachios.Client client = null;
		boolean reconnect = false;

		BackOff backoff = (new ExponentialBackOff.Builder()).setInitialIntervalMillis(initialIntervalMillis)
				.setMaxElapsedTimeMillis(maxElapsedTimeMillis).setMaxIntervalMillis(maxIntervalMillis).build();

		try {
			// long orderId = from the
			int orderId = -1;

			String line = new String(value);
			String[] buf = line.split("\"order_id\":");
			if (buf.length >= 2) {
				int index = buf[1].indexOf(",");
				if (index != -1) {
					try {
						orderId = Integer.parseInt(buf[1].substring(0, index).trim());
					} catch (Exception e) {
						orderId = -1;
					}
				}
			}

			if (orderId == -1) {
				return;
			}

			long backOffMillis = backoff.nextBackOffMillis();
			while (!succeeded && (backOffMillis != BackOff.STOP)) {
				clientInfo = getClientInfo(orderId, reconnect, "process");
				if (clientInfo == null) {
					logger.info("clientInfo is null");

					backOffMillis = backoff.nextBackOffMillis();
					try {
						Thread.sleep(backOffMillis);
					} catch (Exception e) {
					}
					continue;
				}

				if (clientInfo.isBatchEnabled() == true) {
					clientInfo.addBuffer(orderId, value);
					if (clientInfo.isBatchBufferFull() == true) {
						logger.info("buffer full, empty");
						client = clientInfo.getConnection(reconnect);
						if (client == null) {
							logger.info("failed get client, retry in {} getting client", backOffMillis);
							try {
								Thread.sleep(backOffMillis);
							} catch (Exception e) {
							}
							backOffMillis = backoff.nextBackOffMillis();
							continue;
						}
						
						int actionRetry = 5;
						while (actionRetry-- > 0) {
							try {
								succeeded = client.process_batch(ByteBuffer.wrap(clientInfo.getBuffer()));
								break;
							} catch (TException x) {
								logger.info("error: ", x);
								reconnect = true;
							}
						}
						
						backOffMillis = backoff.nextBackOffMillis();
						clientInfo.releaseConnection(client);

					} else {
						logger.info("added to batch buffer: {} - {}", clientInfo.ipAddress, clientInfo.size());
						succeeded = true;
					}
				} else {
					client = clientInfo.getConnection(reconnect);
					if (client == null) {
						logger.info("failed get client, retry in {} getting client", backOffMillis);
						try {
							Thread.sleep(backOffMillis);
						} catch (Exception e) {
						}
						backOffMillis = backoff.nextBackOffMillis();
						continue;
					}

					int actionRetry = 5;
					while (actionRetry-- > 0) {
						try {
							succeeded = client.process(orderId, ByteBuffer.wrap(value));
							break;
						} catch (TException x) {
							logger.info("error: ", x);
							reconnect = true;
						}
					}
					backOffMillis = backoff.nextBackOffMillis();
					clientInfo.releaseConnection(client);
				}
			}
		} catch (Exception e) {
			logger.info("exception process {}", e);
			e.printStackTrace();
		} finally {
			if (!succeeded) {
				processFailureRequests.mark();
			}
			context.stop();
		}
	}

	/**
	 * 
	 * @param value
	 */
	public void process_batch(byte[] value) {
		processRequests.mark();
		boolean succeeded = false;
		final Timer.Context context = processTimer.time();
		PistachiosConnectionInfo clientInfo;
		Pistachios.Client client = null;
		boolean reconnect = false;

		BackOff backoff = (new ExponentialBackOff.Builder()).setInitialIntervalMillis(initialIntervalMillis)
				.setMaxElapsedTimeMillis(maxElapsedTimeMillis).setMaxIntervalMillis(maxIntervalMillis).build();

		try {
			// long orderId = from the
			int orderId = -1;

			String line = new String(value);
			String[] buf = line.split("\"order_id\":");
			if (buf.length >= 2) {
				int index = buf[1].indexOf(",");
				if (index != -1) {
					try {
						orderId = Integer.parseInt(buf[1].substring(0, index).trim());
					} catch (Exception e) {
						orderId = -1;
					}
				}
			}

			if (orderId == -1) {
				return;
			}

			long backOffMillis = backoff.nextBackOffMillis();
			while (!succeeded && (backOffMillis != BackOff.STOP)) {
				clientInfo = getClientInfo(orderId, reconnect, "process");
				client = clientInfo.getConnection(reconnect);
				if (client == null) {
					logger.info("failed get client, retry in {} getting client", backOffMillis);
					try {
						Thread.sleep(backOffMillis);
					} catch (Exception e) {
					}
					backOffMillis = backoff.nextBackOffMillis();
					continue;
				}

				int actionRetry = 5;
				while (actionRetry-- > 0) {
					try {
						succeeded = client.process(orderId, ByteBuffer.wrap(value));
						break;
					} catch (TException x) {
						logger.info("error: ", x);
						reconnect = true;
					}
				}
				backOffMillis = backoff.nextBackOffMillis();
				clientInfo.releaseConnection(client);
			}

		} catch (Exception e) {
			logger.info("exception process {}", value, e);
		} finally {
			if (!succeeded) {
				processFailureRequests.mark();
			}
			context.stop();
		}
	}

	public static void main(String[] args) {
		PistachiosClient client;
		try {
			client = new PistachiosClient();
		} catch (Exception e) {
			logger.info("error creating clietn", e);
			return;
		}

		long id = 0;
		@SuppressWarnings("unused")
		boolean store = false;
		String value = "";
		if (args.length == 2 && args[0].equals("lookup")) {
			try {
				id = Long.parseLong(args[1]);
				System.out.println("client.lookup(" + id + ")" + new String(client.lookup(0, id)));
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else if (args.length == 3 && args[0].equals("store")) {
			try {
				id = Long.parseLong(args[1]);
			} catch (Exception e) {
				e.printStackTrace();
			}
			store = true;
			value = args[2];
			client.store(0, id, value.getBytes());

		} else {
			System.out.println("USAGE: xxxx lookup id or xxxx store id value");
			System.exit(0);
		}

		/*
		if (args.length != 1) {
		  System.out.println("Please enter 'simple' or 'secure'");
		  System.exit(0);
		}
		*/

	}

}
