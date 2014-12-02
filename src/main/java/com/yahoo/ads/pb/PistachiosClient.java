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
import org.apache.thrift.transport.TSSLTransportFactory;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TSSLTransportFactory.TSSLTransportParameters;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import java.nio.ByteBuffer;
import com.yahoo.ads.pb.helix.HelixPartitionSpectator;
import com.yahoo.ads.pb.util.ConfigurationManager;
import org.apache.commons.configuration.Configuration;
import java.net.InetAddress;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.api.client.util.ExponentialBackOff;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.codahale.metrics.JmxReporter;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.client.util.BackOff;

/**
 * Main Pistachio Client Class
 * <ul>
 * <li>To use the Client, new an instance and call the functions
 * <li>TBA: Thread Safty
 * <li>TBA: Connection Reuse
 *     (see <a href="#setXORMode">setXORMode</a>)
 * </ul>
 * <p>
 * 
 * @author      Gavin Li
 * @version     %I%, %G%
 * @since       1.0
 */
public class PistachiosClient {
	private static Logger logger = LoggerFactory.getLogger(PistachiosClient.class);
	final static MetricRegistry metrics = new MetricRegistry();
	final static JmxReporter reporter = JmxReporter.forRegistry(metrics).build();

	private final static Meter lookupFailureRequests = metrics.meter(MetricRegistry.name(PistachiosServer.class, "lookupFailureRequests"));
	private final static Meter storeFailureRequests = metrics.meter(MetricRegistry.name(PistachiosServer.class, "storeFailureRequests"));

	private final static Timer lookupTimer = metrics.timer(MetricRegistry.name(PistachiosServer.class, "lookupTimer"));
	private final static Timer storeTimer = metrics.timer(MetricRegistry.name(PistachiosServer.class, "storeTimer"));



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

	ConcurrentHashMap<String, Pistachios.Client> ipToClientMap = new ConcurrentHashMap<String, Pistachios.Client>();

	public PistachiosClient() throws Exception {
		try {
			helixPartitionSpectator = new HelixPartitionSpectator(
								conf.getString(ZOOKEEPER_SERVER), // zkAddr
								"PistachiosCluster",
								InetAddress.getLocalHost().getHostName() //conf.getString(PROFILE_HELIX_INSTANCE_ID) // instanceName
								);

		} catch(Exception e) {
			logger.error("Error init HelixPartitionSpectator, are zookeeper and helix installed and configured correctly?", e);
			throw e;
		}
	}

	private void closeClient(long id) {
		ipToClientMap.remove(id);
	}

	private Pistachios.Client getClient(long id, boolean reconnect) {
		  String ip = null;
		  int count = 0;
		  while (ip == null && count++ < 1) {
		  int shard = (int)id % 256;
		  shard = shard < 0 ? shard + 256: shard;
		ip = helixPartitionSpectator.getOneInstanceForPartition("PistachiosResource", shard, "MASTER");
			logger.info("partition found {}", ip);

		  }
		if (ip == null) {
			logger.info("partition not found");
			return null;
		}
		logger.debug("partition found:" + ip);

		Pistachios.Client client = null;
		if (reconnect  || ((client =ipToClientMap.get(ip)) == null)) {

			try {
				TTransport transport;
				transport = new TSocket(ip, 9090, 1000);
				transport.open();

				TProtocol protocol = new  TBinaryProtocol(transport);
				client = new Pistachios.Client(protocol);

				ipToClientMap.put(ip, client);


				//transport.close();
			} catch (TException x) {
				logger.info("error: ", x);
			} 
		}

      //perform(store, client);
		return client;
	}

    /** 
     * To lookup the value of an id. Given the id return the value as a byte array.
     *
     * @param id        id to look up as long.
     * @return          <code>byte array</code> return in byte array
     */
	public byte[] lookup(long id) {

		final Timer.Context context = lookupTimer.time();
		boolean succeeded = false;
		Pistachios.Client client = null;
		byte[] ret =  null;
		boolean reconnect = false;
		BackOff backoff;

		backoff = (new ExponentialBackOff.Builder()).setInitialIntervalMillis(initialIntervalMillis)
											  .setMaxElapsedTimeMillis(maxElapsedTimeMillis)
											  .setMaxIntervalMillis(maxIntervalMillis)
											  .build();

		try {
			long backOffMillis = backoff.nextBackOffMillis();
			while (!succeeded && (backOffMillis != BackOff.STOP)) {
				client = getClient(id, reconnect);
				if (client == null) {
					logger.info("failed get client, retry in {} getting client", backOffMillis);
					try{
						Thread.sleep(backOffMillis);
					}catch(Exception e) {
					}
					backOffMillis = backoff.nextBackOffMillis();
					continue;
				}

				int actionRetry = 5;
				while (actionRetry-- > 0) {
					try {
						ret = client.lookup(id).array();
						logger.info("retry {} client.lookup(" + id + ")" + new String(ret), actionRetry);
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
					} catch(Exception e) {
					}
				}
				backOffMillis = backoff.nextBackOffMillis();
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

    /** 
     * To store the key value.
     *
     * @param id        id to store as long.
     * @param value     value to store as byte array
     */
	public void store(long id, byte[] value) {
		final Timer.Context context = storeTimer.time();
		boolean succeeded = false;
		Pistachios.Client client = null;
		byte[] ret =  null;
		boolean reconnect = false;

		BackOff backoff;

		backoff = (new ExponentialBackOff.Builder()).setInitialIntervalMillis(initialIntervalMillis)
											  .setMaxElapsedTimeMillis(maxElapsedTimeMillis)
											  .setMaxIntervalMillis(maxIntervalMillis)
											  .build();


		try {
			long backOffMillis = backoff.nextBackOffMillis();
			while (!succeeded && (backOffMillis != BackOff.STOP)) {
				client = getClient(id, reconnect);
				if (client == null) {
					logger.info("failed get client, retry in {} getting client", backOffMillis);
					try{
					Thread.sleep(backOffMillis);
					}catch(Exception e) {
					}
					backOffMillis = backoff.nextBackOffMillis();
					continue;
				}
				int actionRetry = 5;
				while (actionRetry-- > 0) {
					try {
						client.store(id, ByteBuffer.wrap(value));
						logger.info("client.store(" + id + ","+ value+")" );
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
					try{
					Thread.sleep(backOffMillis);
					}catch(Exception e) {
					}
				}
				backOffMillis = backoff.nextBackOffMillis();
			}

		} catch (Exception e) {
			logger.info("exception store {} {}", id, value, e);
		} finally {
			if (!succeeded)
				storeFailureRequests.mark();
			context.stop();
		}
	}

  public static void main(String [] args) {
	  PistachiosClient client;
	  try {
	  client = new PistachiosClient();
	  }catch (Exception e) {
		  logger.info("error creating clietn", e);
		  return;
	  }

		  long id = 0;
		  boolean store = false;
		  String value="" ;
	  if (args.length ==2 && args[0].equals("lookup") ) {
		  try {
		  id = Long.parseLong(args[1]);
			System.out.println("client.lookup(" + id + ")" + new String(client.lookup(id)));
		  } catch (Exception e) {
		  }
	  } else if (args.length == 3 && args[0].equals("store") ) {
		  try {
		  id = Long.parseLong(args[1]);
		  } catch (Exception e) {
		  }
		  store = true;
		  value = args[2];
		  client.store(id, value.getBytes());

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
