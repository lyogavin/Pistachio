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
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.api.client.util.ExponentialBackOff;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.codahale.metrics.JmxReporter;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.client.util.BackOff;
import com.yahoo.ads.pb.PistachiosClientImpl;
import com.yahoo.ads.pb.PistachiosServer;
import com.yahoo.ads.pb.network.netty.NettyPistachioClient;

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
	final static JmxReporter reporter = JmxReporter.forRegistry(metrics).inDomain("pistachio.client.metrics").build();

	private final static Meter lookupFailureRequests = metrics.meter(MetricRegistry.name(PistachiosServer.class, "lookupFailureRequests"));
	private final static Meter storeFailureRequests = metrics.meter(MetricRegistry.name(PistachiosServer.class, "storeFailureRequests"));
	private final static Meter processFailureRequests = metrics.meter(MetricRegistry.name(PistachiosServer.class, "processFailureRequests"));

	private final static Timer lookupTimer = metrics.timer(MetricRegistry.name(PistachiosServer.class, "lookupTimer"));
	private final static Timer storeTimer = metrics.timer(MetricRegistry.name(PistachiosServer.class, "storeTimer"));
	private final static Timer processTimer = metrics.timer(MetricRegistry.name(PistachiosServer.class, "processTimer"));

    private PistachiosClientImpl clientImpl = new NettyPistachioClient();



	static {
		reporter.start();
	}
	Configuration conf = ConfigurationManager.getConfiguration();
	private int initialIntervalMillis = conf.getInt("Pistachio.BackOff.InitialIntervalMillis", 100);
	private int maxElapsedTimeMillis = conf.getInt("Pistachio.BackOff.MaxElapsedTimeMillis", 100 * 1000);
	private int maxIntervalMillis = conf.getInt("Pistachio.BackOff.MaxIntervalMillis", 5000);
    BackOff backoff = (new ExponentialBackOff.Builder()).setInitialIntervalMillis(initialIntervalMillis)
        .setMaxElapsedTimeMillis(maxElapsedTimeMillis)
        .setMaxIntervalMillis(maxIntervalMillis)
        .build();


	public PistachiosClient() throws Exception {
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
		byte[] ret =  null;
        long backOffMillis =  0;

		try {
			while (!succeeded && (backOffMillis != BackOff.STOP)) {
                ret = clientImpl.lookup(id);
                //if (ret != null) {
                //TODO: add a way to return success status 
                if (true) {
                    succeeded = true;
                    backoff.reset();
                    break;
                } else {
                    try{
                        backOffMillis = backoff.nextBackOffMillis();
                        Thread.sleep(backOffMillis);
                    }catch(Exception e) {
                    }
                    continue;
				}
            }

            if (!succeeded) {
                logger.debug("failed lookup, retry in {}", backOffMillis);
            }
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
     * @return          <code>boolean</code> succeeded or not
     */
	public boolean store(long id, byte[] value) {
		final Timer.Context context = storeTimer.time();
		boolean succeeded = false;
        long backOffMillis =  0;


		try {
			while (!succeeded && (backOffMillis != BackOff.STOP)) {
				if (!clientImpl.store(id, value)) {
					logger.info("failed get client, retry in {} getting client", backOffMillis);
					try{
                        backOffMillis = backoff.nextBackOffMillis();
                        Thread.sleep(backOffMillis);
					}catch(Exception e) {
					}
					continue;
				} else {
                    succeeded = true;
                    backoff.reset();
                    break;
                }
            }

            if (!succeeded) {
                logger.info("failed store, retry in {}", backOffMillis);
			}

		} catch (Exception e) {
			logger.info("exception store {} {}", id, value, e);
		} finally {
			if (!succeeded)
				storeFailureRequests.mark();
			context.stop();
		}
        return succeeded;
	}

    /** 
     * To close all the resource gracefully
     */
	public void close() {
        clientImpl.close();
    }

    /** 
     * To process a batch of events
     *
     * @param id        id to store as long.
     * @param events    list of events as byte []
     * @return          <code>boolean</code> succeeded or not
     */
	public boolean processBatch(long id, List<byte[]> events) {
		final Timer.Context context = processTimer.time();
		boolean succeeded = false;
		byte[] ret =  null;

        long backOffMillis =  0;

		try {
			while (!succeeded && (backOffMillis != BackOff.STOP)) {
				if (!clientImpl.processBatch(id, events)) {
					logger.info("failed get client, retry in {} getting client", backOffMillis);
					try{
                        backOffMillis = backoff.nextBackOffMillis();
                        Thread.sleep(backOffMillis);
					}catch(Exception e) {
					}
					continue;
				} else {
                    succeeded = true;
                    backoff.reset();
                    break;
                }
            }

            if (!succeeded) {
                logger.info("failed process, retry in {}", backOffMillis);
			}

		} catch (Exception e) {
			logger.info("exception process {} {}", id, events, e);
		} finally {
			if (!succeeded)
				storeFailureRequests.mark();
			context.stop();
		}
        return succeeded;
	}

  public static void main(String [] args) {
	  PistachiosClient client = null;
      try {
          client = new PistachiosClient();
      }catch (Exception e) {
          logger.info("error creating clietn", e);
          if (client != null)
              client.close();
          return;
      }

      try {

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
      } finally {
          client.close();
      }




	/*
    if (args.length != 1) {
      System.out.println("Please enter 'simple' or 'secure'");
      System.exit(0);
    }
	*/

  }

}
