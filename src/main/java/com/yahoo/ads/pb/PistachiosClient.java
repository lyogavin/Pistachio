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
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PistachiosClient {
	private static Logger logger = LoggerFactory.getLogger(PistachiosClient.class);
	String ZOOKEEPER_SERVER = "Pistachio.ZooKeeper.Server";
	String PROFILE_HELIX_INSTANCE_ID = "Profile.Helix.InstanceId";
	Configuration conf = ConfigurationManager.getConfiguration();

	HelixPartitionSpectator helixPartitionSpectator;

	HashMap<String, Pistachios.Client> ipToClientMap = new HashMap<String, Pistachios.Client>();

	public PistachiosClient() {
		try {
			helixPartitionSpectator = new HelixPartitionSpectator(
								conf.getString(ZOOKEEPER_SERVER), // zkAddr
								"PistachiosCluster",
								InetAddress.getLocalHost().getHostName() //conf.getString(PROFILE_HELIX_INSTANCE_ID) // instanceName
								);

		} catch(Exception e) {
			return;
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
				transport = new TSocket(ip, 9090);
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

	public byte[] lookup(long id) {

		boolean succeeded = false;
		Pistachios.Client client = null;
		int retry = 100;
		byte[] ret =  null;
		boolean reconnect = false;

		while (!succeeded && retry-- > 0) {
			logger.info("retry {} getting client", retry);
			client = getClient(id, reconnect);
			if (client == null) {
				try{
				Thread.sleep(1000);
				}catch(Exception e) {
				}
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
				Thread.sleep(1000);
				} catch(Exception e) {
				}
			}
		}

		//perform(store, client);
		return ret;
	}

	public void store(long id, byte[] value) {
		boolean succeeded = false;
		Pistachios.Client client = null;
		int retry = 100;
		byte[] ret =  null;
		boolean reconnect = false;

		while (!succeeded && retry-- > 0) {
			client = getClient(id, reconnect);
			if (client == null) {
				try{
				Thread.sleep(1000);
				}catch(Exception e) {
				}
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
				//closeClient(id);
				try{
				Thread.sleep(1000);
				}catch(Exception e) {
				}
			}
		}
	}

  public static void main(String [] args) {
	  PistachiosClient client = new PistachiosClient();

		  long id = 0;
		  boolean store = false;
		  String value="" ;
	  if (args.length ==2 && args[0].equals("lookup") ) {
		  try {
		  id = Long.parseLong(args[1]);
		  client.lookup(id);
		  } catch (Exception e) {
		  }
			//System.out.println("client.lookup(" + id + ")" + new String(client.lookup(id).array()));
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
