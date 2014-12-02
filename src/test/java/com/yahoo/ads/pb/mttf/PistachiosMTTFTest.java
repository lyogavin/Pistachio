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

// Generated code
//import tutorial.*;
//import shared.*;

package com.yahoo.ads.pb.mttf;
import com.yahoo.ads.pb.PistachiosClient;
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
import java.util.Random;
import java.util.Arrays;


public class PistachiosMTTFTest{
	private static Logger logger = LoggerFactory.getLogger(PistachiosMTTFTest.class);


  public static void main(String [] args) {
	  PistachiosClient client;
	  try {
	  client = new PistachiosClient();
	  }catch (Exception e) {
		  logger.info("error creating client", e);
		  return;
	  }
	  Random rand = new Random();

	  while(true) {
		  try {
		  long id = rand.nextLong();
		  String value=InetAddress.getLocalHost().getHostName() + rand.nextInt() ;
		  client.store(0, id, value.getBytes());
		  for (int i =0; i<30; i++) {
			  byte[] clientValue = client.lookup(0, id);
			  String remoteValue = new String(clientValue);
			  if (Arrays.equals(value.getBytes(), clientValue) || !remoteValue.contains(InetAddress.getLocalHost().getHostName())) {
				  logger.debug("succeeded checking id {} value {}", id, value);
			  } else {
				  logger.error("failed checking id {} value {} != {}", id, value, new String(clientValue));
				  System.exit(0);
			  }
			  Thread.sleep(100);
		  }
		  } catch (Exception e) {
			  System.out.println("error testing"+ e);
			  System.exit(0);
		  }
	  }
  }

}
