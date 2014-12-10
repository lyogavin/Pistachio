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


public class PistachiosBenchmarking implements Runnable {
	private static Logger logger = LoggerFactory.getLogger(PistachiosBenchmarking.class);
    private PistachiosClient client;
    private Random rand;
    private static int recordAverageSize = 1024;


    public PistachiosBenchmarking() {
	  try {
	  client = new PistachiosClient();
	  }catch (Exception e) {
		  logger.info("error creating client", e);
		  return;
	  }
	  rand = new Random();
    }
    public void run() {
        logger.info("starting thread....");
        while(true) {
            try {
                long id = rand.nextLong();
                byte[] bytes = new byte[rand.nextInt(recordAverageSize << 1)];
                rand.nextBytes(bytes);
                client.store(id, bytes);
            } catch (Exception e) {
                System.out.println("error testing"+ e);
                System.exit(0);
            }
        }
    }


  public static void main(String [] args) {
      int threadCount = 50;
              logger.info("parsing error {}", args.length);
      if (args.length >=1) {
          try {
              threadCount = Integer.parseInt(args[0]);
              logger.info("parsd {} {}", args[0], threadCount);
          } catch(Exception e) {
              logger.info("parsing error", e);
          }
      }
      if (args.length >=2) {
          try {
              recordAverageSize = Integer.parseInt(args[1]);
              logger.info("parsd {} {}", args[1], recordAverageSize);
          } catch(Exception e) {
              logger.info("parsing error", e);
          }
      }

      for (int i = 0; i< threadCount; i++) {
          Thread thread = new Thread(new PistachiosBenchmarking(), "benchmarking"+i);
          thread.start();
      }

  }

 }
