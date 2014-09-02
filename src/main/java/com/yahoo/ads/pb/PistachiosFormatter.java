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

import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TSSLTransportFactory;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TSSLTransportFactory.TSSLTransportParameters;
import java.nio.ByteBuffer;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import com.yahoo.ads.pb.store.StorePartition;
import com.yahoo.ads.pb.store.TKStoreFactory;


import org.apache.helix.model.StateModelDefinition;
import com.yahoo.ads.pb.store.TLongKyotoCabinetStore;
import com.yahoo.ads.pb.kafka.KeyValue;
import com.yahoo.ads.pb.helix.PartitionHandler;
import com.yahoo.ads.pb.helix.PartitionHandlerFactory;
import com.yahoo.ads.pb.helix.BootstrapOnlineOfflineStateModel;
import com.yahoo.ads.pb.helix.BootstrapOnlineOfflineStateModelFactory;
import com.yahoo.ads.pb.helix.BootstrapPartitionHandler;
import com.yahoo.ads.pb.helix.HelixPartitionManager;
import com.yahoo.ads.pb.helix.HelixPartitionSpectator;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.controller.GenericHelixController;
import org.apache.helix.HelixAdmin;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import kafka.utils.ZKStringSerializer$;


//import com.yahoo.ads.pb.platform.perf.IncrementCounter;
//import com.yahoo.ads.pb.platform.perf.InflightCounter;
import com.yahoo.ads.pb.util.ConfigurationManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.configuration.Configuration;
import com.google.common.base.Joiner;

import kafka.admin.CreateTopicCommand;
import org.I0Itec.zkclient.ZkClient;
import org.apache.helix.model.InstanceConfig;
import 	org.apache.helix.HelixDefinedState;



// Generated code

import java.util.HashMap;
import java.util.Properties;


public class PistachiosFormatter{
	private static Logger logger = LoggerFactory.getLogger(PistachiosServer.class);


  public static void main(String [] args) {
	  String usage = "Usage: xxxx [zk string] [comma seperated cluster] [num partition] [num replica] (kafka zk path, optional)";
	  if (args.length <4) {
		  System.out.println("invalid number of parameters");
		  System.out.println(usage);
		  return;
	  }

	  int numPartitions = -1;
	  int numReplicas = -1;
	  String[] hostList = null;
	  String kafkaZKPath = null;

	  try {
		  numPartitions = Integer.parseInt(args[2]);
		  numReplicas = Integer.parseInt(args[3]);
		  hostList = args[1].split(",");
		  if (args.length >=5)
			  kafkaZKPath = args[4];
	  } catch(Exception e) {
	  }

	  if (numPartitions == -1 || numReplicas == -1|| hostList == null || hostList.length == 0) {
		  System.out.println("invalid parameters");
		  System.out.println(usage);
		  return;
	  }

	  


    try {
		ZKHelixAdmin admin = new ZKHelixAdmin(args[0]);
		admin.addCluster("PistachiosCluster");

		// define model
		StateModelDefinition stateModel = new StateModelDefinition.Builder("MasterSlave")
		  // OFFLINE is the state that the system starts in (initial state is REQUIRED)
			.initialState("OFFLINE")

			// Lowest number here indicates highest priority, no value indicates lowest priority
			.addState("MASTER", 1)
			.addState("SLAVE", 2)
			.addState("OFFLINE")

			// Note the special inclusion of the DROPPED state (REQUIRED)
			.addState(HelixDefinedState.DROPPED.toString())

			// No more than one master allowed
			.upperBound("MASTER", 1)

			// R indicates an upper bound of number of replicas for each partition
			.dynamicUpperBound("SLAVE", "R")

			// Add some high-priority transitions
			.addTransition("SLAVE", "MASTER", 1)
			.addTransition("OFFLINE", "SLAVE", 2)

			// Using the same priority value indicates that these transitions can fire in any order
			.addTransition("MASTER", "SLAVE", 3)
			.addTransition("SLAVE", "OFFLINE", 3)

			// Not specifying a value defaults to lowest priority
			// Notice the inclusion of the OFFLINE to DROPPED transition
			// Since every state has a path to OFFLINE, they each now have a path to DROPPED (REQUIRED)
			.addTransition("OFFLINE", HelixDefinedState.DROPPED.toString())

			// Create the StateModelDefinition instance
			.build();

		// Use the isValid() function to make sure the StateModelDefinition will work without issues
		// Assert.assertTrue(stateModel.isValid());
		try {
		admin.addStateModelDef("PistachiosCluster", "MasterSlave", stateModel);
		} catch(Exception e) {
			logger.info("error:", e);
		}

		logger.info("adding resource");
		try {
		admin.addResource("PistachiosCluster", "PistachiosResource", numPartitions, "MasterSlave", "SEMI_AUTO");
		} catch(Exception e) {
			logger.info("error:", e);
		}
		logger.info("adding host");
		for (String host : hostList) {
			InstanceConfig instanceConfig = new InstanceConfig(host);
			instanceConfig.setHostName(host);
			instanceConfig.setPort("1234");
			instanceConfig.setInstanceEnabled(true);

			//Add additional system specific configuration if needed. These can be accessed during the node start up.
			//instanceConfig.getRecord().setSimpleField("key", "value");
			try {
			admin.addInstance("PistachiosCluster", instanceConfig);
			} catch(Exception e) {
				logger.info("error:", e);
			}
		}
		logger.info("rebalancing");
		admin.rebalance("PistachiosCluster", "PistachiosResource", numReplicas);

		logger.info("adding topic to zk path: {}", kafkaZKPath);
		ZkClient zkClient = new ZkClient(args[0]+ (kafkaZKPath != null ? "/" + kafkaZKPath : ""), 30000, 30000, ZKStringSerializer$.MODULE$);

		for (int i =0; i<numPartitions; i++) {
			CreateTopicCommand.createTopic(zkClient , "PistachiosPartition." + i, 1, 1, "");
		}
		zkClient.close();
		//Thread.sleep(120000);
	  } catch(Exception e) {
		  logger.info("error:", e);
	  }
  }

}
