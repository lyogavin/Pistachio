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

import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.HelixException;

import kafka.utils.ZKStringSerializer$;

//import com.yahoo.ads.pb.platform.perf.IncrementCounter;
//import com.yahoo.ads.pb.platform.perf.InflightCounter;
import com.yahoo.ads.pb.util.ConfigurationManager;
import com.yahoo.ads.pb.util.PistachiosConstants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;

import kafka.admin.CreateTopicCommand;
import kafka.utils.ZkUtils;

import org.I0Itec.zkclient.ZkClient;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.HelixDefinedState;
import org.apache.helix.model.IdealState;

// Generated code

import java.util.List;

public class PistachiosFormatter {

	public static class PistachioClusterInfo {
		List<String> hostList;
		int numPartitions;
		int numReplicas;
	}

	private static Logger logger = LoggerFactory.getLogger(PistachiosServer.class);

	public static PistachioClusterInfo getClusterInfo() {
		try {
			String zookeeperConnStr = ConfigurationManager.getConfiguration().getString("Pistachio.ZooKeeper.Server");
			ZKHelixAdmin admin = new ZKHelixAdmin(zookeeperConnStr);
			IdealState idealState = admin.getResourceIdealState(PistachiosConstants.CLUSTER_NAME,
					PistachiosConstants.RESOURCE_NAME);
			PistachioClusterInfo info = new PistachioClusterInfo();
			info.numPartitions = idealState.getNumPartitions();
			info.numReplicas = Integer.parseInt(idealState.getReplicas());
			info.hostList = admin.getInstancesInCluster(PistachiosConstants.CLUSTER_NAME);

			logger.info("num partitions: {}, num Replicas: {}, hostList: {}.", info.numPartitions, info.numReplicas, Joiner.on(",")
					.join(info.hostList.toArray()));

			return info;
		} catch (Exception e) {
			logger.info("error getting cluster info", e);
			return null;
		}
	}

	private static void format(ZKHelixAdmin admin, ZkClient zkClient, String[] hostList, int numPartitions, int numReplicas,
			String kafkaTopicPrefix, String kafkaZKPath) {
		try {
			for (int i = 0; i < numPartitions; i++) {
				try {
					//CreateTopicCommand.createTopic(zkClient , "PistachiosPartition." + i, 1, 1, "");
					CreateTopicCommand.createTopic(zkClient, kafkaTopicPrefix + i, 1, 1, "");
				} catch (kafka.common.TopicExistsException tee) {
					logger.info("topic exists, continue", tee);
				} catch (Exception e) {
					logger.info("creating kafka topic error, roll back", e);
					cleanup(admin, zkClient, hostList, numPartitions, numReplicas, kafkaTopicPrefix, kafkaZKPath);
				}
			}
			zkClient.close();

			//ZKHelixAdmin admin = new ZKHelixAdmin(args[1]);
			admin.addCluster(PistachiosConstants.CLUSTER_NAME);

			// define model
			StateModelDefinition stateModel = new StateModelDefinition.Builder(PistachiosConstants.PARTITION_MODEL)
					// OFFLINE is the state that the system starts in (initial state is REQUIRED)
					.initialState(PistachiosConstants.PARTITION_OFFLINE)

					// Lowest number here indicates highest priority, no value indicates lowest priority
					.addState(PistachiosConstants.PARTITION_MASTER, 1)
					.addState(PistachiosConstants.PARTITION_SLAVE, 2)
					.addState(PistachiosConstants.PARTITION_OFFLINE)

					// Note the special inclusion of the DROPPED state (REQUIRED)
					.addState(HelixDefinedState.DROPPED.toString())

					// No more than one master allowed
					.upperBound(PistachiosConstants.PARTITION_MASTER, 1)

					// R indicates an upper bound of number of replicas for each partition
					.dynamicUpperBound(PistachiosConstants.PARTITION_SLAVE, "R")

					// Add some high-priority transitions
					.addTransition(PistachiosConstants.PARTITION_SLAVE, PistachiosConstants.PARTITION_MASTER, 1)
					.addTransition(PistachiosConstants.PARTITION_OFFLINE, PistachiosConstants.PARTITION_SLAVE, 2)

					// Using the same priority value indicates that these transitions can fire in any order
					.addTransition(PistachiosConstants.PARTITION_MASTER, PistachiosConstants.PARTITION_SLAVE, 3)
					.addTransition(PistachiosConstants.PARTITION_SLAVE, PistachiosConstants.PARTITION_OFFLINE, 3)

					// Not specifying a value defaults to lowest priority
					// Notice the inclusion of the OFFLINE to DROPPED transition
					// Since every state has a path to OFFLINE, they each now have a path to DROPPED (REQUIRED)
					.addTransition(PistachiosConstants.PARTITION_OFFLINE, HelixDefinedState.DROPPED.toString())

					// Create the StateModelDefinition instance
					.build();

			// Use the isValid() function to make sure the StateModelDefinition will work without issues
			// Assert.assertTrue(stateModel.isValid());
			try {
				admin.addStateModelDef(PistachiosConstants.CLUSTER_NAME, PistachiosConstants.PARTITION_MODEL, stateModel);
			} catch (HelixException he) {
				if (he.getMessage().contains("already exists")) {
					logger.info("state model def already exists, continue.", he);
				} else {
					logger.info("adding state model def error, roll back and exit", he);
					cleanup(admin, zkClient, hostList, numPartitions, numReplicas, kafkaTopicPrefix, kafkaZKPath);
				}
			} catch (Exception e) {
				logger.info("adding state model def error, roll back and exit", e);
				cleanup(admin, zkClient, hostList, numPartitions, numReplicas, kafkaTopicPrefix, kafkaZKPath);
			}

			logger.info("adding resource");
			try {
				admin.addResource(PistachiosConstants.CLUSTER_NAME, PistachiosConstants.RESOURCE_NAME, numPartitions,
						PistachiosConstants.PARTITION_MODEL, PistachiosConstants.PARTITION_SEMI_AUTO);
			} catch (HelixException he) {
				if (he.getMessage().contains("already exists")) {
					logger.info("resourece already exists, continue.", he);
				} else {
					logger.info("adding resource error, roll back and exit", he);
					cleanup(admin, zkClient, hostList, numPartitions, numReplicas, kafkaTopicPrefix, kafkaZKPath);
				}
			} catch (Exception e) {
				logger.info("adding resource error, roll back and exit", e);
				cleanup(admin, zkClient, hostList, numPartitions, numReplicas, kafkaTopicPrefix, kafkaZKPath);
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
					admin.addInstance(PistachiosConstants.CLUSTER_NAME, instanceConfig);
				} catch (HelixException he) {
					if (he.getMessage().contains("already exists")) {
						logger.info("instance already exists, continue.", he);
					} else {
						logger.info("adding instance error, roll back and exit", he);
						cleanup(admin, zkClient, hostList, numPartitions, numReplicas, kafkaTopicPrefix, kafkaZKPath);
					}
				} catch (Exception e) {
					logger.info("adding instance error, roll back and exit", e);
					cleanup(admin, zkClient, hostList, numPartitions, numReplicas, kafkaTopicPrefix, kafkaZKPath);
				}
			}
			logger.info("rebalancing");
			try {
				admin.rebalance(PistachiosConstants.CLUSTER_NAME, PistachiosConstants.RESOURCE_NAME, numReplicas);
			} catch (Exception e) {
				logger.info("rebalancing error, roll back and exit", e);
				cleanup(admin, zkClient, hostList, numPartitions, numReplicas, kafkaTopicPrefix, kafkaZKPath);
			}

			logger.info("adding topic to zk path: {}", kafkaZKPath);
			//ZkClient zkClient = new ZkClient(args[1]+ (kafkaZKPath != null ? "/" + kafkaZKPath : ""), 30000, 30000, ZKStringSerializer$.MODULE$);

			//Thread.sleep(120000);
			//
			logger.info("format finished succeessfully");
		} catch (Exception e) {
			logger.info("error:", e);
		}
	}

	private static void cleanup(ZKHelixAdmin admin, ZkClient zkClient, String[] hostList, int numPartitions, int numReplicas,
			String kafkaTopicPrefix, String kafkaZKPath) {
		try {
			// TODO, delete not supported until 0.8.1, we'll enable it later
			for (int i = 0; i < numPartitions; i++) {
				//zkClient = new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer);
				zkClient.deleteRecursive(ZkUtils.getTopicPath(kafkaTopicPrefix + i));
			}
			zkClient.close();
			//ZKHelixAdmin admin = new ZKHelixAdmin(args[1]);
			admin.dropCluster(PistachiosConstants.CLUSTER_NAME);
		} catch (Exception e) {
			logger.info("error:", e);
		}
		logger.info("cleanup finished succeessfully");
	}

	public static void main(String[] args) {
		String usage = "Usage: xxxx [format/cleanup/info] [comma seperated cluster] [num partition] [num replica] (kafka zk path, optional)";

		if (args.length > 0 && "info".equals(args[0])) {
			getClusterInfo();
			return;
		}

		if (args.length < 4) {
			System.out.println("invalid number of parameters");
			System.out.println(usage);
			return;
		}

		int numPartitions = -1;
		int numReplicas = -1;
		String[] hostList = null;
		String kafkaZKPath = null;
		String kafkaTopicPrefix = null;

		boolean cleanup = false;

		try {
			numPartitions = Integer.parseInt(args[2]);
			numReplicas = Integer.parseInt(args[3]);
			if ("cleanup".equals(args[0])) {
				cleanup = true;
			}
			hostList = args[1].split(",");
			if (args.length >= 5)
				kafkaZKPath = args[4];

		} catch (Exception e) {
		}

		kafkaTopicPrefix = ConfigurationManager.getConfiguration().getString("Pistachio.Kafka.TopicPrefix");

		if (numPartitions == -1 || numReplicas == -1 || hostList == null || hostList.length == 0 || kafkaTopicPrefix == null) {
			System.out.println("invalid parameters");
			System.out.println(usage);
			return;
		}

		String zookeeperConnStr = ConfigurationManager.getConfiguration().getString("Pistachio.ZooKeeper.Server");
		try {
			ZKHelixAdmin admin = new ZKHelixAdmin(zookeeperConnStr);

			ZkClient zkClient = new ZkClient(zookeeperConnStr + (kafkaZKPath != null ? "/" + kafkaZKPath : ""), 30000, 30000,
					ZKStringSerializer$.MODULE$);

			if (cleanup) {
				cleanup(admin, zkClient, hostList, numPartitions, numReplicas, kafkaTopicPrefix, kafkaZKPath);
			} else {
				format(admin, zkClient, hostList, numPartitions, numReplicas, kafkaTopicPrefix, kafkaZKPath);
			}

			zkClient.close();
			//Thread.sleep(120000);
		} catch (Exception e) {
			logger.info("error:", e);
		}
	}

}
