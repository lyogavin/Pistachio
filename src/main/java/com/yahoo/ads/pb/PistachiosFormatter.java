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
import org.apache.helix.HelixException;
import org.apache.helix.model.ClusterConstraints.ConstraintType;
import kafka.utils.ZKStringSerializer$;


//import com.yahoo.ads.pb.platform.perf.IncrementCounter;
//import com.yahoo.ads.pb.platform.perf.InflightCounter;
import com.yahoo.ads.pb.util.ConfigurationManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.configuration.Configuration;
import com.google.common.base.Joiner;

import kafka.admin.CreateTopicCommand;
import kafka.admin.DeleteTopicCommand;
import kafka.utils.ZkUtils;

import org.I0Itec.zkclient.ZkClient;
import org.apache.helix.model.InstanceConfig;
import     org.apache.helix.HelixDefinedState;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.ConstraintItem;
import org.apache.helix.model.ClusterConstraints.ConstraintAttribute;




// Generated code

import java.util.HashMap;
import java.util.List;
import java.util.Properties;


public class PistachiosFormatter{

    public static class PistachioClusterInfo{
        List<String> hostList;
        int numPartitions;
        int numReplicas;
    }

    private static Logger logger = LoggerFactory.getLogger(PistachiosServer.class);

  public static PistachioClusterInfo getClusterInfo() {
      try {
          String zookeeperConnStr = ConfigurationManager.getConfiguration().getString("Pistachio.ZooKeeper.Server");
          ZKHelixAdmin admin = new ZKHelixAdmin(zookeeperConnStr);
          IdealState idealState = admin.getResourceIdealState("PistachiosCluster", "PistachiosResource");
          PistachioClusterInfo info = new PistachioClusterInfo();
          info.numPartitions = idealState.getNumPartitions();
          info.numReplicas = Integer.parseInt(idealState.getReplicas());
          info.hostList = admin.getInstancesInCluster("PistachiosCluster");

          logger.info("num partitions: {}, num Replicas: {}, hostList: {}.", info.numPartitions,
              info.numReplicas, Joiner.on(",").join(info.hostList.toArray()));

          return info;
      } catch (Exception e) {
          logger.info("error getting cluster info", e);
          return null;
      }
  }

  private static void setConstraints(ZKHelixAdmin admin, int numPartitions) {

        logger.info("pause cluster");
        admin.enableCluster("PistachiosCluster", false);
        // setting partition constraints
        logger.info("setting per partition state transition constraints to 1");
        try {
            for (int constraintId = 0; constraintId < numPartitions; constraintId++) {
                java.util.HashMap<ConstraintAttribute, String>  attributes = new java.util.HashMap<ConstraintAttribute, String>();
                attributes.put(ConstraintAttribute.RESOURCE, "PistachiosResource");
                attributes.put(ConstraintAttribute.PARTITION, "PistachiosResource_"+constraintId);
                logger.info("setting per partition for {} state transition constraints to 1", "PistachiosResource_"+constraintId);
                admin.setConstraint("PistachiosCluster", ConstraintType.STATE_CONSTRAINT, "PistachiosPartitionTransitionConstraint" + constraintId,
                   new ConstraintItem(attributes,"1"));
            }

        } catch(Exception e) {
            logger.info("setting state transition constraints error, roll back and exit", e);
        }
        logger.info("resume cluster");
        admin.enableCluster("PistachiosCluster", true);
  }
  private static void format(ZKHelixAdmin admin, ZkClient zkClient, String[] hostList, int numPartitions, int numReplicas, String kafkaTopicPrefix, String kafkaZKPath) {
    try {
        for (int i =0; i<numPartitions; i++) {
            try {
                //CreateTopicCommand.createTopic(zkClient , "PistachiosPartition." + i, 1, 1, "");
                CreateTopicCommand.createTopic(zkClient , kafkaTopicPrefix + i, 1, 1, "");
            } catch (kafka.common.TopicExistsException tee) {
                logger.info("topic exists, continue", tee);
            } catch (Exception e) {
                logger.info("creating kafka topic error, roll back", e);
                cleanup(admin, zkClient, hostList, numPartitions, numReplicas, kafkaTopicPrefix, kafkaZKPath);
            }
        }
        zkClient.close();

        //ZKHelixAdmin admin = new ZKHelixAdmin(args[1]);
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
        } catch (HelixException he) {
            if (he.getMessage().contains("already exists")) {
                logger.info("state model def already exists, continue.", he);
            }
            else {
                logger.info("adding state model def error, roll back and exit", he);
                cleanup(admin, zkClient, hostList, numPartitions, numReplicas, kafkaTopicPrefix, kafkaZKPath);
            }
        } catch(Exception e) {
                logger.info("adding state model def error, roll back and exit", e);
                cleanup(admin, zkClient, hostList, numPartitions, numReplicas, kafkaTopicPrefix, kafkaZKPath);
        }

        logger.info("adding resource");
        try {
            admin.addResource("PistachiosCluster", "PistachiosResource", numPartitions, "MasterSlave", "SEMI_AUTO");
        } catch (HelixException he) {
            if (he.getMessage().contains("already exists")) {
                logger.info("resourece already exists, continue.", he);
            }
            else {
                logger.info("adding resource error, roll back and exit", he);
                cleanup(admin, zkClient, hostList, numPartitions, numReplicas, kafkaTopicPrefix, kafkaZKPath);
            }
        } catch(Exception e) {
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
                admin.addInstance("PistachiosCluster", instanceConfig);
            } catch (HelixException he) {
                if (he.getMessage().contains("already exists")) {
                    logger.info("instance already exists, continue.", he);
                }
                else {
                    logger.info("adding instance error, roll back and exit", he);
                    cleanup(admin, zkClient, hostList, numPartitions, numReplicas, kafkaTopicPrefix, kafkaZKPath);
                }
            } catch(Exception e) {
                    logger.info("adding instance error, roll back and exit", e);
                    cleanup(admin, zkClient, hostList, numPartitions, numReplicas, kafkaTopicPrefix, kafkaZKPath);
            }
        }
        logger.info("rebalancing");
        try {
        admin.rebalance("PistachiosCluster", "PistachiosResource", numReplicas);
        } catch(Exception e) {
            logger.info("rebalancing error, roll back and exit", e);
            cleanup(admin, zkClient, hostList, numPartitions, numReplicas, kafkaTopicPrefix, kafkaZKPath);
        }

        // setting partition constraints
        logger.info("setting per partition state transition constraints to 1");
        try {
            for (int constraintId = 0; constraintId < numPartitions; constraintId++) {
                java.util.HashMap<ConstraintAttribute, String>  attributes = new java.util.HashMap<ConstraintAttribute, String>();
                attributes.put(ConstraintAttribute.RESOURCE, "PistachiosResource");
                attributes.put(ConstraintAttribute.PARTITION, "PistachiosResource_"+constraintId);
                logger.info("setting per partition for {} state transition constraints to 1", "PistachiosResource_"+constraintId);
                admin.setConstraint("PistachiosCluster", ConstraintType.STATE_CONSTRAINT, "PistachiosPartitionTransitionConstraint" + constraintId,
                   new ConstraintItem(attributes,"1"));
            }

        } catch(Exception e) {
            logger.info("setting state transition constraints error, roll back and exit", e);
            cleanup(admin, zkClient, hostList, numPartitions, numReplicas, kafkaTopicPrefix, kafkaZKPath);
        }

        logger.info("adding topic to zk path: {}", kafkaZKPath);
        //ZkClient zkClient = new ZkClient(args[1]+ (kafkaZKPath != null ? "/" + kafkaZKPath : ""), 30000, 30000, ZKStringSerializer$.MODULE$);

        //Thread.sleep(120000);
        //
        logger.info("format finished succeessfully");
      } catch(Exception e) {
          logger.info("error:", e);
      }
  }

  private static void cleanup(ZKHelixAdmin admin, ZkClient zkClient, String[] hostList, int numPartitions, int numReplicas, String kafkaTopicPrefix, String kafkaZKPath) {
    try {
        // TODO, delete not supported until 0.8.1, we'll enable it later
        for (int i =0; i<numPartitions; i++) {
            //zkClient = new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer);
            zkClient.deleteRecursive(ZkUtils.getTopicPath(kafkaTopicPrefix + i));
        }
        zkClient.close();
        //ZKHelixAdmin admin = new ZKHelixAdmin(args[1]);
        admin.dropCluster("PistachiosCluster");
      } catch(Exception e) {
          logger.info("error:", e);
      }
        logger.info("cleanup finished succeessfully");
  }

  public static void main(String [] args) {
      String usage = "Usage: \tformat_cluster.sh info \n";
      usage += "\tformat_cluster.sh format [comma seperated cluster] [num partition] [num replica] (kafka zk path, optional) \n";
      usage += "\tformat_cluster.sh cleanup [comma seperated cluster] [num partition] [num replica] (kafka zk path, optional) \n";
      //usage += "\tformat_cluster.sh set_constraints [comma seperated cluster] [num partition] \n";
      usage += "\tkafkaTopicPrefix, ZK server conn string should also be set from config. \n";

      if (args.length > 0 && "info".equals(args[0])) {
          getClusterInfo();
          return;
      }

      if (args.length <4) {
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
      boolean setConstraints = false;

      try {
          numPartitions = Integer.parseInt(args[2]);
          numReplicas = Integer.parseInt(args[3]);
          if ("cleanup".equals(args[0])) {
              cleanup = true;
          }
          if ("set_constraints".equals(args[0])) {
              setConstraints = true;
          }
          hostList = args[1].split(",");
          if (args.length >=5)
              kafkaZKPath = args[4];

      } catch(Exception e) {
      }

      String zookeeperConnStr = ConfigurationManager.getConfiguration().getString("Pistachio.ZooKeeper.Server");
      kafkaTopicPrefix = ConfigurationManager.getConfiguration().getString("Profile.Kafka.TopicPrefix");

      if (numPartitions == -1 || numReplicas == -1|| hostList == null || hostList.length == 0 || kafkaTopicPrefix == null || zookeeperConnStr == null) {
          System.out.println("invalid parameters");
          System.out.println(usage);
          return;
      }





    try {
        ZKHelixAdmin admin = new ZKHelixAdmin(zookeeperConnStr);

        ZkClient zkClient = new ZkClient(zookeeperConnStr + (kafkaZKPath != null ? "/" + kafkaZKPath : ""), 30000, 30000, ZKStringSerializer$.MODULE$);

        if (setConstraints) {
            setConstraints(admin, numPartitions);

        } else if (cleanup) {
            cleanup(admin, zkClient, hostList, numPartitions, numReplicas, kafkaTopicPrefix, kafkaZKPath);
        } else {
            format(admin, zkClient, hostList, numPartitions, numReplicas, kafkaTopicPrefix, kafkaZKPath);
        }

        zkClient.close();
        //Thread.sleep(120000);
      } catch(Exception e) {
          logger.info("error:", e);
      }
  }

}
