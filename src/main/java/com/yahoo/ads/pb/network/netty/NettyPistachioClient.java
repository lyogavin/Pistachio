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
package com.yahoo.ads.pb.network.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.channel.ChannelOption;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import com.yahoo.ads.pb.network.netty.NettyPistachioProtocol.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.protobuf.ByteString;
import org.apache.commons.configuration.Configuration;
import com.yahoo.ads.pb.util.ConfigurationManager;
import com.yahoo.ads.pb.helix.HelixPartitionSpectator;

import com.yahoo.ads.pb.PistachiosClientImpl;
import java.net.InetAddress;
import com.yahoo.ads.pb.Partitioner;
import com.yahoo.ads.pb.PistachiosServer;
import com.yahoo.ads.pb.DefaultPartitioner;

/**
 * Sends a list of continent/city pairs to a {@link NettyPistachioServer} to
 * get the local times of the specified cities.
 */
public final class NettyPistachioClient implements PistachiosClientImpl{
	private static Logger logger = LoggerFactory.getLogger(NettyPistachioClient.class);

    static final boolean SSL = System.getProperty("ssl") != null;
    static final String HOST = System.getProperty("host", "127.0.0.1");
//    static final int PORT = Integer.parseInt(System.getProperty("port", "8463"));
    static final int PORT = ConfigurationManager.getConfiguration().getInt("Network.Netty.Port", 9091);

    private NettyPistachioClientHandler handler;
    private List<Channel> channelList = new ArrayList<Channel>();

    private ConcurrentHashMap<String, NettyPistachioClientHandler> hostHandlerMap = new ConcurrentHashMap<String, NettyPistachioClientHandler>();
    private EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
	private HelixPartitionSpectator helixPartitionSpectator;
	static final String ZOOKEEPER_SERVER = "Pistachio.ZooKeeper.Server";
	static final String PROFILE_HELIX_INSTANCE_ID = "Profile.Helix.InstanceId";
	private Configuration conf = ConfigurationManager.getConfiguration();
    private Partitioner partitioner = new DefaultPartitioner();
    private String localHostAddress;

    public NettyPistachioClient() throws Exception{
		try {
			helixPartitionSpectator = new HelixPartitionSpectator(
								conf.getString(ZOOKEEPER_SERVER), // zkAddr
								"PistachiosCluster",
								InetAddress.getLocalHost().getHostName() //conf.getString(PROFILE_HELIX_INSTANCE_ID) // instanceName
								);

            localHostAddress = InetAddress.getLocalHost().getHostAddress();

		} catch(Exception e) {
			logger.error("Error init HelixPartitionSpectator, are zookeeper and helix installed and configured correctly?", e);
			throw e;
		}
    }

    private boolean isLocalCall(long partition) {
        boolean directLocalCall = false;
        String ip = helixPartitionSpectator.getOneInstanceForPartition("PistachiosResource", (int)partition, "MASTER");

        try {
            if (ip.equals(localHostAddress) && PistachiosServer.servingAsServer()) {
                directLocalCall = true;
            }
        } catch (Exception e) {
            logger.info("error:", e);
        }

        return directLocalCall;
    }

    private NettyPistachioClientHandler getHandler(long id) {

        long partition = partitioner.getPartition(id, helixPartitionSpectator.getTotalPartition("PistachiosResource"));

        String ip = helixPartitionSpectator.getOneInstanceForPartition("PistachiosResource", (int)partition, "MASTER");
        logger.debug("ip {} found for id {} partition {}", ip, id, partition);

        if (ip == null) {
            logger.info("partition not found for id {} partition{}", id, partition);
            return null;
        }

        if (!hostHandlerMap.containsKey(ip)) {
            synchronized(hostHandlerMap) {
                try {
                    // Configure SSL.
                    final SslContext sslCtx;
                    if (SSL) {
                        sslCtx = SslContext.newClientContext(InsecureTrustManagerFactory.INSTANCE);
                    } else {
                        sslCtx = null;
                    }

                    Bootstrap b = new Bootstrap();
                    b.group(eventLoopGroup)
                        .channel(NioSocketChannel.class)
                        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, conf.getInt("Network.Netty.ClientConnectionTimeoutMillis", 10000))
                        .handler(new NettyPistachioClientInitializer(sslCtx));

                    // Make a new connection.
                    Channel ch = b.connect(ip, PORT).sync().channel();
                    channelList.add(ch);

                    // Get the handler instance to initiate the request.
                    NettyPistachioClientHandler handler = ch.pipeline().get(NettyPistachioClientHandler.class);
                    hostHandlerMap.put(ip, handler);

                } catch (Throwable e) {
                    logger.info("error constructor ", e);
                }
            }
        }
        return hostHandlerMap.get(ip);
    }

    public void close() {
        for (Channel channel: channelList) {
            channel.close();
        }
        eventLoopGroup.shutdownGracefully();
    }

	public byte[] lookup(long id) {
        long partition = partitioner.getPartition(id, helixPartitionSpectator.getTotalPartition("PistachiosResource"));
        if (isLocalCall(partition)) {
            return PistachiosServer.handler.lookup(id, partition);
        }
        NettyPistachioClientHandler handler = getHandler(id);
        if (handler == null) {
            logger.debug("fail to look up {} because of empty handler", id);
            return null;
        }
        Request.Builder builder = Request.newBuilder();
        Response res = handler.sendRequest(builder.setId(id).setType(RequestType.LOOKUP).setPartition(partition));
        return res.getData().toByteArray();
    }

	public boolean store(long id, byte[] value) {
        long partition = partitioner.getPartition(id, helixPartitionSpectator.getTotalPartition("PistachiosResource"));
        if (isLocalCall(partition)) {
            return PistachiosServer.handler.store(id, partition, value);
        }
        NettyPistachioClientHandler handler = getHandler(id);
        if (handler == null) {
            logger.debug("fail to look up {} because of empty handler", id);
            return false;
        }
        Request.Builder builder = Request.newBuilder();
        Response res = handler.sendRequest(builder.setId(id).setType(RequestType.STORE).setPartition(partition).setData(ByteString.copyFrom(value)));
        return res.getSucceeded();
    }
    public boolean processBatch(long id, List<byte[]> events) {
        NettyPistachioClientHandler handler = getHandler(id);
        if (handler == null) {
            logger.debug("fail to look up {} because of empty handler", id);
            return false;
        }
        Request.Builder builder = Request.newBuilder();
        long partition = partitioner.getPartition(id, helixPartitionSpectator.getTotalPartition("PistachiosResource"));
        builder.setId(id).setType(RequestType.PROCESS_EVENT).setPartition(partition);
        for (byte[] event : events) {
            builder.addEvents(ByteString.copyFrom(event));
        }

        if ((conf.getString("pistachio.process.jar.server.url") != null) && 
           (conf.getString("pistachio.process.class") != null)) {
            builder.setJarServerUrl(conf.getString("pistachio.process.jar.server.url"));
            builder.setProcessClass(conf.getString("pistachio.process.class"));
        }
        Response res = handler.sendRequest(builder);
        return res.getSucceeded();
    }

    static private class TestRunnable extends Thread {
        private java.util.Random rand = new java.util.Random();
        private NettyPistachioClientHandler handler;
        public TestRunnable(NettyPistachioClientHandler handler0) {
            handler = handler0;
        }
        public void run() {
            try {
                // Request and get the response.
                Long l = rand.nextLong();
                    System.out.println("sending : " + l);
                Request.Builder builder = Request.newBuilder();
                Response res = handler.sendRequest(builder.setId(l));

                if (l != res.getId()) {
                    System.out.println("Thread #" + Thread.currentThread().getId() + ", incorrect result: " + l + ", " + res.getId());
                } else  {
                    System.out.println("Thread #" + Thread.currentThread().getId() + ", correct result: " + l );
                }
            } catch (Exception e) {
                    System.out.println("error: " + e);
            }

        }
    }

    public static void main(String[] args) throws Exception {
        // Configure SSL.
        final SslContext sslCtx;
        if (SSL) {
            sslCtx = SslContext.newClientContext(InsecureTrustManagerFactory.INSTANCE);
        } else {
            sslCtx = null;
        }
            System.out.println("start test");

        EventLoopGroup group = new NioEventLoopGroup();
        try {
            Bootstrap b = new Bootstrap();
            b.group(group)
             .channel(NioSocketChannel.class)
             .handler(new NettyPistachioClientInitializer(sslCtx));

            // Make a new connection.
            Channel ch = b.connect(HOST, PORT).sync().channel();

            // Get the handler instance to initiate the request.
            NettyPistachioClientHandler handler = ch.pipeline().get(NettyPistachioClientHandler.class);

            // Request and get the response.
            //Response res = handler.lookup(12345L);
            Thread [] t = new Thread[1000];
            for (int i = 0; i< 1000; i++) {
                t[i] = new TestRunnable(handler);
            }
            for (int i = 0; i< 1000; i++) {
                t[i].start();
            }
            for (int i = 0; i< 1000; i++) {
                t[i].join();
            }

            // Close the connection.
            ch.close();

            // Print the response at last but not least.
            //System.out.println("response: "+ res.getData());
        } finally {
            group.shutdownGracefully();
        }
    }
}
