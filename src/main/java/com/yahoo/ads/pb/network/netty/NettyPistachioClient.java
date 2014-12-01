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

import java.util.Arrays;
import java.util.List;
import com.yahoo.ads.pb.network.netty.NettyPistachioProtocol.*;

/**
 * Sends a list of continent/city pairs to a {@link NettyPistachioServer} to
 * get the local times of the specified cities.
 */
public final class NettyPistachioClient {

    static final boolean SSL = System.getProperty("ssl") != null;
    static final String HOST = System.getProperty("host", "127.0.0.1");
    static final int PORT = Integer.parseInt(System.getProperty("port", "8463"));
    static final List<String> CITIES = Arrays.asList(System.getProperty(
            "cities", "Asia/Seoul,Europe/Berlin,America/Los_Angeles").split(","));

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
            Response res = handler.lookup(12345L);

            // Close the connection.
            ch.close();

            // Print the response at last but not least.
            System.out.println("response: "+ res.getData());
        } finally {
            group.shutdownGracefully();
        }
    }
}
