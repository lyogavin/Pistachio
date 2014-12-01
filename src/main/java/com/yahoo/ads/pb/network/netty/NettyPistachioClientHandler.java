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

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import com.yahoo.ads.pb.network.netty.NettyPistachioProtocol.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Formatter;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NettyPistachioClientHandler extends SimpleChannelInboundHandler<Response> {

    private static final Pattern DELIM = Pattern.compile("/");

    // Stateful properties
    private volatile Channel channel;
    private final BlockingQueue<Response> answer = new LinkedBlockingQueue<Response>();

    public NettyPistachioClientHandler() {
        super(false);
    }

	public Response lookup(Long id) {
		Request.Builder builder = Request.newBuilder();
		builder.setId(123);
        channel.writeAndFlush(builder.build());
        boolean interrupted = false;
		Response response;
        for (;;) {
            try {
                response = answer.take();
                break;
            } catch (InterruptedException ignore) {
                interrupted = true;
            }
        }

        if (interrupted) {
            Thread.currentThread().interrupt();
        }

		return response;

	}


    @Override
    public void channelRegistered(ChannelHandlerContext ctx) {
        channel = ctx.channel();
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Response response) throws Exception {
            System.out.println("got:"+ response.getId());
        answer.add(response);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
}
