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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import com.yahoo.ads.pb.network.netty.NettyPistachioProtocol.*;
import com.google.protobuf.ByteString;

import java.util.Calendar;
import java.util.TimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Calendar.*;

public class NettyPistachioServerHandler extends SimpleChannelInboundHandler<Request> {
	private static Logger logger = LoggerFactory.getLogger(NettyPistachioServerHandler.class);

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Request request) throws Exception {
        logger.debug("got request: {}", request);

		Response.Builder builder = Response.newBuilder();
		builder.setId(request.getId());
		builder.setSucceeded(true);
        builder.setThreadId(request.getThreadId());
        builder.setRequestId(request.getRequestId());
		builder.setData(ByteString.copyFromUtf8("succeeded"));


        Response response = builder.build();
        logger.debug("writing response: {}", response);
        ctx.write(response);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        logger.info("error: ", cause);
        ctx.close();
    }

}
