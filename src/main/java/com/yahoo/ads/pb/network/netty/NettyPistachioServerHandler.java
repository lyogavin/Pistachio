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

import static java.util.Calendar.*;

public class NettyPistachioServerHandler extends SimpleChannelInboundHandler<Request> {

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Request request) throws Exception {
        long currentTime = System.currentTimeMillis();

		Response.Builder builder = Response.newBuilder();
		builder.setId(request.getId());
		builder.setSucceeded(true);
		builder.setData(ByteString.copyFromUtf8("succeeded"));


        ctx.write(builder.build());
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }

}
