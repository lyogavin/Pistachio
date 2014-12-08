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
import com.google.common.collect.Lists;
import com.google.common.base.Function;
import com.yahoo.ads.pb.PistachiosHandler;

import java.util.Calendar;
import java.util.TimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Calendar.*;

public class NettyPistachioServerHandler extends SimpleChannelInboundHandler<Request> {
	private static Logger logger = LoggerFactory.getLogger(NettyPistachioServerHandler.class);
    private PistachiosHandler handler;

    public NettyPistachioServerHandler(PistachiosHandler handler) {
        this.handler = handler;
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Request request) throws Exception {
        logger.debug("got new request: {}", request);
		Response.Builder builder = Response.newBuilder();
		builder.setId(request.getId());
        boolean result = false;

        switch (request.getType()) {
            case LOOKUP: 
                logger.debug("calling lookup");
                try {
                    byte[] res = handler.lookup(request.getId(), request.getPartition());
                    builder.setSucceeded(true);
                    logger.debug("got data: {}", res);
                    if (res != null) {
                        logger.debug("empty data");
                        builder.setData(ByteString.copyFrom(res));
                    }
                } catch (Exception e) {
                    logger.info("error lookup", e);
                    builder.setSucceeded(false);
                }

                break;
            case STORE:
                logger.debug("calling store");
                result = handler.store(request.getId(), request.getPartition(), request.getData().toByteArray());
                builder.setSucceeded(result);
                break;
            case PROCESS_EVENT:
                logger.debug("calling processs");
                result = handler.processBatch(
                    request.getId(), 
                    request.getPartition(), 
                    Lists.transform(request.getEventsList(), 
                                    new Function<ByteString, byte[]>() {
                                        public byte[] apply(ByteString from) {
                                            return from.toByteArray();
                                        }
                                    }));
                builder.setSucceeded(result);
                break;
            default:
                logger.debug("default branch");
                break;
        }

        builder.setThreadId(request.getThreadId());
        builder.setRequestId(request.getRequestId());


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
