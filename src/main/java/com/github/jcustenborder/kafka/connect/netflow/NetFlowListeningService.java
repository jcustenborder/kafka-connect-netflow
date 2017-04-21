/**
 * Copyright © 2017 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.netflow;

import com.github.jcustenborder.kafka.connect.utils.data.SourceRecordConcurrentLinkedDeque;
import com.github.jcustenborder.netty.netflow.v9.NetFlowV9Decoder;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;

class NetFlowListeningService extends AbstractExecutionThreadService {
  private static final Logger log = LoggerFactory.getLogger(NetFlowListeningService.class);
  private final Time time;
  private final NetFlowSourceConnectorConfig config;
  private final SourceRecordConcurrentLinkedDeque records;
  private final ConcurrentLinkedDeque<NetFlowV9Decoder.NetFlowMessage> messages;
  private final NetFlowConverter converter;

  EventLoopGroup bossGroup = new NioEventLoopGroup(1); // (1)
  Bootstrap b;
  ChannelFuture channelFuture;

  public NetFlowListeningService(NetFlowSourceConnectorConfig config, SourceRecordConcurrentLinkedDeque records, Time time) {
    this.config = config;
    this.records = records;
    this.time = time;
    this.messages = new ConcurrentLinkedDeque<>();
    this.converter = new NetFlowConverter(this.config);
  }


  @Override
  protected void startUp() throws Exception {
    log.info("startUp");
    b = new Bootstrap();
    b.group(bossGroup)
        .channel(NioDatagramChannel.class)
        .handler(new ChannelInitializer<DatagramChannel>() {
          @Override
          protected void initChannel(DatagramChannel datagramChannel) throws Exception {
            ChannelPipeline channelPipeline = datagramChannel.pipeline();
            channelPipeline.addLast(
                new LoggingHandler("NetFlow", LogLevel.INFO),
                new NetFlowV9Decoder(),
                new NetFlowV9RequestHandler(messages)
            );
          }
        });


    log.info("Binding to {}", this.config.listenPort);
    this.channelFuture = b.bind(this.config.listenPort).sync();

    log.info("finished startup");
  }

  @Override
  protected void run() throws Exception {
    try {
      while (isRunning() || !this.messages.isEmpty()) {
        try {
          processMessages();
        } catch (Exception ex) {
          log.error("Exception thrown", ex);
        }
        this.channelFuture.channel().closeFuture().await(1, TimeUnit.SECONDS);
      }
    } catch (Exception ex) {
      log.error("Exception thrown", ex);
      throw ex;
    }
  }

  void processMessages() {
    NetFlowV9Decoder.NetFlowMessage message;

    while ((message = this.messages.poll()) != null) {
      SourceRecord record = this.converter.convertFlow(message);
      this.records.add(record);
    }
  }

  @Override
  protected void shutDown() throws Exception {
    log.info("shutDown");
    try {
      this.channelFuture.channel().close();
      this.channelFuture.channel().closeFuture().sync();
      bossGroup.shutdownGracefully();
    } catch (InterruptedException e) {
      log.error("exception thrown", e);
    }
  }

}