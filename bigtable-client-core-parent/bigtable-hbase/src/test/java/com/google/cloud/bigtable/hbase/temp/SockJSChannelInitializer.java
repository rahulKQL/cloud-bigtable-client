package com.google.cloud.bigtable.hbase.temp;

import io.grpc.netty.shaded.io.netty.channel.ChannelInitializer;
import io.grpc.netty.shaded.io.netty.channel.ChannelPipeline;
import io.grpc.netty.shaded.io.netty.channel.socket.SocketChannel;
import io.grpc.netty.shaded.io.netty.handler.codec.http.HttpObjectAggregator;
import io.grpc.netty.shaded.io.netty.handler.codec.http.HttpServerCodec;
import io.grpc.netty.shaded.io.netty.util.concurrent.EventExecutorGroup;

/**
 * The Netty {@link ChannelInitializer} for the SimplePush Server.
 */
public class SockJSChannelInitializer extends ChannelInitializer<SocketChannel> {

  private final SimplePushServerConfig simplePushConfig;
  private final EventExecutorGroup backgroundGroup;
  private final SockJsConfig sockjsConfig;

  /**
   * Sole constructor.
   *
   * @param simplePushConfig the {@link SimplePushServerConfig} configuration.
   * @param sockjsConfig the SockJS {@link SimplePushServerConfig}.
   */
  public SockJSChannelInitializer(final SimplePushServerConfig simplePushConfig,
      final SockJsConfig sockjsConfig,
      final EventExecutorGroup backgroundGroup) {
    this.simplePushConfig = simplePushConfig;
    this.sockjsConfig = sockjsConfig;
    this.backgroundGroup = backgroundGroup;
  }

  @Override
  protected void initChannel(final SocketChannel socketChannel) throws Exception {
    final ChannelPipeline pipeline = socketChannel.pipeline();
    pipeline.addLast(new HttpServerCodec());
    pipeline.addLast(new HttpObjectAggregator(65536));

    final DefaultSimplePushServer simplePushServer = new DefaultSimplePushServer(simplePushConfig);
    pipeline.addLast(new SockJsHandler(new SimplePushServiceFactory(sockjsConfig,
        simplePushServer)));
    pipeline.addLast(backgroundGroup, new UserAgentHandler(simplePushServer));
  }

}
