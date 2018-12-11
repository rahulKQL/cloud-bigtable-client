package com.google.cloud.bigtable.hbase.temp;


import io.grpc.netty.shaded.io.netty.bootstrap.ServerBootstrap;
import io.grpc.netty.shaded.io.netty.channel.Channel;
import io.grpc.netty.shaded.io.netty.channel.EventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioServerSocketChannel;
import io.grpc.netty.shaded.io.netty.util.concurrent.DefaultEventExecutorGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A standalone Netty SockJS SimplePush Server.
 */
public class NettySockJSServer {

  private final Logger logger = LoggerFactory.getLogger(NettySockJSServer.class);

  private final StandaloneConfig config;

  public NettySockJSServer(final StandaloneConfig standaloneConfig) {
    this.config = standaloneConfig;
  }

  public void run() throws Exception {
    final EventLoopGroup bossGroup = new NioEventLoopGroup();
    final EventLoopGroup workerGroup = new NioEventLoopGroup();
    final DefaultEventExecutorGroup reaperExcutorGroup = new DefaultEventExecutorGroup(1);
    final SimplePushServerConfig simplePushConfig = config.simplePushServerConfig();
    try {
      final ServerBootstrap sb = new ServerBootstrap();
      sb.group(bossGroup, workerGroup)
          .channel(NioServerSocketChannel.class)
          .childHandler(new SockJSChannelInitializer(simplePushConfig, config.sockJsConfig(),
              reaperExcutorGroup));
      final Channel ch = sb.bind(simplePushConfig.host(), simplePushConfig.port()).sync().channel();
      logger.info("Server started");
      logger.debug(config.toString());
      ch.closeFuture().sync();
    } finally {
      bossGroup.shutdownGracefully();
      workerGroup.shutdownGracefully();
    }
  }

  /**
   * Starts the server using an optional passed in configuration file.
   * </p>
   * Options:
   * <pre>
   * path/to/config.json
   * </pre>
   *
   * @param args the command line arguments passed.
   * @throws Exception
   */
  public static void main(final String[] args) throws Exception {
    final String configFile = args.length == 1 ? args[0]: "/simplepush-config.json";
    final StandaloneConfig config = ConfigReader.parse(configFile);
    NettySockJSServer server = new NettySockJSServer(config);
    server.run();
    System.out.println(server.config);
  }

}