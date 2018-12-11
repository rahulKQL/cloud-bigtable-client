package com.google.cloud.bigtable.hbase.temp;

/**
 * Factory class that creates instances of {@link SimplePushSockJSService}.
 */
public class SimplePushServiceFactory extends AbstractSockJsServiceFactory {

  private final SimplePushServer simplePushServer;

  /**
   * Sole constructor.
   *
   * @param sockjsConfig the Netty SockJS configuration.
   * @param simplePushServer the {@link SimplePushServer} to be used by all instances created.
   */
  public SimplePushServiceFactory(final SockJsConfig sockjsConfig,
      final SimplePushServer simplePushServer) {
    super(sockjsConfig);
    this.simplePushServer = simplePushServer;
  }

  @Override
  public SockJsService create() {
    return new SimplePushSockJSService(config(), simplePushServer);
  }

}