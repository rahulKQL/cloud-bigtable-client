package com.google.cloud.bigtable.hbase.temp;

public class DefaultSimplePushServer implements SimplePushServer {

  private final SimplePushServerConfig config;

  /**
   * Sole constructor.
   *
   * @param config the {@link SimplePushServerConfig} for this server.
   */
  public DefaultSimplePushServer(final SimplePushServerConfig config) {
    this.config = config;
  }

  @Override
  public String handleShake(String helloMessage) {
    System.out.println("Hello Message:" + helloMessage);
    return "Hi there!";
  }

  @Override
  public SimplePushServerConfig config() {
    return config;
  }
}
