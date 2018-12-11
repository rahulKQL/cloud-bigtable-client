package com.google.cloud.bigtable.hbase.temp;
public class StandaloneConfig {

  private final SimplePushServerConfig pushConfig;
  private final SockJsConfig sockJsConfig;

  public StandaloneConfig(final SimplePushServerConfig pushConfig, final SockJsConfig sockJsConfig) {
    this.pushConfig = pushConfig;
    this.sockJsConfig = sockJsConfig;
  }

  public SimplePushServerConfig simplePushServerConfig() {
    return pushConfig;
  }

  public SockJsConfig sockJsConfig() {
    return sockJsConfig;
  }

  @Override
  public String toString() {
    return "StandaloneConfig[simplePushConfig=" + pushConfig + ", sockJsConfig=" + sockJsConfig + "]";
  }

}