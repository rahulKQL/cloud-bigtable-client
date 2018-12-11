package com.google.cloud.bigtable.hbase.temp;

public interface SimplePushServer {


  String handleShake(String helloMessage);


  /**
   * Returns the configuration for this SimplePush server.
   *
   * @return {@link SimplePushServerConfig} this servers configuration.
   */
  SimplePushServerConfig config();

}
