package com.google.cloud.bigtable.hbase.temp;

import com.google.common.util.concurrent.Runnables;

public class UserAgentReaper implements Runnable {

  private SimplePushServer server;

  public UserAgentReaper(SimplePushServer simplePushServer){
    System.out.println(simplePushServer);

  }
  @Override
  public void run() {
    System.out.println("UserAgentReaper");
    System.out.println(server.config());
  }
}
