package com.google.cloud.bigtable.hbase.temp;
public abstract class AbstractSockJsServiceFactory implements SockJsServiceFactory {

  private final SockJsConfig config;

  protected AbstractSockJsServiceFactory(final SockJsConfig config) {
    this.config = config;
  }

  @Override
  public abstract SockJsService create();

  @Override
  public SockJsConfig config() {
    return config;
  }

}