package com.google.cloud.bigtable.hbase.temp;
/**
 * A factory for creating {@link SockJsService} instances.
 */
public interface SockJsServiceFactory {

  /**
   * Creates a new instance, or reuse and existing instance, of the service.
   * Allows for either creating new instances for every session of to use
   * a single instance, whatever is appropriate for the use case.
   *
   * @return {@link SockJsService} the service instance.
   */
  SockJsService create();

  /**
   * The {@link SockJsConfig} for the session itself.
   *
   * @return Config the configuration for the session.
   */
  SockJsConfig config();

}