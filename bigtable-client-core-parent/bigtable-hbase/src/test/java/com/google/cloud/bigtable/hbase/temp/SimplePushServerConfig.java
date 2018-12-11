package com.google.cloud.bigtable.hbase.temp;

/**
 * Configuration settings for SimplePush server
 */
public interface SimplePushServerConfig {

  /**
   * The default prefix for the the notification endpoint url. This
   * prefix will be used and returned to the client to enable the
   * client to PUT notifications.
   */
  String DEFAULT_ENDPOINT_URL_PREFIX = "/update";

  /**
   * The host that this server will bind to.
   *
   * @return {@code String} the host.
   */
  String host();

  /**
   * The port that this server will bind to.
   *
   * @return {@code port} the port.
   */
  int port();

  /**
   * Determins whether transport layer security is in use.
   *
   * @return {@code true} if transport layer security is in use.
   */
  boolean useEndpointTls();

  /**
   * The password for the private key.
   *
   * @return {@code String[]} password used for generating the server's private key.
   */
  String password();

  /**
   * Returns the endpoint url prefix for this SimplePush server.
   * This will get the channelId appended to it.
   *
   * @return {@code String} the endpoint url prefix.
   */
  String endpointPrefix();

  /**
   * Returns the notification endpoint url prefix for this SimplePush server.
   * This will be the in the format:
   * protocol://endpointHost:endpointPort/endpointPrefix
   *
   * @return {@code String} the notification url.
   */
  String endpointUrl();

  /**
   * The externally available host that this server is reachable by.
   *
   * @return {@code String} the host.
   */
  String endpointHost();

  /**
   * The externally available port that this server is reachable by.
   *
   * @return {@code port} the port.
   */
  int endpointPort();

  /**
   * Returns the UserAgent reaper time out.
   * This is the amount of time which a UserAgent can be inactive after
   * which it will be removed from the system.
   *
   * @return {@code long} the timeout in milliseconds.
   */
  long userAgentReaperTimeout();

  /**
   * Returns the acknowledgement interval.
   * This is the interval time for resending non acknowledged notifications.
   *
   * @return {@code long} the interval in milliseconds.
   */
  long acknowledmentInterval();

  /**
   * Returns the maxium number of threads that will be used for handling
   * notifications.
   *
   * @return {@code int} the maximum number of threads allowed to be created.
   */
  int notifierMaxThreads();

}