package com.google.cloud.bigtable.hbase.temp;

import java.util.concurrent.ScheduledFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SimplePush server implementation using SockJS.
 */
public class SimplePushSockJSService implements SockJsService {

  private final Logger logger = LoggerFactory.getLogger(SimplePushSockJSService.class);

  private final SockJsConfig sockjsConfig;
  private final SimplePushServer simplePushServer;
  private String uaid;
  private SockJsSessionContext session;
  private ScheduledFuture<?> ackJobFuture;

  /**
   * Sole constructor.
   *
   * @param sockjsConfig the SockJS {@link SockJsConfig} for this service.
   * @param simplePushServer the {@link SimplePushServer} that this instance will use.
   */
  public SimplePushSockJSService(final SockJsConfig sockjsConfig, final SimplePushServer simplePushServer) {
    this.sockjsConfig = sockjsConfig;
    this.simplePushServer = simplePushServer;
  }

  @Override
  public SockJsConfig config() {
    return sockjsConfig;
  }

  @Override
  public void onOpen(final SockJsSessionContext session) {
    logger.info("SimplePushSockJSServer onOpen");
    this.session = session;
  }

  @Override
  @SuppressWarnings("incomplete-switch")
  public void onMessage(final String message) throws Exception {
    System.out.println(uaid);
    System.out.println(message);;
  }


  @Override
  public void onClose() {
    logger.info("SimplePushSockJSServer onClose");
    if (ackJobFuture != null) {
      ackJobFuture.cancel(true);
    }
  }

}