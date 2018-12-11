package com.google.cloud.bigtable.hbase.temp;
/**
 * Represents the server side business application server in SockJS.
 */
public interface SockJsService {

  /**
   * The {@link SockJsConfig} for this service
   *
   * @return {@link SockJsConfig} this services configuration.
   */
  SockJsConfig config();

  /**
   * Will be called when a new session is opened.
   *
   * @param session the {@link SockJsSessionContext} which can be stored and used for sending/closing.
   */
  void onOpen(SockJsSessionContext session);

  /**
   * Will be called when a message is sent to the service.
   *
   * @param message the message sent from a client.
   * @throws Exception
   */
  void onMessage(String message) throws Exception;

  /**
   * Will be called when the session is closed.
   */
  void onClose();

}