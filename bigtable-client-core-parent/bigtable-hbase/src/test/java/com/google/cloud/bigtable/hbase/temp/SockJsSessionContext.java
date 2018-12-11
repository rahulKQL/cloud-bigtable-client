package com.google.cloud.bigtable.hbase.temp;

import io.grpc.netty.shaded.io.netty.channel.ChannelHandlerContext;

/**
 * Allows a {@link SockJsService} to interact with its session by providing
 * methods to send data, and to close the session.
 */
public interface SockJsSessionContext {
  /**
   * Send data to the current session. This data might be delivered immediately
   * of queued up in the session depending on the type of session (polling, streaming etc)
   *
   * @param message the message to be sent.
   */
  void send(String message);

  /**
   * Close the current session.
   */
  void close();

  /**
   * Get the underlying ChannelHandlerContext.
   */
  ChannelHandlerContext getContext();

}