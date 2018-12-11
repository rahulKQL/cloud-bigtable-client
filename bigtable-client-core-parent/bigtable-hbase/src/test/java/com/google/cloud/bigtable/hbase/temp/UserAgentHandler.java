package com.google.cloud.bigtable.hbase.temp;


import io.grpc.netty.shaded.io.netty.channel.ChannelHandlerAdapter;
import io.grpc.netty.shaded.io.netty.channel.ChannelHandlerContext;
import io.grpc.netty.shaded.io.netty.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
https://github.com/aerogear/aerogear-simplepush-server/blob/0e59b228ca5909bee2c18341142c709d462d1b8e/server-core/src/main/java/org/jboss/aerogear/simplepush/server/DefaultSimplePushServer.java
 */
public class UserAgentHandler extends ChannelHandlerAdapter {

  private final Logger logger = LoggerFactory.getLogger(UserAgentHandler.class);
  private final SimplePushServer simplePushServer;
  private static ScheduledFuture<?> scheduleFuture;
  private static final AtomicBoolean reaperStarted = new AtomicBoolean(false);

  /**
   * Sole constructor.
   *
   * @param simplePushServer the {@link SimplePushServer} that this reaper will operate on.
   */
  public UserAgentHandler(final SimplePushServer simplePushServer) {
    this.simplePushServer = simplePushServer;
  }

  @Override
  public void handlerAdded(final ChannelHandlerContext ctx) throws Exception {
    System.out.println("Hanlded ADded");
    if (started()) {
      return;
    }
    final SimplePushServerConfig config = simplePushServer.config();
    logger.info("Creating UserAgentReaper job : " + config.userAgentReaperTimeout());
    scheduleFuture = ctx.executor().scheduleAtFixedRate(new UserAgentReaper(simplePushServer),
        config.userAgentReaperTimeout(),
        config.userAgentReaperTimeout(),
        TimeUnit.MILLISECONDS);
    reaperStarted.set(true);
  }


  /**
   * Returns true if the reaper job has started.
   *
   * @return {@code true} if the reaper job has started, false otherwise.
   */
  public boolean started() {
    System.out.println("Started in UAHablder");
    return reaperStarted.get();
  }

  /**
   * Cancels the reaper job if it is active.
   */
  public void cancelReaper() {
    if (scheduleFuture != null) {
      if (scheduleFuture.cancel(true)) {
        reaperStarted.set(false);
      }
    }
  }

}
