package com.google.cloud.bigtable.hbase.temp.http2;

import io.grpc.netty.shaded.io.netty.channel.ChannelHandlerContext;
import io.grpc.netty.shaded.io.netty.channel.ChannelInboundHandlerAdapter;
import io.grpc.netty.shaded.io.netty.channel.ChannelInitializer;
import io.grpc.netty.shaded.io.netty.channel.ChannelPipeline;
import io.grpc.netty.shaded.io.netty.channel.SimpleChannelInboundHandler;
import io.grpc.netty.shaded.io.netty.channel.socket.SocketChannel;
import io.grpc.netty.shaded.io.netty.handler.codec.http.HttpMessage;
import io.grpc.netty.shaded.io.netty.handler.codec.http.HttpObjectAggregator;
import io.grpc.netty.shaded.io.netty.handler.codec.http.HttpServerCodec;
import io.grpc.netty.shaded.io.netty.handler.codec.http.HttpServerUpgradeHandler;
import io.grpc.netty.shaded.io.netty.handler.codec.http.HttpServerUpgradeHandler.UpgradeCodec;
import io.grpc.netty.shaded.io.netty.handler.codec.http.HttpServerUpgradeHandler.UpgradeCodecFactory;
import io.grpc.netty.shaded.io.netty.handler.codec.http2.CleartextHttp2ServerUpgradeHandler;
import io.grpc.netty.shaded.io.netty.handler.codec.http2.Http2CodecUtil;
import io.grpc.netty.shaded.io.netty.handler.codec.http2.Http2ServerUpgradeCodec;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.util.AsciiString;
import io.grpc.netty.shaded.io.netty.util.ReferenceCountUtil;

/**
 * Sets up the Netty pipeline for the example server. Depending on the endpoint config, sets up the
 * pipeline for NPN or cleartext HTTP upgrade to HTTP/2.
 */
public class Http2ServerInitializer extends ChannelInitializer<SocketChannel> {

  private static final UpgradeCodecFactory upgradeCodecFactory = new UpgradeCodecFactory() {
    @Override
    public UpgradeCodec newUpgradeCodec(CharSequence protocol) {
      if (AsciiString.contentEquals(Http2CodecUtil.HTTP_UPGRADE_PROTOCOL_NAME, protocol)) {
        return new Http2ServerUpgradeCodec(new HelloWorldHttp2HandlerBuilder().build());
      } else {
        return null;
      }
    }
  };

  private final SslContext sslCtx;
  private final int maxHttpContentLength;

  public Http2ServerInitializer(SslContext sslCtx) {
    this(sslCtx, 16 * 1024);
  }

  public Http2ServerInitializer(SslContext sslCtx, int maxHttpContentLength) {
    if (maxHttpContentLength < 0) {
      throw new IllegalArgumentException("maxHttpContentLength (expected >= 0): " + maxHttpContentLength);
    }
    this.sslCtx = sslCtx;
    this.maxHttpContentLength = maxHttpContentLength;
  }

  @Override
  public void initChannel(SocketChannel ch) {
    System.out.println(ch);
    System.out.println("h2si initChannel");
    if (sslCtx != null) {
      configureSsl(ch);
    } else {
      configureClearText(ch);
    }
  }

  /**
   * Configure the pipeline for TLS NPN negotiation to HTTP/2.
   */
  private void configureSsl(SocketChannel ch) {
    ch.pipeline().addLast(sslCtx.newHandler(ch.alloc()), new Http2OrHttpHandler());
  }

  /**
   * Configure the pipeline for a cleartext upgrade from HTTP to HTTP/2.0
   */
  private void configureClearText(SocketChannel ch) {
    final ChannelPipeline p = ch.pipeline();
    final HttpServerCodec sourceCodec = new HttpServerCodec();
    final HttpServerUpgradeHandler upgradeHandler = new HttpServerUpgradeHandler(sourceCodec,
        upgradeCodecFactory);

    ch.pipeline().addFirst(new HelloWorldHttp1Handler("ALPN Negotiation"));
    final CleartextHttp2ServerUpgradeHandler cleartextHttp2ServerUpgradeHandler =
        new CleartextHttp2ServerUpgradeHandler(sourceCodec, upgradeHandler,
            new HelloWorldHttp2HandlerBuilder().build());


    p.addLast(cleartextHttp2ServerUpgradeHandler);

    p.addLast(new SimpleChannelInboundHandler<HttpMessage>() {
      @Override
      protected void channelRead0(ChannelHandlerContext ctx, HttpMessage msg) throws Exception {
        // If this handler is hit then no upgrade has been attempted and the client is just talking HTTP.
        System.err.println("Directly talking: " + msg.protocolVersion() + " (no upgrade was attempted)");
        ChannelPipeline pipeline = ctx.pipeline();
        ChannelHandlerContext thisCtx = pipeline.context(this);
        pipeline.addAfter(thisCtx.name(), null, new HelloWorldHttp1Handler("Direct. No Upgrade Attempted."));
        pipeline.replace(this, null, new HttpObjectAggregator(maxHttpContentLength));
        ctx.fireChannelRead(ReferenceCountUtil.retain(msg));
      }
    });

    p.addLast(new UserEventLogger());
  }

  /**
   * Class that logs any User Events triggered on this channel.
   */
  private static class UserEventLogger extends ChannelInboundHandlerAdapter {
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
      System.out.println("User Event Triggered: " + evt);
      if(evt instanceof  CleartextHttp2ServerUpgradeHandler.PriorKnowledgeUpgradeEvent){
        ctx.fireChannelRead(evt);
        ctx.fireUserEventTriggered(evt);
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
      System.out.println(cause);
      throw new Exception(cause);
    }
  }
}