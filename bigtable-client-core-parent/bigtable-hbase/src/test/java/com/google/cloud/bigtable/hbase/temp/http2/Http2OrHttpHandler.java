package com.google.cloud.bigtable.hbase.temp.http2;

import io.grpc.netty.shaded.io.netty.channel.ChannelHandlerContext;
import io.grpc.netty.shaded.io.netty.handler.codec.http.HttpObjectAggregator;
import io.grpc.netty.shaded.io.netty.handler.codec.http.HttpServerCodec;
import io.grpc.netty.shaded.io.netty.handler.ssl.ApplicationProtocolNames;
import io.grpc.netty.shaded.io.netty.handler.ssl.ApplicationProtocolNegotiationHandler;

/**
 * Negotiates with the browser if HTTP2 or HTTP is going to be used. Once decided, the Netty
 * pipeline is setup with the correct handlers for the selected protocol.
 */
public class Http2OrHttpHandler extends ApplicationProtocolNegotiationHandler {

  private static final int MAX_CONTENT_LENGTH = 1024 * 100;

  protected Http2OrHttpHandler() {
    super(ApplicationProtocolNames.HTTP_1_1);
  }

  @Override
  protected void configurePipeline(ChannelHandlerContext ctx, String protocol) throws Exception {
    if (ApplicationProtocolNames.HTTP_2.equals(protocol)) {
      ctx.pipeline().addLast(new HelloWorldHttp2HandlerBuilder().build());
      return;
    }

    if (ApplicationProtocolNames.HTTP_1_1.equals(protocol)) {
      ctx.pipeline().addLast(new HttpServerCodec(),
          new HttpObjectAggregator(MAX_CONTENT_LENGTH),
          new HelloWorldHttp1Handler("ALPN Negotiation"));
      return;
    }

    //Also tried with Http1Handler only.
//    if(protocol != null) {
//      ctx.pipeline().addLast(new HttpServerCodec(), new HttpObjectAggregator(MAX_CONTENT_LENGTH),
//          new HelloWorldHttp1Handler("ALPN Negotiation"));
//      return;
//    }

    throw new IllegalStateException("unknown protocol: " + protocol);
  }
}