package com.google.cloud.bigtable.hbase.temp.http2;

import io.grpc.netty.shaded.io.netty.buffer.ByteBuf;
import io.grpc.netty.shaded.io.netty.buffer.ByteBufUtil;
import io.grpc.netty.shaded.io.netty.channel.ChannelHandlerContext;
import io.grpc.netty.shaded.io.netty.handler.codec.http.FullHttpRequest;
import io.grpc.netty.shaded.io.netty.handler.codec.http.HttpHeaderNames;
import io.grpc.netty.shaded.io.netty.handler.codec.http.HttpMethod;
import io.grpc.netty.shaded.io.netty.handler.codec.http.HttpScheme;
import io.grpc.netty.shaded.io.netty.handler.codec.http.HttpServerUpgradeHandler;
import io.grpc.netty.shaded.io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.grpc.netty.shaded.io.netty.handler.codec.http2.Http2ConnectionDecoder;
import io.grpc.netty.shaded.io.netty.handler.codec.http2.Http2ConnectionEncoder;
import io.grpc.netty.shaded.io.netty.handler.codec.http2.Http2ConnectionHandler;
import io.grpc.netty.shaded.io.netty.handler.codec.http2.Http2Flags;
import io.grpc.netty.shaded.io.netty.handler.codec.http2.Http2FrameListener;
import io.grpc.netty.shaded.io.netty.handler.codec.http2.Http2Headers;
import io.grpc.netty.shaded.io.netty.handler.codec.http2.Http2Settings;
import io.grpc.netty.shaded.io.netty.util.CharsetUtil;

import static io.grpc.netty.shaded.io.netty.buffer.Unpooled.copiedBuffer;
import static io.grpc.netty.shaded.io.netty.buffer.Unpooled.unreleasableBuffer;
import static io.grpc.netty.shaded.io.netty.handler.codec.http.HttpResponseStatus.OK;

/**
 * A simple handler that responds with the message "Hello World!".
 */
public final class HelloWorldHttp2Handler extends Http2ConnectionHandler implements Http2FrameListener {

  static final ByteBuf RESPONSE_BYTES = unreleasableBuffer(copiedBuffer("Hello World", CharsetUtil.UTF_8));

  HelloWorldHttp2Handler(Http2ConnectionDecoder decoder, Http2ConnectionEncoder encoder,
      Http2Settings initialSettings) {
    super(decoder, encoder, initialSettings);
  }

  private static Http2Headers http1HeadersToHttp2Headers(FullHttpRequest request) {
    CharSequence host = request.headers().get(HttpHeaderNames.HOST);
    Http2Headers http2Headers = new DefaultHttp2Headers()
        .method(HttpMethod.GET.asciiName())
        .path(request.uri())
        .scheme(HttpScheme.HTTP.name());
    if (host != null) {
      http2Headers.authority(host);
    }
    return http2Headers;
  }

  /**
   * Handles the cleartext HTTP upgrade event. If an upgrade occurred, sends a simple response via HTTP/2
   * on stream 1 (the stream specifically reserved for cleartext HTTP upgrade).
   */
  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
    System.out.println("userEventTriggered :" + evt);

    if (evt instanceof HttpServerUpgradeHandler.UpgradeEvent) {
      HttpServerUpgradeHandler.UpgradeEvent upgradeEvent =
          (HttpServerUpgradeHandler.UpgradeEvent) evt;
      System.out.println(upgradeEvent.upgradeRequest());
      onHeadersRead(ctx, 1, http1HeadersToHttp2Headers(upgradeEvent.upgradeRequest()), 0 , true);
    }
    super.userEventTriggered(ctx, evt);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    super.exceptionCaught(ctx, cause);
    cause.printStackTrace();
    ctx.close();
  }

  /**
   * Sends a "Hello World" DATA frame to the client.
   */
  private void sendResponse(ChannelHandlerContext ctx, int streamId, ByteBuf payload) {
    System.out.println("sendResps");
    // Send a frame for the response status
    Http2Headers headers = new DefaultHttp2Headers().status(OK.codeAsText());
    encoder().writeHeaders(ctx, streamId, headers, 0, false, ctx.newPromise());
    encoder().writeData(ctx, streamId, payload, 0, true, ctx.newPromise());

    // no need to call flush as channelReadComplete(...) will take care of it.
  }

  @Override
  public int onDataRead(ChannelHandlerContext ctx, int streamId, ByteBuf data, int padding, boolean endOfStream) {
    System.out.println("dataRead");
    int processed = data.readableBytes() + padding;
    if (endOfStream) {
      sendResponse(ctx, streamId, data.retain());
    }
    return processed;
  }

  @Override
  public void onHeadersRead(ChannelHandlerContext ctx, int streamId,
      Http2Headers headers, int padding, boolean endOfStream) {
    System.out.println("onHeaderRead");
    if (endOfStream) {
      ByteBuf content = ctx.alloc().buffer();
      content.writeBytes(RESPONSE_BYTES.duplicate());
      ByteBufUtil.writeAscii(content, " - via HTTP/2");
      sendResponse(ctx, streamId, content);
    }
  }

  @Override
  public void onHeadersRead(ChannelHandlerContext ctx, int streamId, Http2Headers headers, int streamDependency,
      short weight, boolean exclusive, int padding, boolean endOfStream) {
    System.out.println("onHeaderRead2");
    onHeadersRead(ctx, streamId, headers, padding, endOfStream);
  }

  @Override
  public void onPriorityRead(ChannelHandlerContext ctx, int streamId, int streamDependency,
      short weight, boolean exclusive) {
  }

  @Override
  public void onRstStreamRead(ChannelHandlerContext ctx, int streamId, long errorCode) {
  }

  @Override
  public void onSettingsAckRead(ChannelHandlerContext ctx) {
  }

  @Override
  public void onSettingsRead(ChannelHandlerContext ctx, Http2Settings settings) {
  }

  @Override
  public void onPingRead(ChannelHandlerContext ctx, long data) {
  }

  @Override
  public void onPingAckRead(ChannelHandlerContext ctx, long data) {
  }

  @Override
  public void onPushPromiseRead(ChannelHandlerContext ctx, int streamId, int promisedStreamId,
      Http2Headers headers, int padding) {
  }

  @Override
  public void onGoAwayRead(ChannelHandlerContext ctx, int lastStreamId, long errorCode, ByteBuf debugData) {
  }

  @Override
  public void onWindowUpdateRead(ChannelHandlerContext ctx, int streamId, int windowSizeIncrement) {
  }

  @Override
  public void onUnknownFrame(ChannelHandlerContext ctx, byte frameType, int streamId,
      Http2Flags flags, ByteBuf payload) {
  }
}