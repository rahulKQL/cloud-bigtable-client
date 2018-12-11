package com.google.cloud.bigtable.hbase.temp;

import com.sun.deploy.util.SessionState;
import io.grpc.netty.shaded.io.netty.channel.ChannelHandler;
import io.grpc.netty.shaded.io.netty.channel.ChannelHandlerContext;
import io.grpc.netty.shaded.io.netty.channel.SimpleChannelInboundHandler;
import io.grpc.netty.shaded.io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.grpc.netty.shaded.io.netty.handler.codec.http.FullHttpRequest;
import io.grpc.netty.shaded.io.netty.handler.codec.http.FullHttpResponse;
import io.grpc.netty.shaded.io.netty.handler.codec.http.HttpRequest;
import io.grpc.netty.shaded.io.netty.handler.codec.http.HttpResponseStatus;
import io.grpc.netty.shaded.io.netty.handler.codec.http.QueryStringDecoder;
import io.grpc.netty.shaded.io.netty.util.CharsetUtil;
import io.netty.buffer.Unpooled;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This handler is the main entry point for SockJS HTTP Request.
 *
 * It is responsible for inspecting the request uri and adding ChannelHandlers for
 * different transport protocols that SockJS support. Once this has been done this
 * handler will be removed from the channel pipeline.
 */
public class SockJsHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

  private final Map<String, SockJsServiceFactory> factories = new LinkedHashMap<String, SockJsServiceFactory>();

  private static final PathParams NON_SUPPORTED_PATH = new NonSupportedPath();
  private static final Pattern SERVER_SESSION_PATTERN = Pattern.compile("^/([^/.]+)/([^/.]+)/([^/.]+)");

  /**
   * Sole constructor which takes one or more {@code SockJSServiceFactory}. These factories will
   * later be used by the server to create the SockJS services that will be exposed by this server
   *
   * @param factories one or more {@link SockJsServiceFactory}s.
   */
  public SockJsHandler(final SockJsServiceFactory... factories) {
    for (SockJsServiceFactory factory : factories) {
      this.factories.put(factory.config().prefix(), factory);
    }
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx,
      FullHttpRequest fullHttpRequest) throws Exception {
    final String path = new QueryStringDecoder(fullHttpRequest.getUri()).path();
    for (SockJsServiceFactory factory : factories.values()) {
      if (path.startsWith(factory.config().prefix())) {
        handleService(factory, fullHttpRequest, ctx);
        return;
      }
    }
    System.out.println("Write message not found line 58");
  }

  public void messageReceived(final ChannelHandlerContext ctx, final FullHttpRequest request) throws Exception {
    final String path = new QueryStringDecoder(request.getUri()).path();
    for (SockJsServiceFactory factory : factories.values()) {
      if (path.startsWith(factory.config().prefix())) {
        handleService(factory, request, ctx);
        return;
      }
    }
    System.out.println("Write message not found line 70");
  }

  private static void handleService(final SockJsServiceFactory factory,
      final FullHttpRequest request,
      final ChannelHandlerContext ctx) throws Exception {
    final String pathWithoutPrefix = request.getUri().replaceFirst(factory.config().prefix(), "");
    System.out.println("hanldeService");
  }

  private static void handleSession(final SockJsServiceFactory factory,
      final FullHttpRequest request,
      final ChannelHandlerContext ctx,
      final PathParams pathParams) throws Exception {

    System.out.println("handleSession");
    System.out.println(pathParams);
    System.out.println(request);
    ctx.fireChannelRead(request.retain());
  }


  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    if (cause instanceof SessionNotFoundException) {
      final SessionNotFoundException se = (SessionNotFoundException) cause;
      System.out.println("Could not find session [{}]" + se.sessionId());

    } else {
      System.out.println("exception caught:" + cause);
      ctx.fireExceptionCaught(cause);
    }
  }

  static PathParams matches(final String path) {
    final Matcher matcher = SERVER_SESSION_PATTERN.matcher(path);
    if (matcher.find()) {
      final String serverId = matcher.group(1);
      final String sessionId = matcher.group(2);
      final String transport = matcher.group(3);
      return new MatchingSessionPath(serverId, sessionId, transport);
    } else {
      return NON_SUPPORTED_PATH;
    }
  }

  private static final class SessionNotFoundException extends Exception {
    private static final long serialVersionUID = 1101611486620901143L;
    private final String sessionId;
    private final HttpRequest request;

    private SessionNotFoundException(final String sessionId, final HttpRequest request) {
      this.sessionId = sessionId;
      this.request = request;
    }

    public String sessionId() {
      return sessionId;
    }

    public HttpRequest httpRequest() {
      return request;
    }
  }

  /**
   * Represents HTTP path parameters in SockJS.
   *
   * The path consists of the following parts:
   * http://server:port/prefix/serverId/sessionId/transport
   *
   */
  public interface PathParams {
    boolean matches();

    /**
     * The serverId is chosen by the client and exists to make it easier to configure
     * load balancers to enable sticky sessions.
     *
     * @return String the server id for this path.
     */
    String serverId();

    /**
     * The sessionId is a unique random number which identifies the session.
     *
     * @return String the session identifier for this path.
     */
    String sessionId();


  }

  public static class MatchingSessionPath implements PathParams {
    private final String serverId;
    private final String sessionId;

    public MatchingSessionPath(final String serverId, final String sessionId, final String transport) {
      this.serverId = serverId;
      this.sessionId = sessionId;
    }

    @Override
    public boolean matches() {
      return true;
    }

    @Override
    public String serverId() {
      return serverId;
    }

    @Override
    public String sessionId() {
      return sessionId;
    }

  }

  public static class NonSupportedPath implements PathParams {

    @Override
    public boolean matches() {
      return false;
    }

    @Override
    public String serverId() {
      throw new UnsupportedOperationException("serverId is not available in path");
    }

    @Override
    public String sessionId() {
      throw new UnsupportedOperationException("sessionId is not available in path");
    }

  }

}