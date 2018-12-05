package com.google.cloud.bigtable.hbase.temp;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class UserAgentHandler extends ChannelInboundHandlerAdapter {

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) {
    ByteBuf in = (ByteBuf) msg;
    try {
      while (in.isReadable()) { // (1)
        System.out.print((char) in.readByte());
        System.out.println(ctx);
        System.out.flush();
      }
    } finally {
      in.release(); // (2)
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
      throws Exception {
    // Close the connection when an exception is raised.
    cause.printStackTrace();
    ctx.close();
  }
}