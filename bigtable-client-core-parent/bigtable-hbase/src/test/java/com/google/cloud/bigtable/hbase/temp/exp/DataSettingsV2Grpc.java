package com.google.cloud.bigtable.hbase.temp.exp;


import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.cloud.bigtable.grpc.BigtableInstanceName;
import com.google.cloud.bigtable.grpc.BigtableTableName;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import io.grpc.BindableService;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.MethodDescriptor;
import io.grpc.ServerServiceDefinition;
import io.grpc.ServiceDescriptor;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Iterator;

import static com.google.cloud.bigtable.hbase.TestBigtableOptionsFactory.TEST_INSTANCE_ID;
import static com.google.cloud.bigtable.hbase.TestBigtableOptionsFactory.TEST_PROJECT_ID;
import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ClientCalls.blockingServerStreamingCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;

public final class DataSettingsV2Grpc {

  final static BigtableTableName TABLE_NAME =
      new BigtableInstanceName(TEST_PROJECT_ID, TEST_INSTANCE_ID).toTableName("fakeTable");

  private DataSettingsV2Grpc() {}

  public static final String SERVICE_NAME = "com.google.bigtable.v2.models.Bigtable";

  public static final io.grpc.MethodDescriptor<Query, Row> METHOD_QUERY_ROWS =
      getReadRowsMethodHelper();

  private static volatile io.grpc.MethodDescriptor<Query, Row> getReadRowsMethod;
  private static final int METHODID_READ_ROWS = 0;
  private static volatile ServiceDescriptor serviceDescriptor;

  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static io.grpc.MethodDescriptor<Query, Row> getReadRowsMethod() {
    return getReadRowsMethodHelper();
  }

  private static io.grpc.MethodDescriptor<Query, Row> getReadRowsMethodHelper() {
    MethodDescriptor.Marshaller<Query> queryMarshaller = new MethodDescriptor.Marshaller<Query>() {
      @Override
      public InputStream stream(Query value) {
        Query query = Query.create(TABLE_NAME.getTableId());
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos;
        try{
          oos = new ObjectOutputStream(baos);
          oos.writeObject(query);
          oos.flush();
          oos.close();
        } catch(Exception e){
          throw new RuntimeException(e);
        }
        return new ByteArrayInputStream(baos.toByteArray());
      }

      @Override
      public Query parse(InputStream stream) {
        Query outQuery;
        try {
          ObjectInputStream ois = new ObjectInputStream(stream);
          outQuery = (Query) ois.readObject();
        }catch(Exception e){
          throw new RuntimeException(e);
        }
        return outQuery;
      }
    };

    MethodDescriptor.Marshaller<Row> rowMarshallar = new MethodDescriptor.Marshaller<Row>() {
      @Override
      public InputStream stream(Row value) {
        Row row =
            Row.create(
                ByteString.copyFromUtf8("key1"),
                ImmutableList.of(
                    RowCell.create(
                        "family",
                        ByteString.EMPTY,
                        1000,
                        ImmutableList.<String>of(),
                        ByteString.copyFromUtf8("value"))));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos;
        try{
          oos = new ObjectOutputStream(baos);
          oos.writeObject(row);
          oos.flush();
          oos.close();
        } catch(Exception e){
          throw new RuntimeException(e);
        }
        return new ByteArrayInputStream(baos.toByteArray());
      }

      @Override
      public Row parse(InputStream stream) {
        Row row;
        try {
          ObjectInputStream ois = new ObjectInputStream(stream);
          row = (Row) ois.readObject();
        }catch(Exception e){
          throw new RuntimeException(e);
        }
        return row;
      }
    };

    io.grpc.MethodDescriptor<Query, Row> getReadRowsMethod;
    if ((getReadRowsMethod = DataSettingsV2Grpc.getReadRowsMethod) == null) {
      synchronized (DataSettingsV2Grpc.class) {
        if ((getReadRowsMethod = DataSettingsV2Grpc.getReadRowsMethod) == null) {
          DataSettingsV2Grpc.getReadRowsMethod = getReadRowsMethod =
              io.grpc.MethodDescriptor.<Query, Row>newBuilder()
                  .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
                  .setFullMethodName(generateFullMethodName(
                      "com.google.bigtable.v2.models.Bigtable", "ReadRows"))
                  .setSampledToLocalTracing(true)
                  .setRequestMarshaller(queryMarshaller)
                  .setResponseMarshaller(rowMarshallar)
                  .setSchemaDescriptor(new DataSettingsV2Grpc.BigtableMethodDescriptorSupplier("ReadRows"))
                  .build();
        }
      }
    }
    return getReadRowsMethod;
  }

  private static abstract class BigtableBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    BigtableBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.google.bigtable.v2.BigtableProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("Bigtable");
    }
  }


  private static final class BigtableMethodDescriptorSupplier
      extends DataSettingsV2Grpc.BigtableBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    BigtableMethodDescriptorSupplier(String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  public static DataSettingsV2Grpc.BigtableStub newStub(Channel channel) {
    return new DataSettingsV2Grpc.BigtableStub(channel);
  }

  public static DataSettingsV2Grpc.BigtableBlockingStub newBlockingStub(Channel channel) {
    return new DataSettingsV2Grpc.BigtableBlockingStub(channel);
  }

  public static ServiceDescriptor getServiceDescriptor() {
    ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      Class var1 = DataSettingsV2Grpc.class;
      synchronized (DataSettingsV2Grpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = ServiceDescriptor.newBuilder("com.google.bigtable.v2.models.Bigtable")
              .setSchemaDescriptor(new DataSettingsV2Grpc.BigtableFileDescriptorSupplier())
              .addMethod(getReadRowsMethodHelper()).build();
        }
      }
    }

    return result;
  }

  private static final class BigtableFileDescriptorSupplier extends DataSettingsV2Grpc.BigtableBaseDescriptorSupplier {
    BigtableFileDescriptorSupplier() {
    }
  }



  public static final class BigtableStub extends AbstractStub<DataSettingsV2Grpc.BigtableStub> {
    private BigtableStub(Channel channel) {
      super(channel);
    }

    private BigtableStub(Channel channel, CallOptions callOptions) {
      super(channel, callOptions);
    }

    protected DataSettingsV2Grpc.BigtableStub build(Channel channel, CallOptions callOptions) {
      return new DataSettingsV2Grpc.BigtableStub(channel, callOptions);
    }

    public void readRows(Query request, StreamObserver<Row> responseObserver) {
      ClientCalls.asyncServerStreamingCall(this.getChannel().newCall(DataSettingsV2Grpc.getReadRowsMethodHelper(), this.getCallOptions()), request, responseObserver);
    }
  }

  public static final class BigtableBlockingStub extends AbstractStub<DataSettingsV2Grpc.BigtableBlockingStub> {
    private BigtableBlockingStub(Channel channel) {
      super(channel);
    }

    private BigtableBlockingStub(Channel channel, CallOptions callOptions) {
      super(channel, callOptions);
    }

    protected DataSettingsV2Grpc.BigtableBlockingStub build(Channel channel, CallOptions callOptions) {
      return new DataSettingsV2Grpc.BigtableBlockingStub(channel, callOptions);
    }

    public Iterator<Row> readRows(Query request) {
      return ClientCalls.blockingServerStreamingCall(this.getChannel(), DataSettingsV2Grpc.getReadRowsMethodHelper(),
          this.getCallOptions(), request);
    }
  }


  private static final class MethodHandlers<Req, Resp> implements
      ServerCalls.UnaryMethod<Req, Resp>, ServerCalls.ServerStreamingMethod<Req, Resp>,
      ServerCalls.ClientStreamingMethod<Req, Resp>, ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final DataSettingsV2Grpc.DataSettingsImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(DataSettingsV2Grpc.DataSettingsImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    public void invoke(Req request, StreamObserver<Resp> responseObserver) {
      switch(this.methodId) {
      case 0:
        this.serviceImpl.readRows((Query)request, (StreamObserver<Row>) responseObserver);
        break;
      default:
        throw new AssertionError();
      }
    }

    public StreamObserver<Req> invoke(StreamObserver<Resp> responseObserver) {
      switch(this.methodId) {
      default:
        throw new AssertionError();
      }
    }
  }

  public abstract static class DataSettingsImplBase implements BindableService {
    public DataSettingsImplBase() {
    }

    public void readRows(Query request, StreamObserver<Row> responseObserver) {
      ServerCalls
          .asyncUnimplementedUnaryCall(DataSettingsV2Grpc.getReadRowsMethodHelper(), responseObserver);
    }

    public final ServerServiceDefinition bindService() {
      return ServerServiceDefinition.builder(DataSettingsV2Grpc.getServiceDescriptor())
          .addMethod(DataSettingsV2Grpc
              .getReadRowsMethodHelper(), ServerCalls.asyncServerStreamingCall(new DataSettingsV2Grpc.MethodHandlers(this, 0)))
         .build();
    }
  }
}