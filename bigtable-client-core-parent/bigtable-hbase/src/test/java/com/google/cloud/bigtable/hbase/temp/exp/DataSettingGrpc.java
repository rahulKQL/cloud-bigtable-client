package com.google.cloud.bigtable.hbase.temp.exp;


import com.google.bigtable.v2.BigtableGrpc;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.grpc.BigtableInstanceName;
import com.google.cloud.bigtable.grpc.BigtableTableName;
import com.google.protobuf.ByteString;
import io.grpc.MethodDescriptor;
import io.grpc.stub.ClientCalls;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static com.google.cloud.bigtable.hbase.TestBigtableOptionsFactory.TEST_INSTANCE_ID;
import static com.google.cloud.bigtable.hbase.TestBigtableOptionsFactory.TEST_PROJECT_ID;
import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ClientCalls.blockingServerStreamingCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.stub.ServerCalls.asyncServerStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;

public final class DataSettingGrpc {

  final static BigtableTableName TABLE_NAME =
      new BigtableInstanceName(TEST_PROJECT_ID, TEST_INSTANCE_ID).toTableName("fakeTable");

  private DataSettingGrpc() {}

  public static final String SERVICE_NAME = "com.google.bigtable.v2.models.Bigtable";

  public static final io.grpc.MethodDescriptor<Query, Row> METHOD_QUERY_ROWS =
      getReadRowsMethodHelper();

  private static volatile io.grpc.MethodDescriptor<Query, Row> getReadRowsMethod;

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
        return null;
      }

      @Override
      public Row parse(InputStream stream) {
        return null;
      }
    };

    io.grpc.MethodDescriptor<Query, Row> getReadRowsMethod;
    if ((getReadRowsMethod = DataSettingGrpc.getReadRowsMethod) == null) {
      synchronized (DataSettingGrpc.class) {
        if ((getReadRowsMethod = DataSettingGrpc.getReadRowsMethod) == null) {
          DataSettingGrpc.getReadRowsMethod = getReadRowsMethod =
              io.grpc.MethodDescriptor.<Query, Row>newBuilder()
                  .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
                  .setFullMethodName(generateFullMethodName(
                      "google.bigtable.v2.Bigtable", "ReadRows"))
                  .setSampledToLocalTracing(true)
                  .setRequestMarshaller(queryMarshaller)
                  .setResponseMarshaller(rowMarshallar)
                  .setSchemaDescriptor(new DataSettingGrpc.BigtableMethodDescriptorSupplier("ReadRows"))
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
      extends DataSettingGrpc.BigtableBaseDescriptorSupplier
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
}