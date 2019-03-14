package com.google.cloud.bigtable.hbase;

import com.google.cloud.bigtable.hbase.test_env.SharedTestEnvRule;
import java.util.concurrent.TimeUnit;
import org.junit.ClassRule;
import org.junit.rules.Timeout;

public class TestSingleColumnValueFilterH1 extends TestSingleColumnValueFilter {

  @ClassRule
  public static Timeout timeoutRule = new Timeout(8, TimeUnit.MINUTES);

  @ClassRule
  public static SharedTestEnvRule sharedTestEnvRule = SharedTestEnvRule.getInstance();

}
