package org.avaje.metrics.collectd;


import org.avaje.metric.MetricManager;
import org.avaje.metric.report.MetricReportConfig;
import org.avaje.metric.report.MetricReportManager;
import org.junit.Test;

public class CollectdReporterTest {

//    @ClassRule
//    public static Receiver receiver = new Receiver(25826);

  private CollectdReporter reporter;

//    @Before
//    public void setUp() {
//        reporter = CollectdReporter.forRegistry(registry)
//                .withHostName("eddie")
//                .build(new Sender("localhost", 25826));
//    }

  @Test
  public void reportsByteGauges() throws Exception {

    reporter = CollectdReporter.create()
      .withHost("foo5bar")
      .withCollectdHost("localhost")
      .withCollectdPort(25826)
      .withSecurityLevel(SecurityLevel.ENCRYPT)
      .withUsername("user0")
      .withPassword("foo")
      .build();

    MetricManager.jvmMetrics().withReportAlways().registerStandardJvmMetrics();

    MetricReportConfig config = new MetricReportConfig();
    config.setFreqInSeconds(10);
    config.setReporter(reporter);

    MetricReportManager manager = new MetricReportManager(config);


    for (int i = 0; i < 15; i++) {
      Thread.sleep(3000);
    }

    System.out.println("done");
    manager.shutdown();
  }


}


