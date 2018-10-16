package org.avaje.metrics.collectd;

import org.avaje.metric.report.MetricReporter;
import org.avaje.metric.report.ReportMetrics;
import org.avaje.metric.statistics.CounterStatistics;
import org.avaje.metric.statistics.GaugeDoubleStatistics;
import org.avaje.metric.statistics.GaugeLongStatistics;
import org.avaje.metric.statistics.MetricStatistics;
import org.avaje.metric.statistics.MetricStatisticsVisitor;
import org.avaje.metric.statistics.TimedStatistics;
import org.avaje.metric.statistics.ValueStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.time.Clock;
import java.util.List;

/**
 * A reporter which publishes the metrics to a Collectd server.
 *
 * <pre>@{code
 *
 *   CollectdReporter reporter = CollectdReporter.create()
 *       .withHost("hostContainerName")
 *       .withCollectdHost("localhost")
 *       .withCollectdPort(25826)
 *       .withSecurityLevel(SecurityLevel.ENCRYPT)
 *       .withUsername("user")
 *       .withPassword("secret")
 *       .build();
 *
 *
 *   MetricReportConfig config = new MetricReportConfig();
 *   config.setFreqInSeconds(60);
 *   config.setReporter(reporter);
 *
 *   MetricReportManager manager = new MetricReportManager(config);
 *
 * }</pre>
 */
public class CollectdReporter implements MetricReporter {

  public static Builder create() {
    return new Builder();
  }

  public static class Builder {

    private String collectdHost;

    private int collectdPort = 25826;

    private String sourceHost;

    private SecurityLevel securityLevel = SecurityLevel.NONE;

    private String username = "";

    private String password = "";

    private Clock clock = Clock.systemDefaultZone();

    private Builder() {
    }

    /**
     * Set the Collectd hostname to send the metrics to.
     */
    public Builder withCollectdHost(String host) {
      this.collectdHost = host;
      return this;
    }

    /**
     * Set the Collectd port to send the metrics to. Defaults to 25826.
     */
    public Builder withCollectdPort(int port) {
      this.collectdPort = port;
      return this;
    }

    /**
     * Set the host of the source metrics (the container host name).
     */
    public Builder withHost(String hostName) {
      this.sourceHost = hostName;
      return this;
    }

    /**
     * Set the clock to use - defaults to the system clock.
     */
    public Builder withClock(Clock clock) {
      this.clock = clock;
      return this;
    }

    /**
     * Set username for authentication to Collectd for Sign or Encrypt.
     */
    public Builder withUsername(String username) {
      this.username = username;
      return this;
    }

    /**
     * Set password for authentication to Collectd for Sign or Encrypt.
     */
    public Builder withPassword(String password) {
      this.password = password;
      return this;
    }

    /**
     * Set the security level for authentication to Collectd.
     */
    public Builder withSecurityLevel(SecurityLevel securityLevel) {
      this.securityLevel = securityLevel;
      return this;
    }

    /**
     * Build and return a CollectdReporter.
     */
    public CollectdReporter build() {
      if (securityLevel != SecurityLevel.NONE) {
        if (username.isEmpty()) {
          throw new IllegalArgumentException("username is required for securityLevel: " + securityLevel);
        }
        if (password.isEmpty()) {
          throw new IllegalArgumentException("password is required for securityLevel: " + securityLevel);
        }
      }
      Sender sender = new Sender(collectdHost, collectdPort);
      return new CollectdReporter(sourceHost, sender, username, password, securityLevel, clock);
    }
  }

  private static final Logger log = LoggerFactory.getLogger(CollectdReporter.class);

  private static final String FALLBACK_HOST_NAME = "localhost";

  private final String hostName;

  private final Sender sender;

  private final PacketWriter writer;

  private final Clock clock;

  private CollectdReporter(String hostname, Sender sender, String username, String password,
                           SecurityLevel securityLevel, Clock clock) {
    this.clock = clock;
    this.sender = sender;
    this.hostName = (hostname != null) ? hostname : resolveHostName();
    this.writer = new PacketWriter(sender, username, password, securityLevel);
  }

  private String resolveHostName() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (Exception e) {
      log.error("Failed to lookup local host name: {}", e.getMessage(), e);
      return FALLBACK_HOST_NAME;
    }
  }

  @Override
  public void cleanup() {
    // do nothing
  }

  @Override
  public void report(ReportMetrics reportMetrics) {


    log.debug("reporting metrics ...");
    long epochSecs = clock.millis() / 1000;
    MetaData metaData = new MetaData(hostName, epochSecs, reportMetrics.getFreqInSeconds());
    try {
      connect(sender);

      List<MetricStatistics> metrics = reportMetrics.getMetrics();

      Adapter adapter = new Adapter(this, metaData);
      for (MetricStatistics metric : metrics) {
        metric.visit(adapter);
      }

    } catch (Exception e) {
      log.warn("Error trying to send metrics to Collectd", e);

    } finally {
      disconnect(sender);
    }
  }

  private static class Adapter implements MetricStatisticsVisitor {

    private final CollectdReporter reporter;
    private final MetaData metaData;

    Adapter(CollectdReporter reporter, MetaData metaData) {
      this.reporter = reporter;
      this.metaData = metaData;
    }

    @Override
    public void visit(TimedStatistics metric) {
      reporter.reportTimed(metaData, metric);
    }

    @Override
    public void visit(ValueStatistics metric) {
      reporter.reportValues(metaData, metric);
    }

    @Override
    public void visit(CounterStatistics metric) {
      reporter.reportCounter(metaData, metric);
    }

    @Override
    public void visit(GaugeDoubleStatistics metric) {
      reporter.reportGauge(metaData, metric);
    }

    @Override
    public void visit(GaugeLongStatistics metric) {
      reporter.reportGauge(metaData, metric);
    }
  }

  private void reportGauge(MetaData metaData, GaugeLongStatistics metric) {
    metaData.plugin(metric.getName());
    write(metaData.typeInstance("value"), metric.getValue());
  }

  private void reportGauge(MetaData metaData, GaugeDoubleStatistics metric) {
    metaData.plugin(metric.getName());
    write(metaData.typeInstance("value"), metric.getValue());
  }

  private void reportCounter(MetaData metaData, CounterStatistics metric) {
    metaData.plugin(metric.getName());
    write(metaData.typeInstance("count"), metric.getCount());
  }

  private void reportTimed(MetaData metaData, TimedStatistics metric) {
    reportValues(metaData, metric);
  }

  private void reportValues(MetaData metaData, ValueStatistics metric) {

    metaData.plugin(metric.getName());
    write(metaData.typeInstance("count"), metric.getCount());
    write(metaData.typeInstance("max"), metric.getMax());
    write(metaData.typeInstance("mean"), metric.getMean());
    write(metaData.typeInstance("total"), metric.getTotal());
  }

  private void connect(Sender sender) throws IOException {
    if (!sender.isConnected()) {
      sender.connect();
    }
  }

  private void disconnect(Sender sender) {
    try {
      sender.disconnect();
    } catch (Exception e) {
      log.warn("Error disconnecting from Collectd", e);
    }
  }

  private void write(MetaData metaData, Number... values) {
    try {
      writer.write(metaData, values);
    } catch (RuntimeException e) {
      log.warn("Failed to process metric '" + metaData.getPlugin() + "': " + e.getMessage());
    } catch (IOException e) {
      log.error("Failed to send metric to collectd", e);
    }
  }

}
