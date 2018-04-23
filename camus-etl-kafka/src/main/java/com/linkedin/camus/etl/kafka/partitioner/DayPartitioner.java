package com.linkedin.camus.etl.kafka.partitioner;

import com.linkedin.camus.etl.IEtlKey;
import com.linkedin.camus.etl.Partitioner;
import com.linkedin.camus.etl.kafka.common.DateUtils;
import com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;

import static com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat.ETL_OUTPUT_FILE_TIME_PARTITION_MINS;

/**
 * Partitions incoming data into hourly partitions, generates pathnames of the form:
 * {@code etl.destination.path/topic-name/hourly/YYYY/MM/dd/HH}.
 *
 * The following configurations are supported:
 * <ul>
 *     <li>{@code etl.destination.path} - top-level data output directory, required</li>
 *     <li>{@code etl.destination.path.topic.sub.dir} - sub-dir to create under topic dir, defaults to {@code hourly}</li>
 *     <li>{@code etl.default.timezone} - timezone of the events, defaults to {@code America/Los_Angeles}</li>
 *     <li>{@code etl.output.file.time.partition.mins} - partitions size in minutes, defaults to {@code 60}</li>
 * </ul>
 */
public class DayPartitioner extends Partitioner {

  protected static final String OUTPUT_DATE_FORMAT = "YYYY-MM-dd";
  //protected DateTimeZone outputDateTimeZone = null;
  protected DateTimeFormatter outputDateFormatter = null;

  private long outfilePartitionMs;

  @Override
  public String encodePartition(JobContext context, IEtlKey key) {
    return "" + DateUtils.getPartition(outfilePartitionMs, key.getTime(), outputDateFormatter.getZone());
  }

  @Override
  public String generatePartitionedPath(JobContext context, String topic, String encodedPartition) {
    StringBuilder sb = new StringBuilder();
    sb.append(topic).append("/");
    sb.append(EtlMultiOutputFormat.getDestPathTopicSubDir(context)).append("/");
    DateTime bucket = new DateTime(Long.valueOf(encodedPartition));
    sb.append(bucket.toString(outputDateFormatter));

    return sb.toString();
  }

  @Override
  public String generateFileName(JobContext context, String topic, String brokerId, int partitionId, int count,
      long offset, String encodedPartition) {
    StringBuilder sb = new StringBuilder();
    sb.append(topic);
    sb.append(".").append(brokerId);
    sb.append(".").append(partitionId);
    sb.append(".").append(count);
    sb.append(".").append(offset);
    sb.append(".").append(encodedPartition);

    return sb.toString();
  }

  @Override
  public String getWorkingFileName(JobContext context, String topic, String brokerId, int partitionId,
      String encodedPartition) {
    StringBuilder sb = new StringBuilder();
    sb.append("data.").append(topic.replaceAll("\\.", "_"));
    sb.append(".").append(brokerId);
    sb.append(".").append(partitionId);
    sb.append(".").append(encodedPartition);

    return sb.toString();
  }

  @Override
  public void setConf(Configuration conf) {
    if (conf != null) {
      outputDateFormatter =
          DateUtils.getDateTimeFormatter(OUTPUT_DATE_FORMAT,
              DateTimeZone.forID(conf.get(EtlMultiOutputFormat.ETL_DEFAULT_TIMEZONE, "+08:00")));
    }
    outfilePartitionMs = conf.getInt(ETL_OUTPUT_FILE_TIME_PARTITION_MINS, 60) * 60000L;

    super.setConf(conf);
  }
}
