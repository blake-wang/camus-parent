package com.linkedin.camus.etl.kafka.partitioner;

import com.linkedin.camus.etl.IEtlKey;
import com.linkedin.camus.etl.kafka.common.DateUtils;
import com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;

public class MonthPartitioner extends DailyPartitioner {
    protected static final String OUTPUT_DATE_FORMAT = "YYYY-MM";
    protected DateTimeFormatter outputDateFormatter = null;

    @Override
    public String encodePartition(JobContext context, IEtlKey key) {
        return DateTime.parse(new DateTime(key.getTime()).toString(outputDateFormatter), outputDateFormatter).getMillis()+"";
    }

    @Override
    public void setConf(Configuration conf) {
        if (conf != null) {
            outputDateFormatter =
                    DateUtils.getDateTimeFormatter(OUTPUT_DATE_FORMAT,
                            DateTimeZone.forID(conf.get(EtlMultiOutputFormat.ETL_DEFAULT_TIMEZONE, "+08:00")));
        }

        super.setConf(conf);
    }
}
