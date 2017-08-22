
/**
 * kafka related --spp
 */

package org.apache.sqoop.mapreduce;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import com.cloudera.sqoop.lib.SqoopRecord;
import com.cloudera.sqoop.mapreduce.AutoProgressMapper;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;


public class KafkaImportMapper extends AutoProgressMapper<LongWritable, SqoopRecord, SqoopRecord, NullWritable> {
   
	public KafkaImportMapper() {
	}

	@Override
	public void map(LongWritable key, SqoopRecord val, Context context) throws IOException, InterruptedException {
		context.write(val, NullWritable.get());
	}
}
