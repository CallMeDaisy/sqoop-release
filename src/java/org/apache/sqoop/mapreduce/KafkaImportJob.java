/**
 * kafka related --spp
 */

package org.apache.sqoop.mapreduce;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.sqoop.kafka.KafkaPutProcessor;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.hbase.HBasePutProcessor;
import com.cloudera.sqoop.lib.FieldMapProcessor;
import com.cloudera.sqoop.lib.SqoopRecord;
import com.cloudera.sqoop.manager.ConnManager;
import com.cloudera.sqoop.manager.ImportJobContext;
import com.cloudera.sqoop.mapreduce.DataDrivenImportJob;
import com.cloudera.sqoop.util.ImportException;

/**
 * Runs an Kafka import via DataDrivenDBInputFormat to the KafkaPutProcessor in
 * the DelegatingOutputFormat.
 */
public class KafkaImportJob extends DataDrivenImportJob {

	public static final Log LOG = LogFactory.getLog(HBaseImportJob.class.getName());

	public KafkaImportJob(final SqoopOptions opts, final ImportJobContext importContext) {

		super(opts, importContext.getInputFormat(), importContext);
	}

	@Override
	protected void configureMapper(Job job, String tableName, String tableClassName) throws IOException {
		job.setOutputKeyClass(SqoopRecord.class);
		job.setOutputValueClass(NullWritable.class);
		job.setMapperClass(getMapperClass());
	}

	@Override
	protected Class<? extends Mapper> getMapperClass() {
		return KafkaImportMapper.class;
	}

	@Override
	protected Class<? extends OutputFormat> getOutputFormatClass() throws ClassNotFoundException {
		// return RawKeyTextOutputFormat.class;
		return DelegatingOutputFormat.class;
	}

	@Override
	protected void configureOutputFormat(Job job, String tableName, String tableClassName)
			throws ClassNotFoundException, IOException {
		job.setOutputFormatClass(getOutputFormatClass());
		Configuration conf = job.getConfiguration();
		conf.setClass("sqoop.output.delegate.field.map.processor.class", KafkaPutProcessor.class,
				FieldMapProcessor.class);
	}

}
