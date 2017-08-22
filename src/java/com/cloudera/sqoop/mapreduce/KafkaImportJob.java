/**
 * kafka related --spp
 */

package com.cloudera.sqoop.mapreduce;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.manager.ImportJobContext;

/**
 * @deprecated Moving to use org.apache.sqoop namespace.
 */
public class KafkaImportJob
    extends org.apache.sqoop.mapreduce.KafkaImportJob {

  public static final Log LOG = LogFactory.getLog(
		  KafkaImportJob.class.getName());

  public KafkaImportJob(final SqoopOptions opts,
      final ImportJobContext importContext) {
    super(opts, importContext);
  }

}

