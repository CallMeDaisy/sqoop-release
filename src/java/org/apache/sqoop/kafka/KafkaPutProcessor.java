package org.apache.sqoop.kafka;
/**
 * @author shangpingping  
 * spp -- kafka-related
 */

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.cloudera.sqoop.lib.FieldMapProcessor;


public class KafkaPutProcessor implements Closeable, Configurable,FieldMapProcessor {

	public static final Log LOG = LogFactory.getLog(KafkaPutProcessor.class.getName());

	private Configuration conf;
	private Producer<String, String> kafkaProducer;
	private String topic;
	private String tableName;

	public KafkaPutProcessor() {
	}

	@Override
	public void setConf(Configuration config) {
		this.conf = config;
		topic = conf.get("topic");
		tableName = conf.get("table-name");
		
		Properties props = new Properties();
//		ָ��kafka�ڵ㣺ע����������ָ����Ⱥ������Boker��ֻҪָ�����в��ּ��ɣ������Զ�ȡmeta��Ϣ�����ӵ���Ӧ��Boker�ڵ�
		props.put("bootstrap.servers", conf.get("broker-list"));
//	    // �����Ա�ʾ����Ҫ����Ϣ�����յ���ʱ����ack�������ߡ��Ա�֤���ݲ���ʧ
//		props.put("acks", "all");
//		props.put("retries", 0);
//		props.put("batch.size", 16384);
//		props.put("linger.ms", 1);
//		props.put("buffer.memory", 33554432);
//	    // ָ�������������л���ʽ����Ϣ�����Boker,��Ҳ�����ڷ�����Ϣ��ʱ��ָ�����л����ͣ���ָ�����Դ�ΪĬ�����л�����
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		kafkaProducer = new KafkaProducer<String, String>(props);
	}

	@Override
	public Configuration getConf() {
		return this.conf;
	}

	/**
	 * Closes the HBase table and commits all pending operations.
	 */
	public void close() throws IOException {
		if (kafkaProducer != null) {
			kafkaProducer.close();
		}
	}

	@Override
	/**
	 * Processes a record by extracting its field map and converting it into a
	 * list of Put commands into kafka.
	 */
	public void accept(com.cloudera.sqoop.lib.FieldMappable record)
			throws IOException, com.cloudera.sqoop.lib.ProcessingException {
		// TODO Auto-generated method stub
		Map<String, Object> details = record.getFieldMap();
		Iterator iter = details.entrySet().iterator();
		boolean firstEle = true;
		String msg_detail = "{\"tableName\":\"" + tableName + "\",\"rowContent\":{";
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			String feature = (String) entry.getKey();

			String featureValue = "";
			if (entry.getValue() != null) {
				featureValue = entry.getValue().toString();
			}
			if (!firstEle) {
				msg_detail += ",";
			}
			msg_detail += "\"" + feature + "\":\"" + featureValue + "\"";
			firstEle = false;
		}
		msg_detail += "},\"timestamp\":\"" + String.valueOf(System.currentTimeMillis() / 1000) + "\"}";
		ProducerRecord<String, String> data = new ProducerRecord(topic, null, msg_detail);
		kafkaProducer.send(data);
		
	}
	

}
