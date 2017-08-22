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
//		指定kafka节点：注意这里无需指定集群中所有Boker，只要指定其中部分即可，它会自动取meta信息并连接到对应的Boker节点
		props.put("bootstrap.servers", conf.get("broker-list"));
//	    // 该属性表示你需要在消息被接收到的时候发送ack给发送者。以保证数据不丢失
//		props.put("acks", "all");
//		props.put("retries", 0);
//		props.put("batch.size", 16384);
//		props.put("linger.ms", 1);
//		props.put("buffer.memory", 33554432);
//	    // 指定采用哪种序列化方式将消息传输给Boker,你也可以在发送消息的时候指定序列化类型，不指定则以此为默认序列化类型
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
