package com.pzoom.test;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

/**
 * <pre>
 * Created by zhaoming on 14-5-4 下午3:23
 * </pre>
 */
public class KafkaProductor {

	public static void main(String[] args) throws InterruptedException {

		Properties properties = new Properties();
		properties.put("zk.connect", "10.100.10.183:2181");
		properties.put("metadata.broker.list", "10.100.10.186:9092");

		properties.put("serializer.class", "kafka.serializer.StringEncoder");

		ProducerConfig producerConfig = new ProducerConfig(properties);
		Producer<String, String> producer = new Producer<String, String>(
				producerConfig);
		
		StringBuilder sb = new StringBuilder();
		for(int i=0;i<100000;i++)
		{
			sb.append("2014-09-12 16:11:11,733 [pool-5-thread-3] [com.pzoom.mdsp.logfilling.PartitionManager] [INFO] - success write data to hdfs216");
		}
		// 构建消息体
		KeyedMessage<String, String> keyedMessage = new KeyedMessage<String, String>(
				"Settlement_0", sb.toString());
		producer.send(keyedMessage);

		Thread.sleep(1000);

		producer.close();
	}

}
