package com.pzoom.mdsp.mapping;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaInputFormat extends InputFormat<LongWritable, BytesWritable> {
	private static Logger LOG = LoggerFactory.getLogger(KafkaInputFormat.class);

	@Override
	public RecordReader<LongWritable, BytesWritable> createRecordReader(
			InputSplit arg0, TaskAttemptContext arg1) throws IOException,
			InterruptedException {
		return new KafkaRecordReader();
	}

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException,
			InterruptedException {
		List<InputSplit> splits = new ArrayList<InputSplit>();
		Configuration conf = context.getConfiguration();
		String topic = conf.get("kafka.topic");
		String group = conf.get("kafka.groupid");
		ZkUtils zk = null;
		try {
			zk = new ZkUtils(conf);
			 Map<String,List<KafkaPartition>> topicMap = zk.getKafkaTopic(topic);
			if (topicMap == null) {
				LOG.error("getSplits method read kafkatopic failed");
				return null;
			}
			// 按分区进行分片
			for (KafkaPartition kafkaPartition : topicMap.get(topic)) {
				long lastCommit = zk.getLastCommit(group, topic,
						kafkaPartition.getId());
				InputSplit split = new KafkaSplit(topic, kafkaPartition,
						lastCommit);
				splits.add(split);
			}
		}catch (Exception e) {
			LOG.error("getSplits"+e);
		}finally {
			if (zk != null)
				zk.close();
		}
		return splits;
	}

}
