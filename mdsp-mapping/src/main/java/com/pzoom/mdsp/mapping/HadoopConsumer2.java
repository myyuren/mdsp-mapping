package com.pzoom.mdsp.mapping;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pzoom.ha.agent.TextType;
import com.pzoom.utils.HAConfigBroker;

public class HadoopConsumer2 extends Configured implements Tool {
	private static Logger LOG = LoggerFactory.getLogger(HadoopConsumer2.class);
	private static String topic_path;
	private static String kafkaZkConnect;
	private static String kafkaTopic;
	private static String kafkaGroupid;
	private static String htable;
	private static String family;
	private static String qualifier;
	private static String hbaseZkQuorum;
	private static String hbaseZkMaster;
	
	public HadoopConsumer2() {
		super();

		Properties pro = HAConfigBroker.INSTANCE.getCachedProperties(
				"hdfs.properties", TextType.PROPERTIES);
		if (pro != null) {
			topic_path =  pro.getProperty("topic_path");
			kafkaZkConnect = pro.getProperty("kafka.zk.connect");
			kafkaTopic = pro.getProperty("kafka.topic");
			kafkaGroupid = pro.getProperty("kafka.groupid");
			htable = pro.getProperty("htable");
			family = pro.getProperty("family");
			qualifier = pro.getProperty("qualifier");
			hbaseZkQuorum = pro.getProperty("hbase.zookeeper.quorum");
			hbaseZkMaster = pro.getProperty("hbase.zookeeper.master");
		}
	}
	
	public static class KafkaMapper extends
			Mapper<LongWritable, BytesWritable, LongWritable, Text> {
		@Override
		public void map(LongWritable key, BytesWritable value, Context context)
				throws IOException {
			Text out = new Text();
			try {
				out.set(value.getBytes(), 0, value.getLength());
				context.write(key, out);
			} catch (InterruptedException e) {

			}
		}

	}

	// reduce将输入中的key复制到输出数据的key上，并直接输出

	public static class KafkaReducer extends
			Reducer<LongWritable, Text, LongWritable, Text> {
		private HTable table;
		// 实现reduce函数
		public void reduce(LongWritable key, Iterable<Text> values,

		Context context) throws IOException, InterruptedException {

			int sum = 0;
			Iterator<Text> iterator = values.iterator();

			while (iterator.hasNext()) {

				sum += Integer.parseInt(iterator.next().toString());

			}

			Text sumText = new Text();
			sumText.set(String.valueOf(sum));

			Put putrow = new Put(key.toString().getBytes());
			putrow.add(family.getBytes(), qualifier.getBytes(),
					sumText.getBytes());
			Configuration hbaseConf = HBaseConfiguration.create();
			hbaseConf.set("hbase.zookeeper.quorum", hbaseZkQuorum);
			hbaseConf.set("hbase.zookeeper.master", hbaseZkMaster);
			HBaseAdmin admin = new HBaseAdmin(hbaseConf);
			HTableDescriptor htableDescriptor = new HTableDescriptor(
					htable.getBytes());
			htableDescriptor.addFamily(new HColumnDescriptor(family));
			if (!admin.tableExists(htable)) {
				admin.createTable(htableDescriptor);
			}

			table = new HTable(hbaseConf, htable);
			table.put(putrow);
			LOG.info(table.toString() + putrow.toJSON());
			context.write(key, sumText);

		}

	}

	@Override
	public int run(String[] args) throws Exception {
		
		long startTime = System.currentTimeMillis();

		Configuration conf = new Configuration();

		conf.set("kafka.topic", kafkaTopic);

		conf.set("kafka.groupid", kafkaGroupid);

		conf.set("kafka.zk.connect", kafkaZkConnect);

		conf.setInt("kafka.limit", -1);
		conf.setBoolean("mapred.map.tasks.speculative.execution", false);

		Job job = new Job(conf, "Kafka.Consumer");
		job.setJarByClass(getClass());
		job.setMapperClass(KafkaMapper.class);
		// input
		job.setInputFormatClass(KafkaInputFormat.class);
		// output
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		job.setCombinerClass(KafkaReducer.class);
		job.setReducerClass(KafkaReducer.class);

		job.setOutputFormatClass(KafkaOutputFormat.class);

		// job.setNumReduceTasks(0);
		// 1,
		// KafkaOutputFormat.setOutputPath(job, new Path(cmd.getArgs()[0]));
		KafkaOutputFormat.setOutputPath(job, new Path(
				topic_path));
		boolean success = job.waitForCompletion(true);
		if (success) {
			commit(conf);
		}
		long endTime = System.currentTimeMillis();
		long interval = endTime-startTime;
		LOG.info("------------takes:------------"+interval+"seconds");
		return success ? 0 : -1;
	}

	private void commit(Configuration conf) throws IOException {
		ZkUtils zk = new ZkUtils(conf);
		try {
			String topic = conf.get("kafka.topic");
			String group = conf.get("kafka.groupid");
			zk.commit(group, topic);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			zk.close();
		}
	}

	public static void main(String[] args) throws Exception {
		
		File workaround = new File(".");
		System.getProperties().put("hadoop.home.dir",
				workaround.getAbsolutePath());
		new File("./bin").mkdirs();
		new File("./bin/winutils.exe").createNewFile();
		args = new String[] { String.valueOf(System.currentTimeMillis()) };
		int exitCode = ToolRunner.run(new HadoopConsumer2(), args);
		System.exit(exitCode);
	}

}
