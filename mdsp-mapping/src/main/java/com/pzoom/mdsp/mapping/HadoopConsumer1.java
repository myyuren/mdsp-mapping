package com.pzoom.mdsp.mapping;

import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
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

/**
 * HA 获取数据库里面存放的配置参数
 * @author chenbaoyu
 *
 */
public class HadoopConsumer1 extends Configured implements Tool {
	

	private static Logger LOG = LoggerFactory.getLogger(HadoopConsumer1.class);

	private static String topic_path;
	private static String kafkaZkConnect;
	private static String kafkaTopic;
	private static String kafkaGroupid;
	private static String htable;
	private static String family;
	private static String qualifier;
	private static String hbaseZkQuorum;
	private static String hbaseZkMaster;

	public HadoopConsumer1() {
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

	public static class ReducerClass extends
			Reducer<LongWritable, Text, LongWritable, Text> {
		private HTable table;

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
			// context.write(key, sumText);

		}
	}

	@Override
	public int run(String[] args) throws Exception {

		CommandLineParser parser = new PosixParser();
		Options options = buildOptions();

		CommandLine cmd = parser.parse(options, args);

		Configuration conf = new Configuration();

		conf.set("kafka.topic", cmd.getOptionValue("topic", kafkaTopic));

		conf.set("kafka.groupid",
				cmd.getOptionValue("k8test_group", kafkaGroupid));

		conf.set("kafka.zk.connect",
				cmd.getOptionValue("zk-connect", kafkaZkConnect));
		Job job = new Job(conf, "Kafka.Consumer");
		job.setJarByClass(getClass());
		job.setMapperClass(KafkaMapper.class);
		// input
		job.setInputFormatClass(KafkaInputFormat.class);
		// output
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		job.setCombinerClass(ReducerClass.class);
		job.setReducerClass(ReducerClass.class);

		job.setOutputFormatClass(KafkaOutputFormat.class);

		KafkaOutputFormat.setOutputPath(job, new Path(
				topic_path));
		boolean success = job.waitForCompletion(true);
		if (success) {
			commit(conf);
		}
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

	/**
	 * 打包成JAR在hadoop 环境下面运行需要初始化的内容（maybe）
	 * 
	 * @return
	 */
	@SuppressWarnings({ "unused", "static-access" })
	private Options buildOptions() {
		Options options = new Options();

		OptionBuilder.withArgName("topic").withLongOpt("topic");
		options.addOption(OptionBuilder.hasArg().withDescription("kafka topic")
				.create("t"));
		options.addOption(OptionBuilder.withArgName("groupid")
				.withLongOpt("consumer-group").hasArg()
				.withDescription("kafka consumer groupid").create("g"));
		options.addOption(OptionBuilder.withArgName("zk")
				.withLongOpt("zk-connect").hasArg()
				.withDescription("ZooKeeper connection String").create("z"));

		options.addOption(OptionBuilder.withArgName("offset")
				.withLongOpt("autooffset-reset").hasArg()
				.withDescription("Offset reset").create("o"));

		options.addOption(OptionBuilder.withArgName("limit")
				.withLongOpt("limit").hasArg().withDescription("kafka limit")
				.create("l"));

		return options;
	}

	public static void main(String[] args) throws Exception {

		args = new String[] {String.valueOf(System.currentTimeMillis()) };
		int count = 0;
		while (true) {
			int exitCode = ToolRunner.run(new HadoopConsumer1(), args);
			++count;
			LOG.info(exitCode + "*********************" + count);
			try {
				Thread.sleep(1000 * 60 * 1);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

	}

}
