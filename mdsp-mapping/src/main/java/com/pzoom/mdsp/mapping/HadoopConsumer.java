package com.pzoom.mdsp.mapping;

import java.io.IOException;
import java.util.Iterator;

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

public class HadoopConsumer extends Configured implements Tool {
	

	private static Logger LOG = LoggerFactory.getLogger(HadoopConsumer.class);
	
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
		private HBaseAdmin admin;

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
			putrow.add("mdsp".getBytes(), "count".getBytes(),
					sumText.getBytes());
			Configuration hbaseConf = HBaseConfiguration.create();
			hbaseConf.set("hbase.zookeeper.quorum", "hadoop1,hadoop2,hadoop3");
			hbaseConf.set("hbase.zookeeper.master", "hadoop1:60000");
			admin = new HBaseAdmin(hbaseConf);
			table = new HTable(hbaseConf, "mdsp_mapping");
			HTableDescriptor htableDescriptor = new HTableDescriptor(admin.getTableDescriptor(table.getName()));
			htableDescriptor.addFamily(new HColumnDescriptor("mdsp"));
			if (!admin.tableExists("mdsp_mapping")) {
				admin.createTable(htableDescriptor);
			}

			
			table.put(putrow);
			LOG.info(table.toString() + putrow.toJSON());
			// context.write(key, sumText);

		}
	}

	@Override
	public int run(String[] args) throws Exception {
		long startTime = System.currentTimeMillis();
		CommandLineParser parser = new PosixParser();
		Options options = buildOptions();

		CommandLine cmd = parser.parse(options, args);

		Configuration conf = new Configuration();

		conf.set("kafka.topic", cmd.getOptionValue("topic", "k8test"));

		conf.set("kafka.groupid",
				cmd.getOptionValue("k8test_group", "k8test_group"));

		conf.set("kafka.zk.connect",
				cmd.getOptionValue("zk-connect", "10.100.10.183:2181"));
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

		KafkaOutputFormat.setOutputPath(job, new Path("hdfs://hadoop1:9000/user/temp02"));
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
			int exitCode = ToolRunner.run(new HadoopConsumer(), args);
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
