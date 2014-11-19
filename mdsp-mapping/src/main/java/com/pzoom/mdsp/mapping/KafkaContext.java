package com.pzoom.mdsp.mapping;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pzoom.mdsp.model.SettlementEntity;
import com.pzoom.rpc.serialize.RpcSerializer;

public class KafkaContext{
	private static Logger LOG = LoggerFactory.getLogger(KafkaContext.class);
	private String topic;
	private int partition;
	private long curOffset;
	private long lastCommit;
	private List<String> seeds;
	private List<String> replicaBrokers = new ArrayList<String>();
	private Iterator<MessageAndOffset> iterator;
	
	public KafkaContext() {
	}
	
	public KafkaContext(String topic,int partition,List<String> seeds,long lastCommit) {
		this.topic = topic;
		this.partition = partition;
		this.seeds = seeds;
		this.lastCommit = lastCommit;
		this.curOffset = lastCommit;
	}
	
	
	/**
	 * 获取以指定offset开始的Kafka数据，缓存的大小为64M
	 * @param offset zookeeper和保存的offset
	 * @return
	 */
	public Iterator<MessageAndOffset> createIterable(long offset){
		
		SimpleConsumer consumer = null;
		try{
			PartitionMetadata metadata = findLeader(this.seeds);
	
			if (metadata == null) {
				LOG.info("Can't find metadata for Topic and Partition. Exiting");
				return null;
			}
	
			if (metadata.leader() == null) {
				LOG.info("Can't find Leader for Topic and Partition. Exiting");
				return null;
			}
			
			String leadBroker = metadata.leader().host();
			int port = Integer.valueOf(this.seeds.get(0).split(":")[1]);
			String clientName = "Client_" + this.topic + "_" + this.partition;
			
			consumer = new SimpleConsumer(leadBroker, port,
					100000,1024*1024, clientName);
			
			long lastOffset = this.getLastOffset(consumer, topic, partition, kafka.api.OffsetRequest.LatestTime(), clientName);
			LOG.info(offset+"----------------"+lastOffset);
			FetchRequest req = new FetchRequestBuilder().clientId(clientName)
					.addFetch(this.topic, this.partition, offset,1024*1024).build();
			
			FetchResponse fetchResponse = consumer.fetch(req);
			
			if (fetchResponse.hasError()) {
				short code = fetchResponse.errorCode(this.topic, this.partition);
				if (code == ErrorMapping.OffsetOutOfRangeCode())  {
					LOG.error("Offset out of range. code:{} offset:{}",code,this.lastCommit);
					offset =  this.getLastOffset(consumer, topic, partition, kafka.api.OffsetRequest.EarliestTime(), clientName);
					req = new FetchRequestBuilder().clientId(clientName)
							.addFetch(this.topic, this.partition, offset,1024*1024).build();
					fetchResponse = consumer.fetch(req);
					return fetchResponse.messageSet(this.topic, this.partition).iterator();
				}else{
					LOG.error("Error fetching data from the Broker:" + leadBroker + " Reason: " + code);
				}
				consumer.close();
				return null;
			}
			
			return fetchResponse.messageSet(this.topic, this.partition).iterator();
		
		}finally{
			if(consumer != null){
				consumer.close();
			}
		}
	}
	/**
	 * 获取指定区域ID的元数据信息
	 * @param seeds
	 * @return
	 */
	private PartitionMetadata findLeader(List<String> seeds) {
		PartitionMetadata returnMetaData = null;
		for (String seed : this.seeds) {
			SimpleConsumer consumer = null;
			try{
				String[] seedArray = seed.split(":");
				String host = seedArray[0];
				int port = Integer.valueOf(seedArray[1]);
				
				consumer = new SimpleConsumer(host, port, 10000, 64 * 1024,
						"leaderLookup");
				List<String> topics = new ArrayList<String>();
				topics.add(this.topic);
				TopicMetadataRequest req = new TopicMetadataRequest(topics);
				
				TopicMetadataResponse resp = consumer.send(req);
				List<TopicMetadata> metaData = resp.topicsMetadata();
				for (TopicMetadata item : metaData) {
					for (PartitionMetadata part : item.partitionsMetadata()) {
						if (part.partitionId() == this.partition) {
							returnMetaData = part;
							break;
						}
					}
				}
				
			} catch (Exception e) {
				LOG.error("Error communicating with Broker [" + seed
						+ "] to find Leader for [" + topic
						+ ", " + partition + "] Reason: " + e);
				
			} finally {
	
				if (consumer != null)
					consumer.close();
	
			}
		}
		
		if (returnMetaData != null) {
			this.replicaBrokers.clear();
			for (kafka.cluster.Broker replica : returnMetaData.replicas()) {
				this.replicaBrokers.add(replica.host()+":"+replica.port());
			}
		}
		return returnMetaData;
	}
	

	/**
	 * 检查并获取新的kafka数据
	 * @return
	 */
	public KeyValueInfo getNextKeyValue(){
		if(this.iterator == null){
			this.iterator = this.createIterable(this.curOffset);
		}

		if(!this.iterator.hasNext()){
			this.iterator = this.createIterable(this.curOffset);
			if(!this.iterator.hasNext())
			{
				LOG.info("--------------------------"+this.curOffset);
				return null;
			}
		}
		
		MessageAndOffset messageOffset = iterator.next();
        Message message = messageOffset.message();
        
		KeyValueInfo keyValueInfo = new KeyValueInfo();
		LongWritable key = new LongWritable();
		BytesWritable value = new BytesWritable();
		
		
        ByteBuffer buffer = message.payload();
        byte[] bytes = new byte[buffer.limit()];
        buffer.get(bytes);
        try {
        	SettlementEntity settlementEntity = RpcSerializer.deserialize(bytes, SettlementEntity.class);
        	key.set(settlementEntity.getCpid());
        	value.set(String.valueOf(settlementEntity.getCost()).getBytes(),0,String.valueOf(settlementEntity.getCost()).length());
        } catch (Exception e) {
			e.printStackTrace();
			LOG.error("deserialize SettlementEntity error:"+e.getMessage());
		}
		keyValueInfo.setKey(key);
		keyValueInfo.setValue(value);
		keyValueInfo.setCurOffset(messageOffset.offset());
		
		this.curOffset = messageOffset.nextOffset();
		return keyValueInfo;
	}
	
	/**
	 * 获取最新offset
	 * @param consumer
	 * @param topic
	 * @param partition
	 * @param whichTime
	 * @param clientName
	 * @return
	 */
	private long getLastOffset(SimpleConsumer consumer, String topic,
			int partition, long whichTime, String clientName) {
		TopicAndPartition topicAndPartition = new TopicAndPartition(topic,
				partition);

		Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
		requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(
				whichTime, 1));

		OffsetRequest request = new kafka.javaapi.OffsetRequest(requestInfo,
				kafka.api.OffsetRequest.CurrentVersion(), clientName);

		OffsetResponse response = consumer.getOffsetsBefore(request);
		if (response.hasError()) {
			LOG.error("Error fetching data Offset Data the Broker. Reason: "
							+ response.errorCode(topic, partition));
			return 0;
		}

		long[] offsets = response.offsets(topic, partition);

		return offsets[0];
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public int getPartition() {
		return partition;
	}

	public void setPartition(int partition) {
		this.partition = partition;
	}

	public List<String> getSeeds() {
		return seeds;
	}

	public void setSeeds(List<String> seeds) {
		this.seeds = seeds;
	}

	public long getLastCommit() {
		return lastCommit;
	}

	public void setLastCommit(long lastCommit) {
		this.lastCommit = lastCommit;
	}


	public List<String> getReplicaBrokers() {
		return replicaBrokers;
	}

	public void setReplicaBrokers(List<String> replicaBrokers) {
		this.replicaBrokers = replicaBrokers;
	}


	public Iterator<MessageAndOffset> getIterator() {
		return iterator;
	}

	public void setIterator(Iterator<MessageAndOffset> iterator) {
		this.iterator = iterator;
	}

	public long getCurOffset() {
		return curOffset;
	}

	public void setCurOffset(long curOffset) {
		this.curOffset = curOffset;
	}

	
}
