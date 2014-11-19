package com.pzoom.mdsp.mapping;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author chenbaoyu
 *
 */
public class ZkUtils implements Closeable {
	
    private static Logger LOG = LoggerFactory.getLogger(ZkUtils.class);
    
    private static final String CONSUMERS_PATH = "/consumers";
    private static final String BROKER_IDS_PATH = "/brokers/ids";
    private static final String BROKER_TOPICS_PATH = "/brokers/topics";
	
	private ZkClient zkClient;
	private Map<String, String> brokers;
    public ZkUtils(Configuration config) {
    	String zk = config.get("kafka.zk.connect");
    	zkClient = new ZkClient(zk, 10000, 10000, new StringSerializer() );
    }
	
	@Override
	public void close() throws IOException {
        if (zkClient != null) {
        	zkClient.close();
        }
	}
	
	
    private List<String> getChildrenParentMayNotExist(String path) {
        try {
            List<String> children = zkClient.getChildren(path);
            return children;
        } catch (ZkNoNodeException e) {
            return new ArrayList<String>();
        }
    }
    
    private String getOffsetsPath(String group, String topic, String partition) {
        return CONSUMERS_PATH + "/" + group + "/offsets/" + topic + "/" + partition;
    }
    
    private String getTempOffsetsPath(String group, String topic, String partition) {
        return CONSUMERS_PATH + "/" + group + "/offsets-temp/" + topic + "/" + partition;
    }
    
    private String getTempOffsetsPath(String group, String topic) {
        return CONSUMERS_PATH + "/" + group + "/offsets-temp/" + topic ;
    }
	
    public String getBroker(String id) {
        if (brokers == null) {
            brokers = new HashMap<String, String>();
            List<String> brokerIds = getChildrenParentMayNotExist(BROKER_IDS_PATH);
            for(String bid: brokerIds) {
                String data = zkClient.readData(BROKER_IDS_PATH + "/" + bid);
                try{
	    			ObjectMapper m = new ObjectMapper();  
	    			JsonNode rootNode = m.readValue(data, JsonNode.class);
	    			JsonNode hostNode = rootNode.path("host");
	    			JsonNode portNode = rootNode.path("port");
	                brokers.put(bid, hostNode.getTextValue()+":"+portNode.getIntValue());
                }catch(Exception e){
                	LOG.error(e.getMessage());
                }

            }
        }
        return brokers.get(id);
    }
	
	/**
	 * 获取指定主题下的分区信息
	 * @param topic
	 * @return
	 */
	public Map<String,List<KafkaPartition>> getKafkaTopic(String topic){
		Map<String,List<KafkaPartition>> topicMap = new HashMap<String,List<KafkaPartition>>();
		List<KafkaPartition> partitions = new ArrayList<KafkaPartition>();
		try{
			String topicData = zkClient.readData(BROKER_TOPICS_PATH + "/" + topic);
			if(org.apache.commons.lang.StringUtils.isEmpty(topicData))
			{
				LOG.error("getPartitions method read topic failed");
				return null;
			}
			ObjectMapper m = new ObjectMapper();  
			JsonNode rootNode = m.readValue(topicData, JsonNode.class);
			JsonNode partitionsNode = rootNode.path("partitions");
			for (Iterator<Entry<String, JsonNode>> iterator = partitionsNode.getFields(); iterator.hasNext();) {
				Entry<String, JsonNode> partitionNode = iterator.next();
				KafkaPartition partition = new KafkaPartition(partitionNode.getKey());
				for (Iterator<JsonNode> iterator2 = partitionNode.getValue().iterator(); iterator2
						.hasNext();) {
					JsonNode brokerNode =iterator2.next();
					String broker = this.getBroker(brokerNode.toString());
					if(broker != null){
						partition.getSeeds().add(broker);
					}
				}
				partitions.add(partition);
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		LOG.info("partitions size:"+partitions.size());
		topicMap.put(topic, partitions);
		return topicMap;
	}
	
	
	/**
	 * 提交最新的偏移量到zookeeper
	 * @param group
	 * @param topic
	 * @return
	 */
    public boolean commit(String group, String topic) {
        List<String> partitions = getChildrenParentMayNotExist(getTempOffsetsPath(group, topic));
        for(String partition: partitions) {
            String path = getTempOffsetsPath(group, topic, partition);
            String offset = this.zkClient.readData(path);
            setLastCommit(group, topic, partition, Long.valueOf(offset), false);
            this.zkClient.delete(path);
        }
        return true;
    }
    /**
     * 获取保存在zookeeper上的最大偏移量
     * @param group 分组
     * @param topic 主题 
     * @param partition 分区
     * @return
     */
    public long getLastCommit(String group, String topic, String partition) {
        String znode = getOffsetsPath(group ,topic ,partition);
        String offset = this.zkClient.readData(znode, true);
        
        if (offset == null) {
            return 0L;
        }
        return Long.valueOf(offset)+1;
    }
    /**
     * 更新zookeeper上的最大偏移量
     * @param group 
     * @param topic
     * @param partition
     * @param commit 最新偏移量
     * @param temp 
     */
    public void setLastCommit(String group, String topic, String partition, long commit, boolean temp) {
        String path = temp? getTempOffsetsPath(group ,topic ,partition)
                        : getOffsetsPath(group ,topic ,partition);
        if (!zkClient.exists(path)) {
        	zkClient.createPersistent(path, true);
        }
        zkClient.writeData(path, commit);
    }

    static class StringSerializer implements ZkSerializer {

        public StringSerializer() {}
        @Override
        public Object deserialize(byte[] data) throws ZkMarshallingError {
            if (data == null) return null;
            return new String(data);
        }

        @Override
        public byte[] serialize(Object data) throws ZkMarshallingError {
            return data.toString().getBytes();
        }
        
    }
    
    public static void main(String[] args) {
    	 Configuration conf = new Configuration();
         
         conf.set("kafka.topic", "k8hadoop");
         
         conf.set("kafka.groupid","test_group");
         
         conf.set("kafka.zk.connect","10.100.10.183:2181");
         
         ZkUtils zk = new ZkUtils(conf);
         
         String zkconn = conf.get("kafka.zk.connect");
         ZkClient zkClient = new ZkClient(zkconn, 10000, 10000, new StringSerializer() );
         
         /**
          * 测试获取zookeeper节点信息
          */
         //part1:
//         Map<String, String> brokers = new HashMap<String, String>();
//         List<String> brokerIds = zk.getChildrenParentMayNotExist(BROKER_IDS_PATH);
//         for(String bid: brokerIds) {
//             String data = zkClient.readData(BROKER_IDS_PATH + "/" + bid);
//             try{
//	    			ObjectMapper m = new ObjectMapper();  
//	    			JsonNode rootNode = m.readValue(data, JsonNode.class);
//	    			JsonNode hostNode = rootNode.path("host");
//	    			JsonNode portNode = rootNode.path("port");
//	                brokers.put(bid, hostNode.getTextValue()+":"+portNode.getIntValue());
//             }catch(Exception e){
//             	e.printStackTrace();
//             }
//
//         }
         //part2:
//         List<KafkaPartition> partitions = new ArrayList<KafkaPartition>();
// 		try{
// 			String topicData = zkClient.readData(BROKER_TOPICS_PATH + "/" + "com.pzoom.mdsp.model.SettlementEntity");
// 			System.out.println(topicData);
// 			ObjectMapper m = new ObjectMapper();  
// 			JsonNode rootNode = m.readValue(topicData, JsonNode.class);
// 			JsonNode partitionsNode = rootNode.path("partitions");
// 			for (Iterator<Entry<String, JsonNode>> iterator = partitionsNode.getFields(); iterator.hasNext();) {
// 				Entry<String, JsonNode> partitionNode = iterator.next();
// 				KafkaPartition partition = new KafkaPartition(partitionNode.getKey());
// 				for (Iterator<JsonNode> iterator2 = partitionNode.getValue().iterator(); iterator2
// 						.hasNext();) {
// 					JsonNode brokerNode =iterator2.next();
// 					System.out.println(brokerNode.toString());
// 				}
// 				partitions.add(partition);
// 			}
// 		}catch(Exception e){
// 			e.printStackTrace();
// 		}
 		//part3:
         Map<String,List<KafkaPartition>> topicMap = zk.getKafkaTopic("k8test");
        for (KafkaPartition kafkaPartition : topicMap.get("k8test")) {
        	long lastCommit = zk.getLastCommit("test_group", "k8test", kafkaPartition.getId());
        	System.out.println(lastCommit);
		}
        
        String topicData = zkClient.readData(BROKER_TOPICS_PATH);
        
        System.out.println(topicData);
        
        
	}
}
