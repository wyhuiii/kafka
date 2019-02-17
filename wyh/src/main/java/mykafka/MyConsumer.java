package mykafka;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * 消息的消費者
 * @author wyhui
 *
 */
public class MyConsumer {
	public static void main(String[] args) {
		//配置消费者属性并创建消费者
		Properties consumerProp = new Properties();
		consumerProp.put("bootstrap.servers","192.168.184.128:9092");//连接kafka集群server，若有多个server，用逗号隔开
		consumerProp.put("group.id", "wyhtest");//每个消费者分配独立的group.id，该值不是必需的，它指定了KafkaCosumer属于哪个消费者群组。创建不属于任何一个群组的消費者也是可以的，只是这样做不太常见。
		consumerProp.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");//与生产者中的属性类似，不过这里指的是使用指定的类把字节数组转成java对象。
		consumerProp.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		consumerProp.put("enable.auto.commit", "true");//消費者自動提交偏移量，提交間隔默認是5s。
		consumerProp.put("auto.commit.interval.ms", "30000");//offset的提交間隔
		//創建消費者
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(consumerProp);
		// 訂閱主題
		consumer.subscribe(Arrays.asList("window-count-out"));//subscribe方法接受的是一個Topic列表作為參數，所以需要先創建一個主題列表。方法的參數也可以使用正則表達式匹配多個主題，比如傳入"mytopic.*"進行匹配。
		//輪詢
		while(true) {//这是一个无限循环。消费者实际上是一个长期运行的应用程序，它通过持续轮询向kafka请求数据。
			/**
			 * poll（）：消费者必须持续对kafka进行轮询，否则会被认为已经死亡，它的分区会被移交给群组里的其他消费者。
			 * 传入的参数是一个超时时间，用于控制poll（）方法的阻塞时间。
			 * 返回的是一个记录列表。
			 */
			ConsumerRecords<String, String> records = consumer.poll(1000);
			for(ConsumerRecord<String, String> record : records) {//遍历记录列表
				long offset = record.offset();
				int partition = record.partition();
				Object key = record.key();//返回的是Object
				Object value = record.value();
				System.out.println("offset:"+offset+",key:"+key+",value:"+value);
			}
		}
	
	}
}
