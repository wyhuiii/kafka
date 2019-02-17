package mykafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * 消息生产者实体类
 * @author wyhui
 */
public class MyProducer {
	private final static String TOPIC = "window-count";//定义Topic
	private static KafkaProducer<String,String> producer = null;//声明一个KafkaProducer对象，这里的泛型是消息中key和value的类型，我们这里都定义为String类型。
	public static void main(String[] args) {
		//配置生产者属性并创建生产者
		Properties producerProp = new Properties();
		producerProp.put("bootstrap.servers","192.168.184.128:9092");//这里的server我们就使用本地的，但是推荐写至少两个，防止其中有一个宕机。多个server用逗号隔开。
		producerProp.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");//定义消息中key的类型，我们这里定义为String。
		producerProp.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");//定义消息中value的类型，这里我们定义为String。
		//属性配置完成后，将属性对象作为参数，传入生产者对象的构造方法中创建生产者
		producer = new KafkaProducer<String, String>(producerProp);
		//发送消息
			/**
			 * sensd()方法会返回一个包含RecordMetadata的Future对象，
			 * 不过因为我们会忽略返回值，所以无法知道消息是否发送成功。如果不关心发送結果，那么可以使用这种发送方式。
			 */
			/*for(int i = 10;i < 13; i++) {
				ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, Integer.toString(i), Integer.toString(i));//参数分别是主题，消息的key，消息的value
				producer.send(record);
			}
			producer.send(new ProducerRecord<>(TOPIC, "henan", "luoyang"));*/
			
		
			producer.send(new ProducerRecord<>(TOPIC, null, "itemId=\"1115\",itemName=\"联想电脑\",userId=\"1003\""));//key为空
			producer.send(new ProducerRecord<>(TOPIC, null, "itemId=\"1116\",itemName=\"华硕电脑\",userId=\"1023\""));//key为空
			producer.send(new ProducerRecord<>(TOPIC, null, "itemId=\"1118\",itemName=\"惠普电脑\",userId=\"1031\""));//key为空
			producer.send(new ProducerRecord<>(TOPIC, null, "itemId=\"1117\",itemName=\"戴尔电脑\",userId=\"1026\""));//key为空
			producer.send(new ProducerRecord<>(TOPIC, null, "itemId=\"1116\",itemName=\"华硕电脑\",userId=\"1036\""));//key为空
			producer.send(new ProducerRecord<>(TOPIC, null, "itemId=\"1117\",itemName=\"戴尔电脑\",userId=\"1053\""));//key为空
			producer.send(new ProducerRecord<>(TOPIC, null, "itemId=\"1115\",itemName=\"联想电脑\",userId=\"1065\""));//key为空
			producer.send(new ProducerRecord<>(TOPIC, null, "itemId=\"1116\",itemName=\"华硕电脑\",userId=\"1077\""));//key为空
			producer.send(new ProducerRecord<>(TOPIC, null, "itemId=\"1118\",itemName=\"惠普电脑\",userId=\"1046\""));//key为空
			producer.send(new ProducerRecord<>(TOPIC, null, "itemId=\"1115\",itemName=\"联想电脑\",userId=\"1076\""));//key为空
			producer.send(new ProducerRecord<>(TOPIC, null, "itemId=\"1117\",itemName=\"戴尔电脑\",userId=\"1084\""));//key为空
			producer.send(new ProducerRecord<>(TOPIC, null, "itemId=\"1116\",itemName=\"华硕电脑\",userId=\"1027\""));//key为空
			producer.send(new ProducerRecord<>(TOPIC, null, "itemId=\"1115\",itemName=\"联想电脑\",userId=\"1099\""));//key为空

			
			
			
			
			
			
			System.out.println("消息发送完毕");
			/**
			 * 同步发送消息：
			 * send()方法中如果使用一个参数的这个方法那么返回的是一个Future对象，然后调用Future对象的get()方法等待kafka响应。
			 * 如果服务器返回错误，get()方法会拋出异常。如果没有发生错误，我们会得到一个RecordMetadata对象，可以用它获取消息的offset。
			 */
			//producer.send(record).get();
			/**
			 * 异步发送消息：
			 * 在send()方法中需要传入一个实现了org.apache.kafka.clients.producer.Callback这个接口的对象，使用内部类实现onCompletion()方法。
			 * 如果kafka返回一个错误，onCompletion方法会拋出一个非空异常，这里我们只是把异常信息打印出來，实际业务中可以对其进一步处理。
			 */
			/*producer.send(record, new Callback() {
				@Override
				public void onCompletion(RecordMetadata recordMeatadata, Exception e) {
					if(e != null) {
						e.printStackTrace();
					}
				}
			});*/
			producer.close();//关闭生产者
		}
	}
	
	
	
	
	
	
	
	
	
	


