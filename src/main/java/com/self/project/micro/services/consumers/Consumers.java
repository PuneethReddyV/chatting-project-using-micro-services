package com.self.project.micro.services.consumers;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.self.project.micro.services.utils.CommunicatorUtil;
import com.self.project.services.constants.Constants;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * Consumer class creates a new topic in Kakfa and notify producer about its presence using 
 * {@link com.self.project.services.constants.Constants#BROAD_CAST_VALUE this} parameter
 * and start listening to it for any published messages.
 * <br>
 * sample usage <br><br>
 * java -Dlogback.configurationFile="../conf/logback.xml" -cp MicroServices-0.0.1-SNAPSHOT.jar 
 * com.self.project.micro.services.consumers.Consumers ../conf/application.conf <b><i>YOUR_TOPIC_NAME</i></b>
 * 
 * @author Puneeth
 */

public class Consumers {


	public static final Logger LOGGER = LoggerFactory.getLogger(Consumers.class);

	private Config CONFIG;

	private Path configFile;

	private String channelName;

	private String groupChat;
	
	private Producer<String, String> producer;

	private static final Consumers _INSTANCE = new Consumers();

	// To remove subscription from producer
	static{
		Runtime.getRuntime().addShutdownHook(
				new Thread(){
					public void run() {
						try {
							CommunicatorUtil.updateConfig(_INSTANCE.configFile, _INSTANCE.channelName, false);
							LOGGER.info("Thank you for using \"{}\"  channel...", _INSTANCE.channelName);
							CommunicatorUtil.publishEvents(_INSTANCE.groupChat, _INSTANCE.channelName, _INSTANCE.producer, false);
							_INSTANCE.producer.close();
						} catch (Exception e) {
							LOGGER.error("Please have atleast a producer to send messages.\nUnknown error=\"{}\" occurred",e.getMessage());
						}
					};
				}
				);
	}

	public static void main(String[] args) {
		try{
			if(args != null && args.length == 2 ){
				_INSTANCE.configFile = Paths.get(args[0]);
				_INSTANCE.channelName = args[1];
				_INSTANCE.CONFIG = ConfigFactory.parseFile(_INSTANCE.configFile.toFile());
				_INSTANCE.groupChat = _INSTANCE.CONFIG.getString(Constants.BROAD_CAST_VALUE);
				Properties props = CommunicatorUtil.getProperties(_INSTANCE.CONFIG.getConfig(Constants.KAFKA_PRODUCER_CONFIG_HOME));
				_INSTANCE.producer = new KafkaProducer<>(props);
				CommunicatorUtil.publishEvents(_INSTANCE.groupChat, _INSTANCE.channelName, _INSTANCE.producer, true);
				if(CommunicatorUtil.updateConfig(_INSTANCE.configFile, _INSTANCE.channelName, true)){
					CommunicatorUtil.createChannel(_INSTANCE.channelName, _INSTANCE.CONFIG.getConfig(Constants.ZK_CONFIG_HOME));
					_INSTANCE.listenFromChannel(_INSTANCE.channelName, _INSTANCE.groupChat);
				}else{
					LOGGER.error("Unable to update configuration.");
				}

			}else{
				LOGGER.error("OOPS!!! please provide config file location and consumer name.\nSample usage is \n\n"
						+ "java -Dlogback.configurationFile=\"../conf/logback.xml\" -cp MicroServices-0.0.1-SNAPSHOT.jar "
						+ "com.self.project.micro.services.consumers.Consumers ../conf/application.conf TOPIC_NAME");
			}
		} catch (Exception e) {
			LOGGER.error("Please have atleast a producer to send messages.\nUnknown error=\"{}\" occurred",e.getMessage());
		}
	}

	/**
	 * Subscribe to a specific channel and print messages that are published to that channel.
	 * @param channelNames Kafka topic name.
	 */
	private void listenFromChannel(String... channelNames) {
		List<String> topicNames = Arrays.asList(channelNames);
		Properties props = CommunicatorUtil.getProperties(CONFIG.getConfig(Constants.KAFKA_CONSUMER_CONFIG_HOME));
		props.put("group.id", topicNames.get(0));
		@SuppressWarnings("resource")
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		consumer.subscribe(topicNames);
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records)
				if(record.topic().equals(topicNames.get(0))){
					LOGGER.info("Prdoucer personally said >  {} ", record.value());
				}else{
					LOGGER.info("Prdoucer boradcasted >  {} ", record.value());
				}

		}		
	}

}
