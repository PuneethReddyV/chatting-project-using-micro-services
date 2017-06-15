package com.self.project.micro.services.utils;


import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.self.project.services.constants.Constants;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;

import kafka.admin.AdminUtils;
import kafka.common.TopicExistsException;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;

/**
 * A Simple Util class which is used by producer and consumer classes.
 * 
 * @author Puneeth
 *
 */
public class CommunicatorUtil {

	public static final Logger LOGGER = LoggerFactory.getLogger(CommunicatorUtil.class);


	public static final String WELCOME_MESSAGE = "A new subscription is made on \"%s\" channel :)";

	public static final String EXIT_MESSAGE = "\"%s\" channel listener left :(";
	
	/**
	 * Provides properties for a given configuration object.
	 * @param config Configuration which has to be converted into properties.
	 * @return Properties
	 */
	public static Properties getProperties(Config config) {
		Properties returnValue = new Properties();
		for( Entry<String, ConfigValue> element : config.entrySet()){
			String key = element.getKey();
			returnValue.put(key, config.getString(key));
		}
		return returnValue;
	}

	/**
	 * creates a new channel, used whenever a new consumer is up or a producer is up.
	 * @param chanelName new channel needs to be created.
	 * @param zkConfig Zoo Keepers configuration.
	 */
	public static void createChannel(String chanelName, Config zkConfig) {
		ZkClient zkClient = null;
		ZkUtils zkUtils = null;
		try {
			String zookeeperHosts = zkConfig.getString(Constants.HOST_PORT_KEY); 
			int sessionTimeOutInMs = zkConfig.getInt(Constants.ZK_SESSION_TIMEOUT_KEY);
			int connectionTimeOutInMs = zkConfig.getInt(Constants.ZK_SYNS_TIME_KEY);

			zkClient = new ZkClient(zookeeperHosts, sessionTimeOutInMs, connectionTimeOutInMs, ZKStringSerializer$.MODULE$);
			zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperHosts), false);

			String topicName = chanelName;
			Properties topicConfiguration = new Properties();

			AdminUtils.createTopic(zkUtils, topicName, 1, 1, topicConfiguration);

		}catch(TopicExistsException e){
			LOGGER.error("Topic name=\"{}\" exits. Error=\"{}\".", chanelName, e.getMessage());
		}catch (Exception e) {
			LOGGER.error("Unknow error \"{}\" occurred while creating a topic.", e.getMessage());
		} finally {
			if (zkClient != null) {
				zkClient.close();
			}
		}
	}
	
	/**
	 * Notify the producer about this new consumer by updating {@link com.self.project.services.constants.Constants#BROAD_CAST_VALUE this}
	 * value 
	 * @param configFile location of the configuration file.
	 * @param consumerName Name of the consumer
	 * @param isAppend true to subscribe for producer messages, false to remove subscription
	 * @return true if producer is notified successfully else false.
	 */
	public static boolean updateConfig(Path configFile, String consumerName, boolean isAppend){
		boolean returnValue = false;

		try {
			Config config = ConfigFactory.parseFile(configFile.toFile());
			boolean isEmpty = config.getStringList(Constants.CONSUEMR_GROUP_LIST).isEmpty();
			List<String> configuration = new ArrayList<>(Files.readAllLines(configFile, StandardCharsets.UTF_8));

			for (int i = 0; i < configuration.size(); i++) {
				String updatedConsumers;
				if (configuration.get(i).contains("consumer-names")) {
					if(isAppend){
						updatedConsumers = configuration.get(i).split(Constants.CLOSE_BRACKET)[0];
						updatedConsumers = (isEmpty) ?  updatedConsumers+"\""+consumerName+"\""+Constants.CLOSE_BRACKET+"," : updatedConsumers+",\""+consumerName+"\""+Constants.CLOSE_BRACKET+",";

					}else{
						updatedConsumers = configuration.get(i).replace("\""+consumerName+"\"", "");
						updatedConsumers = updatedConsumers.replace(",,", ""); // if any middle variable of list removed
						updatedConsumers = updatedConsumers.replace("[,", "["); // if first variable of list removed
						updatedConsumers = updatedConsumers.replace(",]", "]"); // if last variable of list removed
					}
					configuration.set(i, updatedConsumers);
					break;
				}
			}
			Files.write(configFile, configuration, StandardCharsets.UTF_8);
			returnValue = true;
		} catch (IOException e) {
			LOGGER.error("IOException=\"{}\" occourred while updating configuration", e.getMessage());
		}catch (Exception e) {
			LOGGER.error("Unknown Exception=\"{}\" occourred while configuration", e.getMessage());
		}
		return returnValue;
	}

	/**
	 * Publish any entry or exit of consumer to all consumers.
	 * @param groupChatTopicName where all the consumers are subscribed
	 * @param consumerName channel name through with consumer consumes kafka messages
	 * @param producer Kafka producer instance
	 * @param isWelcomeMessage true for subscription, flase for unsubscription.
	 * @throws Exception exception is thrown incase of any error in publishing messages.
	 */


	public static void publishEvents(String groupChatTopicName, String consumerName, Producer<String, String> producer, boolean isWelcomeMessage) throws Exception{
		if(isWelcomeMessage) {
			sendMessage(groupChatTopicName, String.format(WELCOME_MESSAGE, consumerName), producer);
		}else{
			sendMessage(groupChatTopicName, String.format(EXIT_MESSAGE, consumerName), producer);
		}
	}
	

	/**
	 * Sends messages to specific consumer(s).
	 * @param topicName Name of the topic to send message
	 * @param message Message to communicate
	 * @param producer Producer instance.
	 */
	public static void sendMessage( String topicName, String message, Producer<String, String> producer) throws Exception{
		ProducerRecord<String, String> rec = new ProducerRecord<String, String>(topicName, message);
		producer.send(rec);
	}
}
