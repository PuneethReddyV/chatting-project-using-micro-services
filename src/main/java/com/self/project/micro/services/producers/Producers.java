package com.self.project.micro.services.producers;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.self.project.micro.services.utils.CommunicatorUtil;
import com.self.project.services.constants.Constants;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * Produces messages to all live consumers or to a specific consumer.
 * 
 * <br>
 * Sample usage.
 * <br><br>
 *java -Dlogback.configurationFile="../conf/logback.xml" -cp MicroServices-0.0.1-SNAPSHOT.jar 
 *com.self.project.micro.services.producers.Producers ../conf/application.conf
 * @author Puneeth
 */

public class Producers {

	public static final Logger LOGGER = LoggerFactory.getLogger(Producers.class);

	private static final Scanner MENU_READER = new Scanner(System.in);

	private static final Scanner MESSAGE_READER = new Scanner(System.in);

	private static final ScheduledExecutorService EXECUTOR = Executors.newScheduledThreadPool(1);

	public static final Producers _INSTANCE = new Producers();

	private String groupChat;

	private Producer<String, String> producer;

	private Config CONFIG;

	private Path configFile;

	//A executable command to keep updating the subscribed consumers and their corresponding topics.
	static Runnable command = new Runnable() {
		@Override
		public void run() {
			_INSTANCE.CONFIG = ConfigFactory.parseFile(_INSTANCE.configFile.toFile());
		}
	};

	/**
	 * A hook to make gracefull shutdown by stop listing to the configuration file.
	 */
	static{
		Runtime.getRuntime().addShutdownHook(
				new Thread(){
					public void run() {
						try {
							CommunicatorUtil.sendMessage(_INSTANCE.groupChat, "Producers qiuts :( !!!", _INSTANCE.producer);
							MESSAGE_READER.close();
							MENU_READER.close();
							_INSTANCE.producer.close();
							LOGGER.info("Thank you for using this application...");
							EXECUTOR.shutdown();
						} catch (Exception e) {
							LOGGER.error("unable to publish messages due to error = \"{}\"",e.getMessage());
						}
					};
				}
				);
	}

	public static void main(String[] args) {
		try{
			if(args != null && args.length == 1 ){
				_INSTANCE.configFile = Paths.get(args[0]);
				_INSTANCE.CONFIG = ConfigFactory.parseFile(_INSTANCE.configFile.toFile());
				_INSTANCE.groupChat = _INSTANCE.CONFIG.getString(Constants.BROAD_CAST_VALUE);

				//create group chat to communicate with all consumers.
				CommunicatorUtil.createChannel(_INSTANCE.groupChat, _INSTANCE.CONFIG.getConfig(Constants.ZK_CONFIG_HOME));
				EXECUTOR.scheduleAtFixedRate(command, 30L, 15L, TimeUnit.SECONDS);
				Properties props = CommunicatorUtil.getProperties(_INSTANCE.CONFIG.getConfig(Constants.KAFKA_PRODUCER_CONFIG_HOME));
				_INSTANCE.producer = new KafkaProducer<>(props);

				while(true){
					String topicName = _INSTANCE.showMenu(_INSTANCE.groupChat);
					if(topicName==null){
						break;
					}
					LOGGER.info("Type your message...");
					String message = MESSAGE_READER.nextLine();
					CommunicatorUtil.sendMessage(topicName, message, _INSTANCE.producer);
				}
				System.exit(0);
			}else{
				LOGGER.error("OOPS!!! please provide config file location.\n Sample Usage\n\n"
						+ "java -Dlogback.configurationFile=\"../conf/logback.xml\" -cp MicroServices-0.0.1-SNAPSHOT.jar "
						+ "com.self.project.micro.services.producers.Producers ../conf/application.conf");
			}
		}catch(Exception e){
			LOGGER.error("Unknow exception=\"{}\" occourred ",e.getMessage());
		}
	}

	/**
	 * Displays a menu and directs you to communicate with a <b>or</b> all existing clients.
	 * @param groupChat channel name where all the consumers are subscribed to.
	 * @return opted channel or kafka topic name.
	 */

	private String showMenu(String groupChat){
		String returnValue = null;
		try{
			List<String> counsumers = CONFIG.getStringList(Constants.CONSUEMR_GROUP_LIST);
			counsumers.add(groupChat);
			if(counsumers.isEmpty()){
				LOGGER.warn("You have no clients to listen with you...\n Start a consumer(s) first");
			}else{
				LOGGER.info("Whom do you like to communicate?\nPress\n");
				int i;
				for (i=1; i<= counsumers.size(); i++) {
					LOGGER.info("{} to send message to \"{}\"\n",i, counsumers.get(i-1));
				}
				LOGGER.info("Higer number to quit.\n");
				int choice = MENU_READER.nextInt();
				returnValue = counsumers.get(choice-1);
			}
		}catch(ArrayIndexOutOfBoundsException | NoSuchElementException | IllegalStateException e ){
			//willing to quit
		}catch (Exception e) {
			LOGGER.error("Unknown error=\"{}\" occurred while requesting for clients input.",e.getMessage());
		}

		return returnValue;
	}

}

