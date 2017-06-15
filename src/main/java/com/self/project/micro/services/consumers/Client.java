package com.self.project.micro.services.consumers;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.self.project.micro.services.utils.CommunicatorUtil;

import akka.actor.ActorSystem;
import akka.actor.Props;


/**
 * Client class creates a new topic in Kakfa and notify producer about its presence using 
 * {@link com.self.project.services.constants.Constants#BROAD_CAST_VALUE this} parameter
 * and start listening to it for any published messages.
 * <br>
 * sample usage <br><br>
 * java -Dlogback.configurationFile="../conf/logback.xml" -cp MicroServices-0.0.1-SNAPSHOT.jar 
 * com.self.project.micro.services.consumers.Client ../conf/ <b><i>YOUR_TOPIC_NAME</i></b>
 * 
 * @author Puneeth
 *
 */
public class Client {

	public static final Logger LOGGER = LoggerFactory.getLogger(Client.class);

	public static final Client _INSTANCE = new Client();

	private Path configFileLocation;
	private String privateChannelName;
	ActorSystem chattingSystem;

	// To remove subscription from producer
	static{
		Runtime.getRuntime().addShutdownHook(
				new Thread(){
					@SuppressWarnings("deprecation")
					public void run() {
						try {
							CommunicatorUtil.updateConfig(_INSTANCE.configFileLocation, _INSTANCE.privateChannelName, false);
							_INSTANCE.chattingSystem.shutdown();
							LOGGER.info("Thank you for using \"{}\"  channel...", _INSTANCE.privateChannelName);
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
				_INSTANCE.configFileLocation = Paths.get(args[0]);
				_INSTANCE.privateChannelName = args[1];
				_INSTANCE.chattingSystem = ActorSystem.create("Chatting-System");
				_INSTANCE.chattingSystem.actorOf(Props.create(ConsumerActor.class, _INSTANCE.configFileLocation, _INSTANCE.privateChannelName), "client");
			}else{
				LOGGER.error("OOPS!!! please provide config file location and a private topic name that you will be using.\nSample usage is \n\n"
						+ "java -Dlogback.configurationFile=\"../conf/logback.xml\" -cp MicroServices-0.0.1-SNAPSHOT.jar "
						+ "com.self.project.micro.services.consumers.Client ../conf/ TOPIC_NAME");
			}
		}catch(Exception e){
			LOGGER.error("Unknown exception=\"{}\" occurred while using Consumer actor",e.getMessage());
		}
	}

}
