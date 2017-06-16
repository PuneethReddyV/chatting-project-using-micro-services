package com.self.project.micro.services.consumers;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

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

import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.UntypedActor;
import scala.concurrent.duration.FiniteDuration;

/**
 * Cosumer actor to consume message from a topic.
 * <br>
 * @author Puneeth
 *
 */
public class ConsumerActor extends UntypedActor{

	public static final Logger LOGGER = LoggerFactory.getLogger(ConsumerActor.class);
	private static final List<String> SUBSCRIBE_TO_LIST = new ArrayList<>();

	private Path configFileLocation;
	private String privateChannelName;
	private Config config;
	private String broadCastingGroup;
	private Producer<String, String> producer;
	private KafkaConsumer<String, String> consumer;

	public ConsumerActor(final Path configFileLocation, final String privateChannelName){
		this.configFileLocation = configFileLocation;
		this.privateChannelName = privateChannelName;
		this.config = ConfigFactory.parseFile(this.configFileLocation.toFile());
		this.broadCastingGroup = this.config.getString(Constants.BROAD_CAST_VALUE);
		this.producer = new KafkaProducer<>(CommunicatorUtil.getProperties(this.config.getConfig(Constants.KAFKA_PRODUCER_CONFIG_HOME)));
		Properties props = CommunicatorUtil.getProperties(this.config.getConfig(Constants.KAFKA_CONSUMER_CONFIG_HOME));
		props.put("group.id", privateChannelName);
		this.consumer = new KafkaConsumer<String, String>(props);
		SUBSCRIBE_TO_LIST.add(this.privateChannelName);
		SUBSCRIBE_TO_LIST.add(this.broadCastingGroup);
	}

	@Override
	public void onReceive(Object message) throws Exception {
		if(message instanceof String){
			consumer.subscribe(SUBSCRIBE_TO_LIST);
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records){
				if(record.topic().equals(SUBSCRIBE_TO_LIST.get(0))){
					LOGGER.info("Personal >  {} ", record.value());
				}else{
					LOGGER.info("Broad-casted >  {} ", record.value());
				}
			}
		}else{
			unhandled(message);
		}
	}

	/**
	 * used for subcription
	 */
	@Override
	public void preStart() throws Exception {
		super.preStart();
		CommunicatorUtil.publishEvents(this.broadCastingGroup, this.privateChannelName, producer, true);
		if(CommunicatorUtil.updateConfig(this.configFileLocation, this.privateChannelName, true)){
			CommunicatorUtil.createChannel(this.privateChannelName, this.config.getConfig(Constants.ZK_CONFIG_HOME));
			getContext().system().scheduler().schedule(
					FiniteDuration.create(1, TimeUnit.SECONDS), 
					FiniteDuration.create(1, TimeUnit.SECONDS),
					getContext().self(),
					"consume",
					getContext().dispatcher(),
					ActorRef.noSender()
					);
		}else{
			LOGGER.error("Unable to update configuration so killing this chat...");
			self().tell(PoisonPill.getInstance(), ActorRef.noSender());
		}
	}

	/**
	 * used for unsubscription 
	 */
	@Override
	public void postStop() throws Exception {
		super.postStop();
		CommunicatorUtil.publishEvents(this.broadCastingGroup, this.privateChannelName, producer, false);
		CommunicatorUtil.updateConfig(this.configFileLocation, this.privateChannelName, false);
		LOGGER.info("Thank you for using \"{}\"  channel...", this.privateChannelName);
		this.producer.close();
	}
}
