package com.self.project.services.constants;

/**
 * Constants which are frequenlty used. 
 * @author Puneeth
 * 
 */
public final class Constants {

	// General properties
	public static final String PERIOD = ".";
	public static final String COMMUNICATOR_HOME = "communicator"+PERIOD;
	public static final String CLOSE_BRACKET = "]";
	
	//Zoo Keeper properties
	public static final String ZK_CONFIG_HOME = COMMUNICATOR_HOME+"zkClient";
	public static final String HOST_PORT_KEY = "host-port";
	public static final String ZK_SESSION_TIMEOUT_KEY = "sessionTimeoutMs";
	public static final String ZK_SYNS_TIME_KEY = "connectionTimeoutMs";
	
	// Producer properties
	public static final String PRODUCER_CONFIGURATION = "producer-config";
	public static final String KAFKA_PRODUCER_CONFIG_HOME = COMMUNICATOR_HOME+PRODUCER_CONFIGURATION;
	
	// Consumer properties
	public static final String KAFKA_CONSUMER_CONFIGURATION = COMMUNICATOR_HOME+"consumer";
	public static final String CONFIG = "config";
	public static final String KAFKA_CONSUMER_CONFIG_HOME = KAFKA_CONSUMER_CONFIGURATION+PERIOD+CONFIG;
	public static final String CONSUEMR_GROUP_LIST = KAFKA_CONSUMER_CONFIGURATION+PERIOD+"consumer-names";
	public static final String BROAD_CAST_KEY = "broadcast-channel";
	/**
	 * A list of availble consumers( currently available ). 
	 */
	public static final String BROAD_CAST_VALUE = KAFKA_CONSUMER_CONFIGURATION+PERIOD+BROAD_CAST_KEY;


}
