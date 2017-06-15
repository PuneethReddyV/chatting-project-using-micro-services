Strart Zoo Keeper 
-----------------
cd ZOO_KEEPER_HOME  <br />
bin/zkServer.sh start <br />

Start Kafka
------------
cd KAFKA_HOME  <br />
bin/kafka-server-start.sh config/server.properties <br />

sample command create topic on kafka server
-------------------------------------------
cd KAFKA_HOME <br />
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic talk-to-client1 <br />

Build this project using Maven
-----------------------------
cd PROJECT_HOME( where you find pom.xml ) <br />
mvn -X clean install <br />
cd PROJECT_HOME/target/communicator/lib <br />

Use Producer class of this jar
-----------------------------
java -Dlogback.configurationFile="../conf/logback.xml" -cp MicroServices-0.0.1-SNAPSHOT.jar com.self.project.micro.services.producers.Producers ../conf <br />

Use Consumer Actors of this jar
-------------------------------
java -Dlogback.configurationFile="../conf/logback.xml" -cp MicroServices-0.0.1-SNAPSHOT.jar com.self.project.micro.services.actors.consumers.Consumers ../conf/application.conf TOPIC_NAME <br />


Use Consumer Class of this jar
------------------------------
java -Dlogback.configurationFile="../conf/logback.xml" -cp MicroServices-0.0.1-SNAPSHOT.jar com.self.project.micro.services.actors.consumers.Consumers ../conf/application.conf TOPIC_NAME <br />

