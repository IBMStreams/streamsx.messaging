##README --  IBMStreams/streamsx.messaging

The IBMStreams/streamsx.messaging toolkit project is an open source Streams toolkit project focused on the development of operators and functions that extend IBM InfoSphere Streams ability to interact with messaging systems.

Messaging Toolkit v2.0 is offically released to support InfoSphere Streams v4.0:
* https://github.com/IBMStreams/streamsx.messaging/releases

Check out our newest operators KafkaProducer and KafkaConsumer!
* http://ibmstreams.github.io/streamsx.messaging/com.ibm.streamsx.messaging/doc/spldoc/html/tk$com.ibm.streamsx.messaging/ns$com.ibm.streamsx.messaging.kafka.html

To get started with working with this toolkit:
* https://github.com/IBMStreams/streamsx.messaging/wiki/Getting-Started-with-Toolkit-Development


To buil this toolkit, you will need to first get the Kafka Pre-release 0.9 jar using the following steps:
    At the command line, navigate to your GIT directory.
    To clone the Apache Kafka GitHub repository and ensure that the right version of the code is used, run the following commands:

    git clone https://github.com/apache/kafka.git kafka 
    cd kafka 
    git checkout f1110c3fbb166f94204b6bb18bc4e1a9100d3c4e 

    Run the following commands:
        Linux:

        gradle 
        ./gradlew clean && ./gradlew clients:build

    Navigate to clients/build/libs/kafka-clients-0.9.0.0-SNAPSHOT.jar and copy it into your library directory.
    
    Rename kafka-clients-0.9.0.0-SNAPSHOT.jar to kafka-clients-0.9.0.0.jar and move this file to the toolkit directory so that the resulting path is com.ibm.streamsx.messaging/opt/kafka-clients-0.9.0.0.jar. The jar is now visible to the toolkit.
    Use ant to build.
