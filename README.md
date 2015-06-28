# A collection of ActiveMQ MQTT connector throughput/capacity tests

## Use case

The use case requires guaraneed message delivery to a big number of clients over an unreliable/slow connection (mobile devices).
 * network protocol: MQTT
 * number of clients tens of thousands (currenlty requested up to 20000)
 * Quality of Service: Exactly Once
 * network traffic: moderate. Expected number of messages per client less than 1000 per day
 * acceptable delivery latency: pretty lax. It is ok if the delay is even a minute or more even if the network is connectet at the moment.

## The Environmnet

The broker used in testing is ActiveMQ 5.12 snapshot 179. 

The configuration is out of the box with the following changes:
 * The max number of the MQTT connection is increased to 15000
 * The protocol for the MQTT connector is changed to `mqtt+nio`
 * The dedicated task runner is disabled  ( `org.apache.activemq.UseDedicatedTaskRunner=false` )

The ActiveMQ instance is run on a box with 4 CPU with 1GB allocated for JVM

The MQTT client library used in tests is [PAHO](http://www.eclipse.org/paho/) v 1.0.2

## Test cases

### 1. Basic [connect/disconnect](MQTTDisconnect/) test

####Test case description:

 * connects to a topic with a requested QoS
 * waits for the connection to be established
 * subscribes to recieve messages from the topic (if requested)
 * disconnects from the topic

The test is repeated the requested number of times.
####Test parameters
 * broker url
 * topic name
 * client name
 * Quality of service
 * repeat counter
####Running instructions

HOW TO RUN

It uses fusesource mqtt client (https://github.com/fusesource/mqtt-client)

Example:
\out\artifacts\AMQStressTest_jar>java -jar AMQStressTest.jar 1000 100 "127.0.0.1" "/prefix_" "" "" 30

first argument - number of subscribers = 1000
second argument - number of connect/disconnect cycles = 100
third argument - hostname = "127.0.0.1"
forth argument - client and topic prefix = "/prefix_"
fifth argument - user id = ""
sixth argument - user password = ""
seventh argument - keep alive timeout = 30

You can also run mqtt_conncect-disconnect\out\artifacts\AMQStressTest_jar\AMQStressTest.jar like java -jar AMQStressTest_jar
Client would try to open 1000 connections wait for a few seconds and close it. Then it will try it again One thousand times. Until it would fail either way: from server or client side.
