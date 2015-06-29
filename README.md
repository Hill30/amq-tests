#ActiveMQ MQTT throughput/capacity tests

## Use case

The use case requires guaraneed message delivery to a big number of clients over an unreliable/slow connection (mobile devices).
 * network protocol: MQTT
 * number of clients tens of thousands (currenlty requested up to 20000)
 * Quality of Service: Exactly Once
 * network traffic: moderate. Expected number of messages per client less than 1000 per day
 * acceptable delivery latency: pretty lax. It is ok if the delay is even a minute or more even if the network is connected at the moment.

## The Environmnet

The ActiveMQ instance is run on a box with 4 CPU with 1GB allocated for JVM OS Windows 7 64 bit

The broker used in testing is ActiveMQ 5.12 snapshot 179. 

The configuration is out of the box with the following changes:
 * The max number of the MQTT connection is increased to 15000
 * The protocol for the MQTT connector is changed to `mqtt+nio`
 * The dedicated task runner is disabled  ( `org.apache.activemq.UseDedicatedTaskRunner=false` )

The MQTT client library used in tests is [PAHO](http://www.eclipse.org/paho/) v 1.0.2

## Test cases

 1. [Basic](MQTTDisconnect)  connect/disconnect test
 2. [Batch sequential](MQTTBatchDisconnect) connect/disconnect test
 3. [Batch parallel](ParallelBatchConnect) connect/disconnect test
 4. [Reconnect sequential](ParallelBatchConnect) test

