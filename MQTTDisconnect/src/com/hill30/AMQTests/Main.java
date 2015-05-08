package com.hill30.AMQTests;

import org.eclipse.paho.client.mqttv3.*;

import java.util.logging.Logger;

public class Main {

    public static void main(String[] args) {

        String brokerUrl = "tcp://localhost:1883";
        String clientID = "Client04";
        String topicName = "topic03";
        int QoS = 0;

        Logger logger = Logger.getLogger("main");
        logger.info("hello");

        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(false);

        try {

            int i;
            for (i=0; i<100000; i++) {

                MqttAsyncClient client = new MqttAsyncClient(brokerUrl, clientID, null);
                client.connect(options);

                while(!client.isConnected())
                    Thread.sleep(10);

                try {
                    //if (i==0)
                        client.subscribe(topicName, QoS);

                    client.disconnect();

                } catch (MqttException e) {
                    e.printStackTrace();
                }

                logger.info("disconnected " + Integer.toString(i) + "\r");

                while(client.isConnected())
                    Thread.sleep(10);
            }

        } catch (MqttException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
