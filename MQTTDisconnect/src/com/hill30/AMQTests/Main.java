package com.hill30.AMQTests;

import org.eclipse.paho.client.mqttv3.*;

import java.time.LocalDateTime;

public class Main {

    public static void main(String[] args) {

        args = new String[]{"01", "none"};

        String clientID = "Client" + args[0];
        String topicName = "Topic" + args[0];

        int QoS = -1;
        switch (args[1]) {
            case "0": QoS = 0; break;
            case "1": QoS = 1; break;
            case "2": QoS = 2; break;
        }

        String brokerUrl = "tcp://localhost:1883";

        if (args.length > 2)
            brokerUrl = args[2];

        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(false);

        System.out.printf("started: %s%n", LocalDateTime.now());
        System.out.printf("clientID: %s, Topic: %s, QoS: %s\n", clientID, topicName, args[1]);


        try {

            int i;
            for (i=0; i<100000; i++) {

                MqttAsyncClient client = new MqttAsyncClient(brokerUrl, clientID, null);
                client.connect(options);

                while(!client.isConnected())
                    Thread.sleep(10);

                try {
                    if (QoS >= 0)
                        client.subscribe(topicName, QoS);

                    Thread.sleep(10);

                    client.disconnect();

                } catch (MqttException e) {
                    e.printStackTrace();
                }

                System.out.print("disconnected " + Integer.toString(i) + "\r");

                while(client.isConnected())
                    Thread.sleep(10);
            }

        } catch (MqttException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.printf("\nfinished: %s%n", LocalDateTime.now());
    }
}
