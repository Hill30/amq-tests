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

        System.out.printf("single\nstarted: %s%n", LocalDateTime.now());
        System.out.printf("clientID: %s, Topic: %s, QoS: %s\n", clientID, topicName, args[1]);


        try {

            int i;
            for (i=0; i<5000; i++) {

                MqttAsyncClient client = new MqttAsyncClient(brokerUrl, clientID, null);
                client.connect(options);

                while(!client.isConnected())
                    Thread.sleep(10);

                try {
                    if (QoS >= 0)
                        client.subscribe(topicName, QoS);

                    client.disconnect();

                } catch (MqttException e) {
                    e.printStackTrace();
                }

                // disconnect takes some time on the server, but the isConnected() returns false right away
                // let us wait until the connection is really closed before we try to reconnect
                Thread.sleep(10); // the 10ms is completely off the wall - no idea how to be sure that the disconnect really happened

                System.out.print("disconnected " + Integer.toString(i+1) + "\r");

            }

        } catch (MqttException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.printf("\nfinished: %s%n", LocalDateTime.now());
    }
}
