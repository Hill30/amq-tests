package com.hill30.AMQTests;

import org.eclipse.paho.client.mqttv3.*;

import java.time.LocalDateTime;
import java.util.ArrayList;

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

        System.out.printf("batch 500\nstarted: %s%n", LocalDateTime.now());
        System.out.printf("clientID: %s, Topic: %s, QoS: %s\n", clientID, topicName, args[1]);


        try {

            int i;
            for (i=0; i<10; i++) {

                ArrayList<MqttAsyncClient> clients = new ArrayList<>();

                int j;
                for (j=0; j<500; j++) {
                    MqttAsyncClient client = new MqttAsyncClient(brokerUrl, clientID + "j" + Integer.toString(j), null);
                    client.connect(options);
                    clients.add(client);

                    while(!client.isConnected())
                        Thread.sleep(10);

                    if (QoS >= 0)
                        client.subscribe(topicName + "j" + Integer.toString(j), QoS);

                    System.out.print("connected: " + Integer.toString(i * 500 + j) + "\r");
                }

                Thread.sleep(1000);

                for (j=0; j<500; j++) {

                    MqttAsyncClient client = clients.get(j);

                    client.disconnect();

                    while(client.isConnected())
                        Thread.sleep(10);

                    System.out.print("disconnected: " + Integer.toString(i * 500 + j) + "\r");
                }


            }

        } catch (MqttException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.printf("\nfinished: %s%n", LocalDateTime.now());
    }
}
