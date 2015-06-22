package com.hill30.AMQTests;

import org.eclipse.paho.client.mqttv3.*;

import java.time.LocalDateTime;
import java.util.ArrayList;

public class Main {

    public static void main(String[] args) {

        args = new String[]{"01", "none"};
//        String ip = "10.37.129.2";
        String ip = "localhost";

        String clientID = "Client" + args[0];
        String topicName = "Topic" + args[0];

        int batchSize = 10000;

        int QoS = -1;
        switch (args[1]) {
            case "0": QoS = 0; break;
            case "1": QoS = 1; break;
            case "2": QoS = 2; break;
        }

        String brokerUrl = "tcp://"+ip+":1883";

        if (args.length > 2)
            brokerUrl = args[2];

        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(false);

        System.out.printf("batch size %d\nstarted: %s%n", batchSize, LocalDateTime.now());
        System.out.printf("clientID: %s, Topic: %s, QoS: %s\n", clientID, topicName, args[1]);


        try {

            int i;
            for (i=0; i<1; i++) {

                ArrayList<MqttAsyncClient> clients = new ArrayList<>();

                int j;
                for (j=0; j<batchSize; j++) {
                    MqttAsyncClient client = new MqttAsyncClient(brokerUrl, clientID + "j" + Integer.toString(j), null);

                    try {
                        client.connect(options).waitForCompletion();
                    } catch (MqttException e) {
                        System.out.println("\nConnect for " + clientID + "j" + Integer.toString(j)+ " failed " + e.toString() + "\n");
                        continue;
                    }

                    clients.add(client);

                    if (QoS >= 0)
                        client.subscribe(topicName + "j" + Integer.toString(j), QoS);

                    System.out.printf("connected: pass %2d #%4d\r",i+1 ,j+1);
                }

                for (j=0; j<batchSize; j++) {

                    MqttAsyncClient client = clients.get(j);

                    client.disconnect();

                    System.out.printf("disconnected: pass %2d #%4d\r", i+1, j+1);
                }


            }

        } catch (MqttException e) {
            e.printStackTrace();
        }

        System.out.printf("\nfinished: %s%n", LocalDateTime.now());
    }
}
