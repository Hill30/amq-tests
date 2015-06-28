package com.hill30.AMQTests;

import org.eclipse.paho.client.mqttv3.*;

import java.time.LocalDateTime;
import java.util.ArrayList;

public class Main {

    public static void main(String[] args) {

        /**********************************************
         * Assign test parameter values to the variables below
         */

        String brokerUrl = "tcp://10.211.55.5:1883";
        //String brokerUrl = "tcp://localhost:1883";

        String clientID = "Client01";
        String topicName = "Topic01";

        // Quality of Service
        int QoS = -1;
        // Quality of Service values:
        // 0 - at most once
        // 1 - at least once
        // 2 - exactly once
        // if QoS is set to -1, subscribe will be skipped

        int batchSize = 10000;
        int repetitions = 1;

        /*
         **********************************************/

        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(false);

        System.out.printf("batch size %d\nstarted: %s%n", batchSize, LocalDateTime.now());
        System.out.printf("clientID: %s, Topic: %s, QoS: %d\n", clientID, topicName, QoS);


        try {

            int i;
            for (i=0; i<repetitions; i++) {

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
