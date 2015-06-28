package com.hill30.AMQTests;

import org.eclipse.paho.client.mqttv3.*;

import java.time.LocalDateTime;

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

            int repetitions = 10000;

        /*
         **********************************************/


        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(false);

        System.out.printf("single\nstarted: %s%n", LocalDateTime.now());
        System.out.printf("clientID: %s, Topic: %s, QoS: %d\n", clientID, topicName, QoS);

        try {

            int i;
            for (i=0; i<repetitions; i++) {

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
