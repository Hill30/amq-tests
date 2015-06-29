package com.hill30.AMQTests;

import org.eclipse.paho.client.mqttv3.*;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Date;

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

        int repetitions = 1;
        int batchSize = 10000;

        /*
         **********************************************/


        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(false);

        System.out.printf("batch size %d\nstarted: %s%n", batchSize, LocalDateTime.now());
        System.out.printf("clientID: %s, Topic: %s, QoS: %d\n", clientID, topicName, QoS);


        try {

            int i;
            for (i=0; i<repetitions; i++) {

                Date start = new Date();
                ArrayList<ConnectionAdapter> adapters = new ArrayList<>();
                int j;
                for (j=0; j<batchSize; j++) {
                    adapters.add(
                            new ConnectionAdapter(
                                    brokerUrl,
                                    clientID + "j" + Integer.toString(j),
                                    topicName + "j" + Integer.toString(j),
                                    QoS, options
                            ));
                }

                for (j=0; j<batchSize; j++) {
                    adapters.get(j).Connect();
                    System.out.printf("Connected %d\r", (j + 1));
                }
//                    //System.out.print("connected: " + Integer.toString(i * batchSize + j + 1) + "\r");

                System.out.printf("\nConnects initiated in %d msec\n", new Date().getTime() - start.getTime());

                start = new Date();
                boolean checked = false;
                while (!checked) {
                    for (j=0; j<batchSize; j++) {
                        checked = adapters.get(j).IsConnected() || adapters.get(j).IsAborted();
                        if (!checked)
                            break;
                    }
                    if (!checked) {
                        Thread.sleep(10);
                        continue;
                    }
                    break;
                }

                System.out.printf("Waiting for pending connects... - done in %d msec\n", new Date().getTime() - start.getTime());

                start = new Date();
                for (j=0; j<batchSize; j++) {
                    Thread.sleep(10);
                    adapters.get(j).Disconnect();
                    System.out.printf("Disconnected %d\r", j + 1);
                    //System.out.print("disconnected: " + Integer.toString(i * batchSize + j + 1) + "\r");
                }
                System.out.printf("\nDisconnects initiated in %d msec\n", new Date().getTime() - start.getTime());

                start = new Date();
                boolean disconnected = false;
                while (!disconnected) {
                    for (j=0; j<batchSize; j++) {
                        disconnected = !adapters.get(j).IsConnected();
                        if (!disconnected)
                            break;
                    }
                    if (!disconnected) {
                        Thread.sleep(10);
                        continue;
                    }
                    break;
                }

                System.out.printf("Waiting for pending disconnects... - done in %d msec\n", new Date().getTime() - start.getTime());

            }

        } catch (MqttException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.printf("\nfinished: %s%n", LocalDateTime.now());
    }
}
