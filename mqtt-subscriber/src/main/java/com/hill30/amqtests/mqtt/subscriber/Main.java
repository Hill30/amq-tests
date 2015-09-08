package com.hill30.amqtests.mqtt.subscriber;

import java.time.LocalDateTime;

public class Main {

    public static void main(String[] args) {

        if (args.length < 3) {
            return;
        }

        /**********************************************
         * Assign test parameter values to the variables below
         */

        int batchSize = 10;
        int index = 1;
        boolean isPublisher = false;

        String brokerUrl = "" ;

        if (args.length < 3) {
            System.out.println("Wrong params");
        } else {
            if (args[0] != null) {
                brokerUrl = args[0];
            }

            if (args[1] != null) {
                batchSize = Integer.parseInt(args[1]);
            }
            if (args[2] != null) {
                index = Integer.parseInt(args[2]);
            }
        }

        String clientID = "C"; // + index;
        String topicName = "T/";

        // Quality of Service
        int QoS = 1;
        // Quality of Service values:
        // 0 - at most once
        // 1 - at least once
        // 2 - exactly once
        // if QoS is set to -1, subscribe will be skipped


        /*
         **********************************************/

        if (isPublisher) {
            System.out.printf("PUBLISHER - %d\n", index);

        } else {
            System.out.printf("SUBSCRIBER\n", index);
        }
        System.out.printf("Broker URL: %s \n", brokerUrl);
        System.out.printf("Batch size %d\nstarted: %s%n", batchSize, LocalDateTime.now());
        System.out.printf("clientID: %s\n", clientID);
        System.out.printf("Topic: %s \n", topicName);
        System.out.printf("QoS: %s\n",  QoS);

        Runner runner = new Runner(batchSize, brokerUrl, clientID, topicName, QoS, 0, isPublisher, index);

        Thread runnerThread = new Thread(runner);
        runnerThread.start();

        try {
            runnerThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.printf("\nfinished: %s%n", LocalDateTime.now());
    }
}
