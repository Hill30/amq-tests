package com.hill30.AMQTests;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
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
        int QoS = 2;
        // Quality of Service values:
        // 0 - at most once
        // 1 - at least once
        // 2 - exactly once
        // if QoS is set to -1, subscribe will be skipped

        //int repetitions = 10000;
        int batchSize = 10000;

        /*
         **********************************************/

        System.out.printf("batch size %d\nstarted: %s%n", batchSize, LocalDateTime.now());
        System.out.printf("clientID: %s, Topic: %s, QoS: %d\n", clientID, topicName, QoS);

        Runner runner = new Runner(batchSize, brokerUrl, clientID, topicName, QoS);

        Thread runnerThread = new Thread(runner);
        runnerThread.start();

        try{
            BufferedReader br =
                    new BufferedReader(new InputStreamReader(System.in));

            String input;

            while((input=br.readLine())!=null){
                runner.Submit(input);
            }

            runner.stop();

        }catch(IOException io){
            io.printStackTrace();
        }


        System.out.printf("\nfinished: %s%n", LocalDateTime.now());
    }
}
