package com.hill30.AMQTests;

import org.eclipse.paho.client.mqttv3.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.Scanner;

public class Main {

    public static void main(String[] args) {

        args = new String[]{"01", "none"};

        String clientID = "Client" + args[0];
        String topicName = "Topic" + args[0];

        int batchSize = 10000;

        int QoS = 2;
        switch (args[1]) {
            case "0": QoS = 0; break;
            case "1": QoS = 1; break;
            case "2": QoS = 2; break;
        }

        String brokerUrl = "tcp://10.211.55.5:1883";

        if (args.length > 2)
            brokerUrl = args[2];

        System.out.printf("batch size %d\nstarted: %s%n", batchSize, LocalDateTime.now());
        System.out.printf("clientID: %s, Topic: %s, QoS: %s\n", clientID, topicName, args[1]);

        Runner runner = new Runner(batchSize, brokerUrl, clientID, topicName, QoS);

        Thread runnerThread = new Thread(runner);
        runnerThread.start();

        try{
            BufferedReader br =
                    new BufferedReader(new InputStreamReader(System.in));

            String input;

            while((input=br.readLine())!=null){
                System.out.println("Executing " + input);
                runner.Submit(input);
            }

        }catch(IOException io){
            io.printStackTrace();
        }


        System.out.printf("\nfinished: %s%n", LocalDateTime.now());
    }
}
