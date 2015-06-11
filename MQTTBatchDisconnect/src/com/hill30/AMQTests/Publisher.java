package com.hill30.AMQTests;

import org.eclipse.paho.client.mqttv3.*;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.Random;

public class Publisher {

    public static int randInt(int min, int max) {
        Random rand = new Random();
        int randomNum = rand.nextInt((max - min) + 1) + min;
        return randomNum;
    }

    public static void main(String[] args) {
        //args = new String[]{"01", "none"};
        int numberOfLoops = 2000;


        String clientID = "Publisher" + args[0];
        String topicName = "Topic" + args[0];

        int QoS = -1;
        switch (args[1]) {
            case "0": QoS = 0; break;
            case "1": QoS = 1; break;
            case "2": QoS = 2; break;
        }

        String brokerUrl = "tcp://localhost:1883";

        System.out.printf("messages %d\nstarted: %s%n", numberOfLoops, LocalDateTime.now());
        System.out.printf("clientID: %s, Topic: %s, QoS: %s\n", clientID, topicName, args[1]);

        if (args.length > 2)
            brokerUrl = args[2];

        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(false);
        //options.setKeepAliveInterval();

        try {

            int i;

            for (i=0; i<numberOfLoops; i++) {

                int j = randInt(0, numberOfLoops-1);

                MqttAsyncClient client = new MqttAsyncClient(brokerUrl, clientID + "j" + Integer.toString(j), null);
                client.connect(options);

                while(!client.isConnected())
                    Thread.sleep(10);

                if (QoS >= 0) {

                    MqttMessage msg = new MqttMessage();
                    String json = "{\"clientID\": " + clientID + "j" + Integer.toString(j) + ",\"timestamp\":" + (new Date()).getTime() + "}";
                    msg.setPayload(json.getBytes());
                    msg.setQos(QoS);

                    client.publish(topicName + "j" + Integer.toString(j), msg);
                    System.out.print("sent: " + Integer.toString(i) + "\r");
                    Thread.sleep(1000);
                }

                Thread.sleep(100);
            }

        } catch (MqttException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.printf("\nfinished: %s%n", LocalDateTime.now());
        try {
            System.in.read();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
