package com.hill30.AMQTests;

import org.eclipse.paho.client.mqttv3.*;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Date;

public class Main {

    public static void main(String[] args) {

        args = new String[]{"01", "none"};

        String clientID = "Client" + args[0];
        String topicName = "Topic" + args[0];

        int batchSize = 10000;

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

        System.out.printf("batch size %d\nstarted: %s%n", batchSize, LocalDateTime.now());
        System.out.printf("clientID: %s, Topic: %s, QoS: %s\n", clientID, topicName, args[1]);


        try {

            int i;
            for (i=0; i<1; i++) {

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
                        checked = adapters.get(j).IsConnected() || adapters.get(j).IsDisconnected();
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

                for (j=0; j<batchSize; j++) {
                    Thread.sleep(10);
                    adapters.get(j).Disconnect();
                    System.out.printf("Disconnected %d\r", j + 1);
                    //System.out.print("disconnected: " + Integer.toString(i * batchSize + j + 1) + "\r");
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
