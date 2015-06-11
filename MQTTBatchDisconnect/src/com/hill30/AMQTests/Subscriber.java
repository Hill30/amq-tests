package com.hill30.AMQTests;


import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.eclipse.paho.client.mqttv3.*;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Date;

public class Subscriber {

    public static void main(String[] args) {
        int numberOfLoops = 1;
        int connectionsInBatch = 8000;

        String clientID = "Subscriber" + args[0];
        String topicName = "Topic" + args[0];

        int QoS = -1;
        switch (args[1]) {
            case "0": QoS = 0; break;
            case "1": QoS = 1; break;
            case "2": QoS = 2; break;
        }

        String brokerUrl = "failover:(tcp://localhost:1883)";
        if (args.length > 2)
            brokerUrl = args[2];

        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(false);
        //options.setKeepAliveInterval(60000);
        //options.setConnectionTimeout(60000);
        System.out.printf("batch %d\nstarted: %s%n", connectionsInBatch, LocalDateTime.now());
        System.out.printf("clientID: %s, Topic: %s, QoS: %s\n", clientID, topicName, args[1]);

        try {

            int i;
            for (i=0; i<numberOfLoops; i++) {

                ArrayList<MqttAsyncClient> clients = new ArrayList<>();

                int j;

                for (j=0; j<connectionsInBatch; j++) {
                    MqttAsyncClient client = new MqttAsyncClient(brokerUrl, clientID + "j" + Integer.toString(j), null);
                    client.connect(options);
                    clients.add(client);

                    while(!client.isConnected())
                        Thread.sleep(10);

                    if (QoS >= 0)
                        client.subscribe(topicName + "j" + Integer.toString(j), QoS);

                        client.setCallback(new MqttCallback() {
                            int recivedMessages = 0;

                            @Override
                            public void connectionLost(Throwable throwable) {
                                throwable.printStackTrace();
                            }

                            @Override
                            public void messageArrived(String topic, MqttMessage mqttMessage) throws Exception {
                                recivedMessages++;
                                System.out.println("-------------------------------------------------");
                                System.out.println("| Topic:" + topic);

                                JsonParser parser = new JsonParser();
                                JsonObject o = parser.parse(new String(mqttMessage.getPayload())).getAsJsonObject();

                                long currentTimeMills = (new Date()).getTime();
                                long messageTimeMills =  o.get("timestamp").getAsLong();


                                long diff = currentTimeMills - messageTimeMills;

                                long second = (diff / 1000) % 60;


                                String time = String.format("%02d sec %d mills", second, diff);

                                System.out.println("| Message: " + o.get("clientID") + " | Delivery time: " + time);

                                System.out.println("-------------------RECIVED:"+ recivedMessages + "----------------------------");
                            }

                            @Override
                            public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {

                            }
                        });

                    System.out.print("connected: " + Integer.toString(i * 500 + j) + "\r");
                    Thread.sleep(100);
                }

                Thread.sleep(1000);
                /*for (j=0; j<connectionsInBatch; j++) {
                    MqttAsyncClient client = clients.get(j);
                    client.disconnect();
                    while(client.isConnected())
                        Thread.sleep(10);
                    System.out.print("disconnected: " + Integer.toString(i * 500 + j) + "\r");
                }*/
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
