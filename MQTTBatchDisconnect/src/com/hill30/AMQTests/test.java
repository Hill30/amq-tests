package com.hill30.AMQTests;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.eclipse.paho.client.mqttv3.*;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.Random;

public class test {
    private static int receivedMessages = 0;

    private static int numberOfLoops = 1;
    private static int connectionsInBatch = 5000;
    private static String brokerUrl = "failover:(tcp://localhost:1883)";

    private static String clientID = "Subscriber";
    private static String topicName = "Topic";

    private static int QoS = -1;

    public static void main(String[] args) {
        SubscribeRunner runner = new SubscribeRunner();
        try {
            runner.run(args);
        } catch (MqttException e) {
            System.err.println("Runner exception " + e.getMessage());
        }
    }
    private static Random rand = new Random();

    public static int randInt(int min, int max) {
        return  rand.nextInt((max - min) + 1) + min;
    }

    public static class SubscribeRunner  {


        private void constantlyReconnect(MqttAsyncClient client, int j){
            while (true){
                try {
                    int timeout = randInt(2000, 30000);//10*6*1000 + (j*100);
                    System.out.print("Before reconnecting: " + timeout + "\r\n");
                    try { Thread.sleep(timeout); } catch (InterruptedException e) {}
                    System.out.print("Reconnecting: " + topicName + "j" + Integer.toString(j) + "\r\n");
                    client.connect();

                    for (int i = 0; i < 50; i++){
                        if (!client.isConnected()) {
                            pause();
                        } else {
                            break;
                        }
                    }

                    if(!client.isConnected())
                        continue;

                    return;
                } catch (Exception e) {
                    if(client.isConnected())
                        return;
                    System.out.println("Unable to reconnect" + topicName + "j" + Integer.toString(j) + "\r\n");
                }
            }
        }

        public void connect(int j) throws MqttException {
            try {
                MqttConnectOptions options = new MqttConnectOptions();
                options.setCleanSession(false);
                options.setKeepAliveInterval(10);
                options.setConnectionTimeout(10);

                final MqttAsyncClient client = new MqttAsyncClient(brokerUrl, clientID + "j" + Integer.toString(j), null);
                client.setCallback(new MqttCallback() {
                    @Override
                    public void connectionLost(Throwable throwable) {
                        constantlyReconnect(client, j);
                        System.out.print("Reconnected: " + topicName + "j" + Integer.toString(j) + "\r");
                    }

                    @Override
                    public void messageArrived(String topic, MqttMessage mqttMessage) throws Exception {
                        receivedMessages++;
                        System.out.println("-------------------------------------------------");
                        System.out.println("| Topic:" + topic + "");

                        JsonParser parser = new JsonParser();
                        JsonObject o = parser.parse(new String(mqttMessage.getPayload())).getAsJsonObject();

                        long currentTimeMills = (new Date()).getTime();
                        long messageTimeMills = o.get("timestamp").getAsLong();
                        long diff = currentTimeMills - messageTimeMills;
                        long second = (diff / 1000) % 60;


                        String time = String.format("%02d sec %d mills", second, diff);

                        System.out.println("| Message: " + o.get("clientID") + " | Delivery time: " + time);
                        System.out.println("-------------------RECEIVED:" + receivedMessages + "----------------------------");
                    }

                    @Override
                    public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {

                    }
                });

                client.connect(options);

                while (!client.isConnected()) {
                    pause();
                }
                if (QoS >= 0)
                    client.subscribe(topicName + "j" + Integer.toString(j), QoS);
            } catch (Exception e){
                System.out.println("Excption " + e.getMessage());
            }

        }

        private void pause() {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                // Error handling goes here...
            }
        }

        public void run(String[] args) throws MqttException {
            switch (args[1]) {
                case "0": QoS = 0; break;
                case "1": QoS = 1; break;
                case "2": QoS = 2; break;
            }

            if (args.length > 2)
                brokerUrl = args[2];

            System.out.printf("batch %d\nstarted: %s%n", connectionsInBatch, LocalDateTime.now());

            try {

                int i;
                for (i=0; i<numberOfLoops; i++) {
                    for (int j=0; j<connectionsInBatch; j++) {
                        try {
                            connect(j);
                            System.out.print("connected: " + Integer.toString(i * 500 + j) + "\r");
                            Thread.sleep(100);
                        } catch (Exception e){
                            System.err.println("Exception on connect " + e.getMessage());
                            //Thread.sleep(2000);
                            connect(j);
                        }
                    }
                    Thread.sleep(1000);
                }
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


}
