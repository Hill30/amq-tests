package com.hill30.amqstresstest.paho.subscriber;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.util.Date;
/*
public class Subscriber extends Thread {

    private String topicPerfix;
    private int topicId;
    private String user;
    private String password;
    private String host;
    private int port;
    private boolean isWorking;
    private String serviceUri;
    private MqttClient mqtt;

    public Subscriber(String topicPrefix, int topicId, String user, String password, String host, int port) {
        this.topicPerfix = topicPrefix;
        this.topicId = topicId;
        this.user = user;
        this.password = password;
        this.host = host;
        this.port = port;
    }

    public void disconnect() {
        try {
            mqtt.disconnect();
        } catch (MqttException ex) {
            System.out.println("Disconnect failed for: " + topic());
        }
    }


    @Override
    public void run() {
        System.out.println("Subscribing to topic: " + topic());

        try {
            MqttClient mqtt = new MqttClient(serviceUri, topic());
            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setUserName(user);
            connOpts.setCleanSession(false);
            connOpts.setPassword(password.toCharArray());

            mqtt.setCallback(new MqttCallback() {
                public void connectionLost(Throwable cause) {
                    System.out.println((new Date()) + " " + topicId + " disconnected");
                }

                public void messageArrived(String topic, MqttMessage message) {
                }

                public void deliveryComplete(IMqttDeliveryToken token) {
                }
            });

            synchronized (Subscriber.class) {
                isWorking = true;
                while(isWorking)
                    Subscriber.class.wait();
            }

        } catch (MqttException e) {
            e.printStackTrace();
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void finish(){
        isWorking = false;
    }

    private String topic(){
        return topicPerfix+topicId;
    }
    */