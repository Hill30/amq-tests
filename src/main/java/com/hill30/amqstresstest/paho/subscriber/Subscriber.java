package com.hill30.amqstresstest.paho.subscriber;

import org.eclipse.paho.client.mqttv3.*;

import java.util.Date;
import java.util.logging.Level;

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
        this.serviceUri = "tcp://" + host + ":" + port;
    }

    public void disconnect() {
        try {
            mqtt.disconnect();
        } catch (Exception ex) {
            ex.printStackTrace();
            SubscriberRunnner.logger.log(Level.ALL, ex.getMessage(), ex);
        }
    }


    @Override
    public void run() {
        System.out.println("Subscribing to topic: " + topic());

        try {
            mqtt = new MqttClient(serviceUri, topic());
            MqttConnectOptions connOpts = new MqttConnectOptions();
            if(user != null && !user.isEmpty()) {
                connOpts.setUserName(user);
            }
            connOpts.setCleanSession(false);
            if(password != null && !password.isEmpty()) {
                connOpts.setPassword(password.toCharArray());
            }

            mqtt.setCallback(new MqttCallback() {
                public void connectionLost(Throwable cause) {
                    SubscriberRunnner.logger.info((new Date()) + " " + topicId + " disconnected");
                }

                public void messageArrived(String topic, MqttMessage message) {
                    if(topic != null && message != null) {
                        SubscriberRunnner.logger.info("Message from " + topic + " arrived: " + message.toString());
                    }
                }

                public void deliveryComplete(IMqttDeliveryToken token) {
                }
            });

            mqtt.connect(connOpts);

            synchronized (Subscriber.class) {
                isWorking = true;
                while (isWorking)
                    Subscriber.class.wait();
            }

        } catch (MqttException e) {
            e.printStackTrace();
            SubscriberRunnner.logger.log(Level.ALL, e.getMessage(), e);
        } catch (InterruptedException e) {
            e.printStackTrace();
            SubscriberRunnner.logger.log(Level.ALL, e.getMessage(), e);
        }
    }

    public void finish() {
        isWorking = false;
    }

    private String topic() {
        return topicPerfix + topicId;
    }
}
