package com.hill30.amqtests.openwire.publisher;

import org.apache.activemq.ActiveMQSslConnectionFactory;

import javax.jms.*;
import java.util.ArrayList;
import java.util.Arrays;

public class SyncPublisher implements Runnable {

    private  String BROKER_HOST = "ssl://10.0.1.55:%d";
    private  int BROKER_PORT = 62616;
    private  String BROKER_URL = String.format(BROKER_HOST, BROKER_PORT);
    private  Boolean NON_TRANSACTED = false;

    private  int NUM_MESSAGES_TO_SEND = 500;
    private  int NUM_OF_TOPICS = 20000;
    private  String TOPIC_NAME = "Topic";

    private  int index = 1;
    private  boolean isConnected = false;
    private  ActiveMQSslConnectionFactory connectionFactory;
    private  Connection c = null;
    private  int sent = 0;
    private String verb = "";

    public SyncPublisher() {

    }

    @Override
    public void run() {

        if (index < 1) {
            index = 1;
        }

        if (index > NUM_OF_TOPICS) {
            index = NUM_OF_TOPICS;
        }

        final int offset = (index - 1) * NUM_OF_TOPICS;
        final int limit = index * NUM_OF_TOPICS;

        connectionFactory  = new ActiveMQSslConnectionFactory(BROKER_URL);
        connectionFactory.setUserName("admin");
        connectionFactory.setPassword("admin");

        connectionFactory.setKeyStore("file:///C:/Downloads/certs/client.ks");
        connectionFactory.setKeyStorePassword("password");
        connectionFactory.setTrustStore("file:///C:/Downloads/certs/client.ts");
        connectionFactory.setTrustStorePassword("password");
        long unixTime = System.currentTimeMillis() / 1000L;

        /*ArrayList<Integer> pids = new ArrayList<>(Arrays.asList(1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000));
        long threadId = Thread.currentThread().getId()%100 + 1;
        for (int i = 0; i < NUM_MESSAGES_TO_SEND; i++) {
            verb = "Starting...";
            pids.forEach(j -> {
                publishToTopic(TOPIC_NAME + ".Out." + "j" + j, "message" + j + " | " + threadId);
                sent++;
                verb = "Sending...";
                publishToQueue(TOPIC_NAME + ".overall." + unixTime, "message" + j + " | " + threadId);
            });
        }*/

        long threadId = Thread.currentThread().getId()%100 + 1;
        for (int i = 0; i < NUM_MESSAGES_TO_SEND; i++) {
            for (int j = offset; j <= limit-1; j++) {
                publishToTopic(TOPIC_NAME + ".j" + j, "message" + j + " | " + threadId);
                sent++;
                verb = "Sending...";
                publishToQueue(TOPIC_NAME + ".overall." + unixTime, "message" + j + " | " + threadId);
            }
        }

        try {
            if (c != null) {
                c.close();
            }
        } catch (JMSException e) {
            e.printStackTrace();
        }

    }

    public  void publishToTopic(String dest, String msg) {
        try {
            establishConnection();
            if (c != null) {
                Session session = c.createSession(NON_TRANSACTED, Session.AUTO_ACKNOWLEDGE);
                Destination destination = session.createTopic(dest);
                MessageProducer producer = session.createProducer(destination);
                producer.setDeliveryMode(DeliveryMode.PERSISTENT);
                TextMessage message = session.createTextMessage(msg);
                producer.send(message);
                producer.close();
                session.close();
            }
        } catch (JMSException e) {
            verb = "Failed while sending to topic" + dest + " ...";
            //printStats("Failed while sending to topic" + dest + " ...");

            isConnected = false;
            publishToTopic(dest, msg);
        }
    }

    public  void publishToQueue(String dest, String msg) {
        try {
            establishConnection();
            if (c != null) {
                Session session = c.createSession(NON_TRANSACTED, Session.AUTO_ACKNOWLEDGE);
                Destination destination = session.createQueue(dest);
                MessageProducer producer = session.createProducer(destination);
                producer.setDeliveryMode(DeliveryMode.PERSISTENT);
                TextMessage message = session.createTextMessage(msg);
                producer.send(message);
                producer.close();
                session.close();
            }
        } catch (JMSException e) {
            verb = "Failed while sending to queue" + dest + "...";
            //printStats("Failed while sending to queue" + dest + "...");

            isConnected = false;
            publishToTopic(dest, msg);
        }
    }


    public void delay(long mills) {
        try {
            Thread.sleep(mills);
        } catch (InterruptedException e1) {
            establishConnection();
        }
    }

    public void establishConnection() {
        try {
            while (!isConnected) {
                c = connectionFactory.createConnection();
                c.start();
                isConnected = true;

                verb = "Connected...";
                //printStats("Connected...");
            }

        } catch (JMSException e) {

            verb = "Reconnecting...";
            //printStats("Reconnecting...");

            isConnected = false;
            establishConnection();
        }
    }


    public  void printStats() {
        System.out.println(verb + " \t\t\t| BROKER: " + BROKER_URL + "; Topic prefix: " +  TOPIC_NAME +  "; Topics: "  + NUM_OF_TOPICS  +  "; Messages to send: "  +  NUM_MESSAGES_TO_SEND +   "; Sent: " + sent);
    }

}
