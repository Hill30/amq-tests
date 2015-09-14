package com.hill30.amqtests.mqtt.subscriber;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.time.LocalDateTime;
import java.util.Properties;

public class Main {
    public static void main(String[] args) {

        /**********************************************
         * Assign test parameter values to the variables below
         */

        int batchSize = 10000;
        int index = 2;
        String brokerUrl = "ssl://10.0.1.55:8883" ;
        String clientID = "C";
        String topicName = "T/";


        Properties props = new Properties();

        // Quality of Service

        // Quality of Service values:
        // 0 - at most once
        // 1 - at least once
        // 2 - exactly once
        // if QoS is set to -1, subscribe will be skipped
        int QoS = 1;

        System.out.println("Working Directory = " + System.getProperty("user.dir"));

        File configFile = new File("config.xml");

        if(configFile.exists() && !configFile.isDirectory()) {

            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = null;

            try {
                dBuilder = dbFactory.newDocumentBuilder();
                Document doc = dBuilder.parse(configFile);

                doc.getDocumentElement().normalize();

                if (doc.getElementsByTagName("mongodb").getLength() > 0) {
                    Node mongoNode = doc.getElementsByTagName("mongodb").item(0);
                    if (mongoNode.getNodeType() == Node.ELEMENT_NODE) {
                        Element eElement = (Element) mongoNode;


                        props.setProperty("mongoHost", eElement.getAttribute("host"));
                        props.setProperty("mongoPort", eElement.getAttribute("port"));

                        System.out.println(props.getProperty("mongoHost"));
                        System.out.println(props.getProperty("mongoPort"));

                    }
                }

                if (doc.getElementsByTagName("broker").getLength() > 0) {
                    Node mongoNode = doc.getElementsByTagName("broker").item(0);
                    if (mongoNode.getNodeType() == Node.ELEMENT_NODE) {
                        Element mongo = (Element) mongoNode;
                        System.out.println("Protocol: " + mongo.getAttribute("protocol"));


                        if (mongo.getAttribute("protocol").equals("ssl")) {

                            if (doc.getElementsByTagName("ssl").getLength() > 0) {
                                Node sslNode = doc.getElementsByTagName("ssl").item(0);
                                if (mongoNode.getNodeType() == Node.ELEMENT_NODE) {
                                    Element ssl = (Element) sslNode;

                                    System.out.println("keyStore: " + ssl.getAttribute("keyStore"));
                                    System.out.println("keyStorePwd: " + ssl.getAttribute("keyStorePwd"));
                                    System.out.println("trustStore: " + ssl.getAttribute("trustStore"));
                                    System.out.println("trustStorePwd: " + ssl.getAttribute("trustStorePwd"));
                                }
                            }
                        }

                        System.out.println("Broker Host: " + mongo.getAttribute("host"));
                        System.out.println("Broker Port: " + mongo.getAttribute("port"));
                        System.out.println("Client Id: " + mongo.getAttribute("clientID"));
                        System.out.println("Username: " + mongo.getAttribute("userName"));
                        System.out.println("Password: " + mongo.getAttribute("userPassword"));
                        System.out.println("Topic Name: " + mongo.getAttribute("topic"));
                    }
                }

                if (doc.getElementsByTagName("settings").getLength() > 0) {
                    Node settingsNode = doc.getElementsByTagName("settings").item(0);
                    if (settingsNode.getNodeType() == Node.ELEMENT_NODE) {
                        Element eElement = (Element) settingsNode;
                        System.out.println("Connections: " + eElement.getAttribute("connections"));
                        System.out.println("Offset: " + eElement.getAttribute("offset"));
                    }
                }

            } catch (Exception e) {
                e.printStackTrace();
            }

        } else {
            return;
        }

        Runner runner = new Runner(batchSize, brokerUrl, clientID, topicName, QoS, 0, false, index);

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
