package com.hill30.AMQTests;

import org.eclipse.paho.client.mqttv3.*;

/**
 * Created by michaelfeingold on 6/19/15.
 */
public class ConnectionAdapter implements Runnable {


    private String brokerUrl;
    private String clientID;
    private String topicName;
    private MqttConnectOptions options;
    private int QoS;
    private MqttAsyncClient client;
    private Thread thread;
    private boolean connected = false;

    public ConnectionAdapter(String brokerUrl, String clientID, String topicName, int QoS, MqttConnectOptions options) {
        this.brokerUrl = brokerUrl;
        this.clientID = clientID;
        this.topicName = topicName;
        this.QoS = QoS;
        this.options = options;
        //thread = new Thread(this);
        //thread.start();
    }

    public void Connect() throws MqttException {
        client = new MqttAsyncClient(brokerUrl, clientID, null);
        while (true) {
            try {
                client.connect(options).waitForCompletion();
            } catch (MqttException e) {
                System.out.println("\nConnect for " + clientID + " failed " + e.toString() + "\nRetrying...");
                continue;
            }

            ConnectionAdapter.this.connected = true;

            //System.out.print("connected " + clientID + "\r");

            //if (QoS >= 0)
            //    client.subscribe(topicName, QoS);
            break;
        }

    }

    public boolean IsConnected() {
        return connected;
    }

    public void Disconnect() throws MqttException {

        if (connected) {
            try {
                client.disconnect().waitForCompletion();
                //System.out.print("disconnected: " + clientID + "\r");
            } catch (MqttException e) {
                System.out.println("Disconnect for " + clientID + " failed " + e.toString());
            }

        }
    }

    @Override
    public void run() {
        try {
            Connect();
        } catch (MqttException e) {
            e.printStackTrace();
        }

        while (client != null)
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
    }
}
