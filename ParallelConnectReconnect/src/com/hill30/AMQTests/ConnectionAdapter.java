package com.hill30.AMQTests;

import org.eclipse.paho.client.mqttv3.*;

import java.io.PrintStream;

/**
 * Created by michaelfeingold on 6/19/15.
 */
public class ConnectionAdapter {


    private final PrintStream log;
    private String brokerUrl;
    private String clientID;
    private String topicName;
    private int QoS;
    private MqttAsyncClient client;
    private boolean connected = false;
    private boolean aborted = false;

    public ConnectionAdapter(String brokerUrl, String clientID, String topicName, int QoS, PrintStream log) {
        this.brokerUrl = brokerUrl;
        this.clientID = clientID;
        this.topicName = topicName;
        this.QoS = QoS;
        this.log = log;
    }

    public void Connect() {

        aborted = false;
        connected = false;

        try {
            client = new MqttAsyncClient(brokerUrl, clientID, null);
        } catch (MqttException e) {
            log.println("\nCould not create client for " + clientID + " : " + e.toString());
            aborted = true;
        }

        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(false);

/*
        try {
            client.connect(options, new IMqttActionListener() {
                @Override
                public void onSuccess(IMqttToken iMqttToken) {
                    ConnectionAdapter.this.connected = true;
                }

                @Override
                public void onFailure(IMqttToken iMqttToken, Throwable throwable) {
                    System.log.println("\nConnect for " + clientID + " failed: " + throwable.toString());
                    ConnectionAdapter.this.aborted = true;
                }
            });
        } catch (MqttException e) {
            System.log.println("\nConnect for " + clientID + " failed: " + e.toString());
            ConnectionAdapter.this.aborted = true;
        }
//*/

//*
        try {
            client.connect(options).waitForCompletion(1000);
            connected = true;
            client.setCallback(new MqttCallback() {
                @Override
                public void connectionLost(Throwable throwable) {
                    ConnectionAdapter.this.connected = false;
                }

                @Override
                public void messageArrived(String s, MqttMessage mqttMessage) throws Exception {

                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {

                }
            });
        } catch (MqttException e) {
            log.println("\nConnect for " + clientID + " failed: " + e.toString());
            aborted = true;
        }

//*/
    }

    public boolean IsConnected() { return connected; }

    public boolean IsAborted() {
        return aborted;
    }

    public void Run() {

    }

    public void Disconnect() {

        if (connected) {
            try {
                client.disconnect(100, null, new IMqttActionListener() {
                    @Override
                    public void onSuccess(IMqttToken iMqttToken) {
                        ConnectionAdapter.this.connected = false;
                    }

                    @Override
                    public void onFailure(IMqttToken iMqttToken, Throwable throwable) {
                        log.println("Disconnect for " + clientID + " failed " + throwable.toString());
                        ConnectionAdapter.this.aborted = true;
                    }
                });
            } catch (MqttException e) {
                log.println("Disconnect for " + clientID + " failed " + e.toString());
                ConnectionAdapter.this.aborted = true;
            }

/*
            try {
                client.disconnect().waitForCompletion();
                //System.log.print("aborted: " + clientID + "\r");
                ConnectionAdapter.this.connected = false;
            } catch (MqttException e) {
                System.log.println("Disconnect for " + clientID + " failed " + e.toString());
                ConnectionAdapter.this.connected = false;
            }
*/
        }
    }

}
