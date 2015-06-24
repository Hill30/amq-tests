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
    private boolean aborted = false;

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
//*
        try {
            client.connect(options, new IMqttActionListener() {
                @Override
                public void onSuccess(IMqttToken iMqttToken) {
                    ConnectionAdapter.this.connected = true;
                }

                @Override
                public void onFailure(IMqttToken iMqttToken, Throwable throwable) {
                    System.out.println("\nConnect for " + clientID + " failed: " + throwable.toString());
                    ConnectionAdapter.this.aborted = true;
                }
            });
        } catch (MqttException e) {
            System.out.println("\nConnect for " + clientID + " failed: " + e.toString());
            ConnectionAdapter.this.aborted = true;
        }
//*/

/*
        while (true) {
            try {
                client.connect(options).waitForCompletion();
            } catch (MqttException e) {
                System.out.println("\nConnect for " + clientID + " failed: " + e.toString() + "\nRetrying...");
                continue;
            }

            ConnectionAdapter.this.connected = true;

            //System.out.print("connected " + clientID + "\r");

            //if (QoS >= 0)
            //    client.subscribe(topicName, QoS);
            break;
        }
//*/
    }

    public boolean IsConnected() {
        return connected;
    }

    public boolean IsAborted() {
        return aborted;
    }

    public void Disconnect() throws MqttException {

        if (connected) {
            client.disconnect(100, null, new IMqttActionListener() {
                @Override
                public void onSuccess(IMqttToken iMqttToken) {
                    ConnectionAdapter.this.connected = false;
                }

                @Override
                public void onFailure(IMqttToken iMqttToken, Throwable throwable) {
                    System.out.println("Disconnect for " + clientID + " failed " + throwable.toString());
                    ConnectionAdapter.this.connected = false;
                }
            });

/*
            try {
                client.disconnect().waitForCompletion();
                //System.out.print("aborted: " + clientID + "\r");
                ConnectionAdapter.this.connected = false;
            } catch (MqttException e) {
                System.out.println("Disconnect for " + clientID + " failed " + e.toString());
                ConnectionAdapter.this.connected = false;
            }
*/
        }
    }

    @Override
    public void run() {
        try {
            Connect();
        } catch (MqttException e) {
            e.printStackTrace();
        }

        while(!connected)
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        while(connected)
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

    }
}
