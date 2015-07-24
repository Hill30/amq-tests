package com.hill30.AMQTests;

import org.eclipse.paho.client.mqttv3.*;

import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.Date;

public class ConnectionAdapter {


    private final PrintStream log;
    private final Runner runner;
    private String brokerUrl;
    private String clientID;
    private String topicName;
    private int QoS;
    private MqttAsyncClient client;
    private boolean connected = false;

    public ConnectionAdapter(Runner runner, String clientID, String topicName) {
        this.runner = runner;
        brokerUrl = runner.getBrokerUrl();
        this.clientID = clientID;
        this.topicName = topicName;
        QoS = runner.getQoS();
        log = runner.getlog();
    }

    public void Connect() {

        if (connected)
            return;

        try {
            client = new MqttAsyncClient(brokerUrl, clientID, null);
        } catch (MqttException e) {
            log.printf("%s: Could not create client for %s : %s\r\n", new Date().toString(), clientID, e.toString());
            runner.reportConnectionError();
        }

        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(false);

//*
        try {
            client.connect(options).waitForCompletion(1000);
            connected = true;
            runner.reportConnect();
            if (QoS > 0)
                try {
                    client.subscribe(topicName, QoS);
                } catch (MqttException e) {
                    log.printf("%s: Subscribe for %s failed %s\r\n", new Date().toString(), clientID, e.toString());
                    runner.reportSubscribeError();
                }
            client.setCallback(new MqttCallback() {
                @Override
                public void connectionLost(Throwable throwable) {
                    ConnectionAdapter.this.connected = false;
                    runner.reportDisconnect(null);
                }

                @Override
                public void messageArrived(String s, MqttMessage mqttMessage) throws Exception {
                    messageReceived(mqttMessage);
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {

                }
            });
        } catch (MqttException e) {
            log.printf("%s: Connect for %s failed %s\r\n", new Date().toString(), clientID, e.toString());
            runner.reportConnectionError();
        }

//*/
    }

    public boolean IsConnected() { return connected; }

    public void Disconnect() {

        if (connected) {
            try {
                client.disconnect(1000, null, new IMqttActionListener() {
                    @Override
                    public void onSuccess(IMqttToken iMqttToken) {
                        ConnectionAdapter.this.connected = false;
                        runner.reportDisconnect(ConnectionAdapter.this);
                    }

                    @Override
                    public void onFailure(IMqttToken iMqttToken, Throwable throwable) {
                        log.printf("%s: Disconnect for %s failed : %S\r\n", new Date().toString(), clientID, throwable.toString());
                        runner.reportDisconnectionError();
                    }
                });
            } catch (MqttException e) {
                log.printf("%s: Disconnect for %s failed : %S\r\n", new Date().toString(), clientID, e.toString());
                runner.reportDisconnectionError();
            }

        } else
            log.printf("%s: %s Already disconnected\r\n", new Date().toString(), clientID);
    }

    private int countSent = 0;
    public void publish(MqttAsyncClient client) {
        try {
            ByteBuffer message = ByteBuffer.allocate(clientID.getBytes().length+4);
            message.putInt(countSent).put(clientID.getBytes());
            client.publish(topicName, new MqttMessage(message.array()));
            countSent++;
            runner.reportPublish();
        } catch (MqttException e) {
            log.printf("%s: Publish #%d for %s failed %s\r\n", new Date().toString(), countSent, clientID, e.toString());
        }
    }

    private int countReceived = 0;
    private void messageReceived(MqttMessage s) {
        ByteBuffer message = ByteBuffer.wrap(s.getPayload());
        int count = message.getInt();
        String ClientID = new String(message.slice().array());
        if (count < countReceived)
            runner.reportReceive(0, 1);
        else if (count > countReceived)
            runner.reportReceive(1, 0);
        else
            runner.reportReceive(0, 0);
        countReceived = count+1;
    }

}
