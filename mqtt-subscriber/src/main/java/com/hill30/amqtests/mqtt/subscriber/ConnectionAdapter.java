package com.hill30.amqtests.mqtt.subscriber;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.eclipse.paho.client.mqttv3.*;

import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.*;


public class ConnectionAdapter {
    private static Timer scheduler = new Timer();
    private final PrintStream log;
    private final Runner runner;
    private String brokerUrl;
    private String clientID;
    private String topicName;
    private int QoS;
    private MqttAsyncClient client;
    private boolean connected = false;
    private  Properties sslProps;
    public ConnectionAdapter(Runner runner, String clientID, String topicName) {
        this.runner = runner;
        brokerUrl = runner.getBrokerUrl();
        this.clientID = clientID;
        this.topicName = topicName;
        QoS = runner.getQoS();
        log = runner.getlog();
    }

    public Properties getSSLSettings() {
        final Properties properties = new Properties();
        properties.setProperty("com.ibm.ssl.keyStore", "C:/Downloads/certs/client.ks");
        properties.setProperty("com.ibm.ssl.keyStorePassword", "password");
        properties.setProperty("com.ibm.ssl.trustStore", "C:/Downloads/certs/client.ts");
        properties.setProperty("com.ibm.ssl.trustStorePassword", "password");
        return properties;
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

        if (sslProps == null) {
            sslProps = getSSLSettings();
        }

        MqttConnectOptions options = new MqttConnectOptions();
        options.setSSLProperties(sslProps);
        options.setCleanSession(false);
        options.setUserName("admin");
        options.setPassword("admin".toCharArray());

        try {
            client.setCallback(new MqttCallback() {
                @Override
                public void connectionLost(Throwable throwable) {
                    ConnectionAdapter.this.connected = false;
                    runner.reportDisconnect(null);
                    scheduleReconnect();
                }

                @Override
                public void messageArrived(String s, MqttMessage mqttMessage) throws Exception {
                    DBObject modifier = new BasicDBObject("received", 1);
                    DBObject incQuery = new BasicDBObject("$inc", modifier);
                    BasicDBObject query = new BasicDBObject("clientId", clientID);
                    runner.coll.update(query, incQuery);
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {}
            });

            client.connect(options, this, new IMqttActionListener() {
                @Override
                public void onSuccess(IMqttToken iMqttToken) {
                    runner.reportConnect();
                    connected = true;

                    DBObject flag = new BasicDBObject("connected", 1);
                    DBObject setQuery = new BasicDBObject("$set", flag);
                    BasicDBObject query = new BasicDBObject("clientId", clientID);
                    runner.coll.update(query, setQuery, false, false);

                    if (QoS > 0) {
                        try {

                            client.subscribe(topicName, QoS);

                            flag = new BasicDBObject("subscribed", 1);
                            setQuery = new BasicDBObject("$set", flag);
                            query = new BasicDBObject("clientId", clientID);
                            runner.coll.update(query, setQuery, false, false);

                        } catch (MqttException e) {

                            log.printf("%s: Subscribe for %s failed %s\r\n", new Date().toString(), clientID, e.toString());
                            runner.reportSubscribeError();

                            flag = new BasicDBObject("subscribed", 0);
                            setQuery = new BasicDBObject("$set", flag);
                            query = new BasicDBObject("clientId", clientID);
                            runner.coll.update(query, setQuery, false, false);

                        }
                    }
                }

                @Override
                public void onFailure(IMqttToken iMqttToken, Throwable throwable) {
                    /*DBObject flag = new BasicDBObject("connected", 0);
                    DBObject setQuery = new BasicDBObject("$set", flag);
                    BasicDBObject query = new BasicDBObject("clientId", clientID);
                    runner.coll.update(query, setQuery, false, false);

                    log.printf("%s: Connect for %s failed %s\r\n", new Date().toString(), clientID, throwable.toString());
                    */
                    runner.reportConnectionError();
                    scheduleReconnect();

                }
            });

        } catch (MqttException e) {

            DBObject flag = new BasicDBObject("connected", 0);
            DBObject setQuery = new BasicDBObject("$set", flag);
            BasicDBObject query = new BasicDBObject("clientId", clientID);
            runner.coll.update(query, setQuery, false, false);

            log.printf("%s: Connect for %s failed %s\r\n", new Date().toString(), clientID, e.toString());
            runner.reportConnectionError();
            scheduleReconnect();

        }
    }

    public boolean isConnected() {
        return client.isConnected();
    }

    public void scheduleReconnect() {
        int delay = randInt(1,10);
        scheduler.schedule(new TimerTask() {
            @Override
            public void run() {
                Connect();
            }
        }, delay*1000);
    }


   // public boolean IsConnected() { return connected; }

    public void Disconnect() {

        if (connected) {
            try {
                client.disconnect(1000, null, new IMqttActionListener() {
                    @Override
                    public void onSuccess(IMqttToken iMqttToken) {
                        ConnectionAdapter.this.connected = false;
                        runner.reportDisconnect(ConnectionAdapter.this);


                        DBObject flag = new BasicDBObject("connected", 0);
                        DBObject setQuery = new BasicDBObject("$set", flag);
                        BasicDBObject query = new BasicDBObject("clientId", clientID);
                        runner.coll.update(query, setQuery, false, false);
                    }

                    @Override
                    public void onFailure(IMqttToken iMqttToken, Throwable throwable) {
                        log.printf("%s: Disconnect for %s failed : %S\r\n", new Date().toString(), clientID, throwable.toString());
                        if (((MqttException) throwable).getReasonCode() == 32102) {
                            // 32102 means 'currently disconnecting' let us hope for the best
                            ConnectionAdapter.this.connected = false;
                            runner.reportDisconnect(ConnectionAdapter.this);
                        } else {
                            //runner.reportDisconnectionError();
                        }
                    }
                });
            } catch (MqttException e) {
                log.printf("%s: Disconnect for %s failed : %S\r\n", new Date().toString(), clientID, e.toString());
                //runner.reportDisconnectionError();
            }

        } else {
            log.printf("%s: %s Already disconnected\r\n", new Date().toString(), clientID);
            runner.reportDisconnect(ConnectionAdapter.this);
        }
    }

    public void publish(MqttClient client, int index) {
        try {
            ByteBuffer message = ByteBuffer.allocate(clientID.getBytes().length+4);
            message.putInt(index).put(clientID.getBytes());
            MqttMessage mqttMessage = new MqttMessage(message.array());
            mqttMessage.setQos(runner.getQoS());
            client.publish(topicName, mqttMessage);

            //runner.reportPublish();
        } catch (MqttException e) {
            //runner.reportPublishError();
        }
    }

    private synchronized void messageReceived(MqttMessage s) {
        //runner.reportReceive(0, 0);
    }

    public static int randInt(int min, int max) {
        // NOTE: Usually this should be a field rather than a method
        // variable so that it is not re-seeded every call.
        Random rand = new Random();
        // nextInt is normally exclusive of the top value,
        // so add 1 to make it inclusive
        int randomNum = rand.nextInt((max - min) + 1) + min;
        return randomNum;
    }
}
