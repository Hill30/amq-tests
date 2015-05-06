package com.hill30.amqstresstest.periodicsub;

import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.UTF8Buffer;
import org.fusesource.mqtt.client.*;

import java.net.URISyntaxException;
import java.util.Date;
import java.util.logging.Level;


public class Subscriber {

    private String topicPerfix;
    private int topicId;
    private String user;
    private String password;
    private String host;
    private int port;
    private boolean isWorking;
    private CallbackConnection connection;
    private short keepAliveTimeout;

    public Subscriber(String topicPrefix, int topicId, String user, String password, String host, int port, short keepAliveTimeout) {
        this.topicPerfix = topicPrefix;
        this.topicId = topicId;
        this.user = user;
        this.password = password;
        this.host = host;
        this.port = port;
        this.keepAliveTimeout = keepAliveTimeout;
    }

    public void disconnect() {
        connection.disconnect(null);
    }


    public void run() {
        MQTT mqtt = new MQTT();
        try {
            mqtt.setHost(host, port);
            mqtt.setUserName(user);
            mqtt.setCleanSession(false);
            mqtt.setClientId(topic());
            mqtt.setPassword(password);
            mqtt.setKeepAlive(keepAliveTimeout);

            connection = mqtt.callbackConnection();
            connection.connect(new Callback<Void>() {
                @Override
                public void onSuccess(Void value) {
                }
                @Override
                public void onFailure(Throwable value) {
                    value.printStackTrace();
                }
            });

        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (Exception e) {
        }
    }


    private String topic(){
        return topicPerfix+topicId;
    }
}
