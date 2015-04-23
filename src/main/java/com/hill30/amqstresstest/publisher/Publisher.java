package com.hill30.amqstresstest.publisher;

import org.fusesource.mqtt.client.Callback;
import org.fusesource.mqtt.client.FutureConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.QoS;

import java.net.URISyntaxException;

/**
 * Created by azavarin on 4/8/2015.
 */
public class Publisher {

    private FutureConnection connection;
    private String topic;

    public Publisher(String user, String password, String host, int port, String topic) throws Exception {

        this.topic = topic;

        MQTT mqtt = new MQTT();
        mqtt.setHost(host, port);
        mqtt.setUserName(user);
        mqtt.setPassword(password);

        connection = mqtt.futureConnection();
        connection.connect().await();
    }

    public void publish(final String s) throws Exception {
        connection.publish(topic, s.getBytes(), QoS.EXACTLY_ONCE, false).then(new Callback<Void>() {
            @Override
            public void onSuccess(Void aVoid) {
                System.out.println("Sent message: " + s);
            }

            @Override
            public void onFailure(Throwable throwable) {
                System.out.println("Failed to send message: " + s + " " + throwable.getMessage());
            }
        });
    }

    public void disconnect() {
        connection.disconnect();
    }
}
