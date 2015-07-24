package com.hill30.AMQTests;

import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttException;

import java.io.PrintStream;
import java.util.*;

public class Publisher {
    private final ArrayList<ConnectionAdapter> adapters;
    private Timer timer = new Timer();
    private String brokerUrl;
    private MqttAsyncClient client;
    private PrintStream log;
    Random randomizer = new Random();
    private int messageFrequency;

    public Publisher(Runner runner, ArrayList<ConnectionAdapter> adapters, int interval) {
        brokerUrl = runner.getBrokerUrl();
        this.adapters = adapters;
        log = runner.getlog();
        start();
        messageFrequency = 24*60*60*1000/interval;
    }

    public void start() {
        try {
            client = new MqttAsyncClient(brokerUrl, "publisher", null);
            client.connect().waitForCompletion();
        } catch (MqttException e) {
            log.printf("%s: Could not create client for %s : %s\r\n", new Date().toString(), "publisher", e.toString());
        }

        adapters.forEach(adapter -> {
            schedulePublish(adapter);
        });
    }

    public void stop() {
        timer.cancel();
        if (client.isConnected())
            try {
                client.disconnect();
            } catch (MqttException e) {
                log.printf("%s: Error disconnecting %s : %s\r\n", new Date().toString(), "publisher", e.toString());
            }
    }

    private void schedulePublish(ConnectionAdapter adapter) {
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                adapter.publish(client);
                schedulePublish(adapter);
            }
        }, randomizer.nextInt(messageFrequency + 1));
    }
}
