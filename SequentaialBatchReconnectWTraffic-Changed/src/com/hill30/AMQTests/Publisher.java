package com.hill30.AMQTests;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;

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
    private Runner runner;

    public Publisher(Runner runner, ArrayList<ConnectionAdapter> adapters, int interval) {
        this.runner = runner;
        brokerUrl = runner.getBrokerUrl();
        this.adapters = adapters;
        log = runner.getlog();
        start();
        messageFrequency = 24*60*60*1000/interval;
    }

    public void start() {
        try {
            client = new MqttAsyncClient(brokerUrl, "publisher", new MqttDefaultFilePersistence("./Storage"));

            MqttConnectOptions options = new MqttConnectOptions();
            options.setCleanSession(false);
            client.connect(options).waitForCompletion();
            client.setCallback(new MqttCallback() {
                @Override
                public void connectionLost(Throwable throwable) {
                    runner.schedulePublisherRestart();
                }

                @Override
                public void messageArrived(String s, MqttMessage mqttMessage) throws Exception {

                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {

                }
            });
        } catch (MqttException e) {
            log.printf("%s: Could not create client for %s : %s\r\n", new Date().toString(), "publisher", e.toString());
            runner.schedulePublisherRestart();
        }

        adapters.forEach(adapter -> {
            schedulePublish(adapter);
        });
    }

    public void suspendPublish() {
        /*
        if (timer != null)
            timer.cancel();
        timer = null;
        */
        adapters.forEach(adapter -> {
            adapter.canPublish = false;
        });
    }

    public void resumePublish() {
        /*
        if (timer == null)
            timer = new Timer();

        adapters.forEach(adapter -> {
            schedulePublish(adapter);
        });
        */
        adapters.forEach(adapter -> {
            adapter.canPublish = true;
        });
    }

    public void stop() {
        if (timer != null)
            timer.cancel();
        timer = null;
        if (client.isConnected())
            try {
                client.disconnect();
            } catch (MqttException e) {
                log.printf("%s: Error disconnecting %s : %s\r\n", new Date().toString(), "publisher", e.toString());
            }
        //System.out.printf("Messages sent %d received %d over %d sec. (%f messages per sec)", );
    }

    private void schedulePublish(ConnectionAdapter adapter) {
        if (timer != null)
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    adapter.publish(client);
                    schedulePublish(adapter);
                }
            }, randomizer.nextInt(messageFrequency + 1));
    }
}
