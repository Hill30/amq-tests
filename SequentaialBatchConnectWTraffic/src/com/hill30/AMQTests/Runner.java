package com.hill30.AMQTests;

import java.io.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.Objects;
import java.util.function.Consumer;

public class Runner implements Runnable {

    private int batchSize;
    private String brokerUrl;
    private String clientID;
    private String topicName;
    private int QoS;
    private boolean stop = false;
    private String command = "";
    ArrayList<ConnectionAdapter> adapters = new ArrayList<>();
    private PrintStream log = null;
    private int connectionErrors = 0;
    private int connections = 0;
    private int disconnectionErrors = 0;
    private int subscribeErrors = 0;
    private boolean disconnecting;
    private String verb="";
    private Publisher publisher;
    private int sent = 0;
    private int received = 0;
    private int lost = 0;
    private int dups = 0;

    public Runner(int batchSize, String brokerUrl, String clientID, String topicName, int qoS) {
        this.batchSize = batchSize;
        this.brokerUrl = brokerUrl;
        this.clientID = clientID;
        this.topicName = topicName;
        QoS = qoS;

        try {
            log = new PrintStream("Exceptions.log");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        Start();

        publisher = new Publisher(this, adapters, 100);

        verb = "Monitoring";
        while (!stop)
            try {
                Thread.sleep(1000);
                if (!disconnecting)
                    Reconnect();
                if (!Objects.equals(command, ""))
                    System.out.printf("Executing %s", command);
                    Execute(command);
                command = "";
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        stop = false;
    }

    private void Start() {

        connectionErrors = 0;
        subscribeErrors = 0;
        disconnectionErrors = 0;
        disconnecting = false;

        verb = "Connecting";

        Date start = new Date();
        int j;
        for (j = 0; j < batchSize; j++) {
            adapters.add(
                    new ConnectionAdapter(
                            this,
                            clientID + "j" + Integer.toString(j),
                            topicName + "j" + Integer.toString(j)
                    ));
        }

        for (j = 0; j < batchSize; j++) {
            adapters.get(j).Connect();
        }

        System.out.printf("\n%s %d Connects initiated in %d msec\n",
                new Date().toString(), batchSize, new Date().getTime() - start.getTime());

    }

    private void Reconnect() {
        int j;
        for (j = 0; j < adapters.size(); j++) {
            if (!adapters.get(j).IsConnected())
                adapters.get(j).Connect();
        }
    }

    public void stop() {
        Disconnect();
        stop = true;
    }

    public void Submit(String command) {
        this.command = command;
    }

    private void Execute(String command) {
        switch(command) {
            case("disconnect") : Disconnect();
                break;
            case("restart") : Disconnect(); Start();
                break;
            case("reconnect") : Reconnect();
                break;
            case("?"):
                report();
                break;
            case(""):
                break;
            default:
                System.out.println("unknown command: >" + command + "<");
                break;
        }
    }

    private void Disconnect() {
        verb = "Disconnecting";
        disconnecting = true;
        Date start = new Date();
        Iterable<ConnectionAdapter> a = (Iterable<ConnectionAdapter>)adapters.clone();
        final int[] count = {0};
        a.forEach(adapter -> {
            adapter.Disconnect();
            count[0]++;
        });

        System.out.printf("\n%s %d Disconnects initiated in %d msec\n",
                new Date().toString(), count[0], new Date().getTime() - start.getTime());
        verb = "Monitoring";

    }

    public PrintStream getlog() {
        return log;
    }

    public int getQoS() {
        return QoS;
    }

    public String getBrokerUrl() {
        return brokerUrl;
    }

    public void report() {
        System.out.printf(
                "%s... Connections: %d; Messages sent: %d received %d lost %d dups %d; Errors connect: %d subscribe %d, disconnect: %d \r",
                verb, connections, sent, received, lost, dups,
                connectionErrors, subscribeErrors, disconnectionErrors);
    }

    public synchronized void reportConnect() {
        connections++;
        report();
        if (connections == batchSize)
            System.out.printf("\n%s All of %d connections successfully connected\n", new Date().toString(), connections);
    }

    public synchronized void reportDisconnect(ConnectionAdapter adapter) {
        if (adapter != null)
            adapters.remove(adapter);
        connections--;
        report();
        if (connections == 0)
            System.out.printf("\n%s All of %d connections successfully disconnected\n", new Date().toString(), batchSize);
    }

    public synchronized void reportConnectionError() {
        connectionErrors++;
        report();
    }

    public synchronized void reportDisconnectionError() {
        disconnectionErrors++;
        report();
    }

    public synchronized void reportSubscribeError() {
        subscribeErrors++;
        report();
    }

    public synchronized void reportPublish() {
        sent++;
        report();
    }

    public synchronized void reportReceive(int lost, int dups) {
        received++;
        this.lost += lost;
        this.dups += dups;
        report();
    }
}