package com.hill30.AMQTests;

import java.io.*;
import java.util.ArrayList;
import java.util.Date;

public class Runner implements Runnable {

    private int batchSize;
    private String brokerUrl;
    private String clientID;
    private String topicName;
    private int QoS;
    private int messagesPerDay;
    ArrayList<ConnectionAdapter> adapters = new ArrayList<>();
    private PrintStream log = null;
    private int connectionErrors = 0;
    private int connections = 0;
    private int disconnectionErrors = 0;
    private int subscribeErrors = 0;
    private String verb="";
    private Publisher publisher;
    private int sent = 0;
    private int received = 0;
    private int lost = 0;
    private int dups = 0;
    private int publishErrors = 0;

    public Runner(int batchSize, String brokerUrl, String clientID, String topicName, int qoS, int messagesPerDay) {
        this.batchSize = batchSize;
        this.brokerUrl = brokerUrl;
        this.clientID = clientID;
        this.topicName = topicName;
        QoS = qoS;
        this.messagesPerDay = messagesPerDay;

        try {
            log = new PrintStream("Exceptions.log");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {

        Start();

        publisher = new Publisher(this, adapters, messagesPerDay);

        verb = "Monitoring";


        try{
            BufferedReader br =
                    new BufferedReader(new InputStreamReader(System.in));

            String command;

            while((command=br.readLine())!=null){
                if (!command.trim().isEmpty())
                    Execute(command);
            }

            Disconnect();

        }catch(IOException io){
            io.printStackTrace();
        }

    }

    private void Start() {

        connectionErrors = 0;
        subscribeErrors = 0;
        disconnectionErrors = 0;
        publishErrors = 0;

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

    private void Execute(String command) {
        System.out.println("Execute command " + command);
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
            default:
                System.out.println("unknown command: >" + command + "<");
                break;
        }
    }

    private void Disconnect() {
        verb = "Disconnecting";
        publisher.stop();
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
                "%s... Connections: %d; Messages sent %d received %d lost %d dups %d; Errors connect: %d subscribe %d publish: %d disconnect: %d \r",
                verb, connections, sent, received, lost, dups,
                connectionErrors, subscribeErrors, publishErrors, disconnectionErrors);
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

    public synchronized void reportPublishError() {
        publishErrors++;
    }
}
