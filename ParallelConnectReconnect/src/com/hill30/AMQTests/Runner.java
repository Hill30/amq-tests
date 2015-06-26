package com.hill30.AMQTests;

import java.util.ArrayList;
import java.util.Date;

/**
 * Created by michaelfeingold on 6/26/2015.
 */
public class Runner implements Runnable {

    private int batchSize;
    private String brokerUrl;
    private String clientID;
    private String topicName;
    private int QoS;
    private boolean stop = false;
    private String command = "";
    ArrayList<ConnectionAdapter> adapters = new ArrayList<>();

    public Runner(int batchSize, String brokerUrl, String clientID, String topicName, int qoS) {
        this.batchSize = batchSize;
        this.brokerUrl = brokerUrl;
        this.clientID = clientID;
        this.topicName = topicName;
        QoS = qoS;
    }

    @Override
    public void run() {
        Start();
        while (!stop)
            try {
                if (command != "")
                    Execute(command);
                command = "";
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        stop = false;
    }

    public void Start() {

        Date start = new Date();
        int j;
        for (j = 0; j < batchSize; j++) {
            adapters.add(
                    new ConnectionAdapter(
                            brokerUrl,
                            clientID + "j" + Integer.toString(j),
                            topicName + "j" + Integer.toString(j),
                            QoS
                    ));
        }

        for (j = 0; j < batchSize; j++) {
            adapters.get(j).Connect();
            System.out.printf("Connected %d\r", (j + 1));
        }
//                    //System.out.print("connected: " + Integer.toString(i * batchSize + j + 1) + "\r");

        System.out.printf("\nConnects initiated in %d msec\n", new Date().getTime() - start.getTime());
/*
        start = new Date();
        boolean checked = false;
        while (!checked) {
            for (j=0; j<batchSize; j++) {
                checked = adapters.get(j).IsConnected() || adapters.get(j).IsAborted();
                if (!checked)
                    break;
            }
            if (!checked) {
                Thread.sleep(10);
                continue;
            }
            break;
        }

        System.out.printf("Waiting for pending connects... - done in %d msec\n", new Date().getTime() - start.getTime());
//*/

    }

    public void Quit() {
        stop = true;
    }

    public void Submit(String command) {
        this.command = command;
    }

    public void Execute(String command) {
        switch(command) {
            case("disconnect") : Disconnect();
                break;
            case("start") : Disconnect(); Start();
                break;
            default:
                System.out.println("unknown command: " + command);
                break;
        }
    }

    private void Disconnect() {
        Date start = new Date();
        int j;
        for (j=0; j<batchSize; j++) {
            adapters.get(0).Disconnect();
            adapters.remove(0);
            System.out.printf("Disconnected %d\r", j + 1);
        }

        System.out.printf("\nDisconnects initiated in %d msec\n", new Date().getTime() - start.getTime());

/*
        start = new Date();
        boolean disconnected = false;
        while (!disconnected) {
            for (j=0; j<batchSize; j++) {
                disconnected = !adapters.get(j).IsConnected();
                if (!disconnected)
                    break;
            }
            if (!disconnected) {
                Thread.sleep(10);
                continue;
            }
            break;
        }

        System.out.printf("Waiting for pending disconnects... - done in %d msec\n", new Date().getTime() - start.getTime());
//*/
    }

}
