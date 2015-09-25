package com.hill30.amqtests.mqtt.subscriber;


import java.io.*;
import java.net.UnknownHostException;
import java.util.*;
import com.mongodb.*;

public class Runner implements Runnable {

    private int batchSize;
    private String brokerUrl;
    private String clientID;
    private String topicName;
    private int QoS;
    ArrayList<ConnectionAdapter> adapters = new ArrayList<>();
    private PrintStream log = null;
    private int connectionErrors = 0;
    private int connections = 0;
    private int c = 0;
    private int subscribeErrors = 0;
    private String verb="";
    private int received = 0;
    private Timer scheduler = null;
    private int pubIndex;

    private MongoClient mongoClient;
    private DB db;
    public  DBCollection coll;

    public Runner(int batchSize, String brokerUrl, String clientID, String topicName, int qoS, int messagesPerDay, boolean isPublisher, int pubIndex) {

        this.batchSize = batchSize;
        this.brokerUrl = brokerUrl;
        this.clientID = clientID;
        this.topicName = topicName;
        this.pubIndex = pubIndex;
        this.QoS = qoS;


        try {
            log = new PrintStream("Exceptions.log");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        try {
            mongoClient = new MongoClient();
        } catch (MongoClientException e) {
            e.printStackTrace();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }


        Start();
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

            synchronized (this) {
                try {
                    this.wait();  // wait for connections to become 0 see reportDisconnect
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        }catch(IOException io){
            io.printStackTrace();
        }

    }

    private void Start() {
        connectionErrors = 0;
        subscribeErrors = 0;

        scheduler = new Timer();

        verb = "Connecting";

        Date start = new Date();


        if (pubIndex < 1) {
            pubIndex = 1;
        }

        if (pubIndex > batchSize) {
            pubIndex = batchSize;
        }
        int offset = (pubIndex-1) * batchSize;
        int limit =  pubIndex * batchSize;

        db = mongoClient.getDB("sub");
        coll = db.getCollection("connects");

        coll.createIndex(new BasicDBObject("clientId", 1),  new BasicDBObject("unique", true));
        coll.createIndex(new BasicDBObject("received", 1));

        for (int j = offset; j <= limit-1; j++) {
            BasicDBObject doc = new BasicDBObject("clientId",  clientID + "j" + Integer.toString(j))
                    .append("topicName",  topicName + "j" + Integer.toString(j));

            // skip duplicate exceptions
            try {
                coll.insert(doc);
            } catch (DuplicateKeyException e) {

            }

            try {
                Thread.sleep(10);
            } catch (Exception e) {
                e.printStackTrace();
            }

            ConnectionAdapter ca = new ConnectionAdapter(
                            this,
                            clientID + "j" + Integer.toString(j),
                            topicName + "j" + Integer.toString(j));
            adapters.add(j, ca);
            ca.Connect();

        }

        //System.out.printf("\n%s %d Connects initiated in %d msec\n",
                //new Date().toString(), batchSize, new Date().getTime() - start.getTime());

    }

    private void Execute(String command) {
        System.out.println("Execute command " + command);
        switch(command) {
            case("dis") :
                Disconnect();
                break;
            case("res") :
                Disconnect();
                Start();
                break;
            case("log"):
                report();
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
        scheduler.cancel();
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
        c = 0;
        adapters.forEach(a -> {
            if (a.isConnected()) {
                c++;
            }
        });

        System.out.printf(
                "Subscription %d) %s... Connections: %d; received %d; Errors connect %d subscribe %d;  \r",
                pubIndex, verb, c, received,
                connectionErrors, subscribeErrors);

    }

    public synchronized void reportConnect() {

        report();
    }

    public synchronized void reportDisconnect(ConnectionAdapter adapter) {
        if (adapter != null)
            //adapters.remove(adapter);
        //connections--;
        report();
       // if (connections == 0) {
       //     System.out.printf("\n%s All of %d connections successfully disconnected\n", new Date().toString(), batchSize);
       //     this.notify();
       // }
    }

    public synchronized void reportConnectionError() {
        connectionErrors++;
        report();
    }

    public  synchronized void reportSubscribeError() {
        subscribeErrors++;
        report();
    }

    public final static void clearConsole()
    {
        try {
            Runtime.getRuntime().exec("cls");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
