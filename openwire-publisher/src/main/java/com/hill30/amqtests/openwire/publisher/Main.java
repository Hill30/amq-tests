package com.hill30.amqtests.openwire.publisher;

import java.util.ArrayList;

public class Main {

    public static void main(String[] args) {

        ArrayList<Thread> threads = new ArrayList<Thread>();
        ArrayList<SyncPublisher> publishers = new ArrayList<SyncPublisher>();

        for (int i=0; i < 1; i++) {
            SyncPublisher sp = new SyncPublisher();
            publishers.add(sp);

            Thread t = new Thread(sp);
            t.start();
            threads.add(t);
        }


        Thread log = new Thread(new Runnable() {
            public void run()
            {
                try {

                    while(true) {
                        //System.out.print("----\n-----\r");
                        publishers.forEach(p -> {
                            p.printStats();
                        });
                        Thread.sleep(10000);
                    }

                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }});

        log.start();

        threads.forEach( thread -> {
            try {
                thread.join();
            } catch (InterruptedException e) {
                    e.printStackTrace();
            }
        });

    }

    /*private static final String BROKER_HOST = "ssl://10.0.1.55:%d";
    private static final int BROKER_PORT = 62616;
    private static  String BROKER_URL = String.format(BROKER_HOST, BROKER_PORT);
    private static final Boolean NON_TRANSACTED = false;

    private static int NUM_MESSAGES_TO_SEND = 10;
    private static int NUM_OF_TOPICS = 20000;
    private static String TOPIC_NAME = "Topic";

    private static int index = 1;
    private static boolean isConnected = false;
    private static ActiveMQSslConnectionFactory connectionFactory;
    private static Connection c = null;

    private static int sent = 0;

    public static void main(String[] args) {

        if (args.length > 0) {
            BROKER_URL = args[0];
        }

        if (args.length > 1) {
            index = Integer.parseInt(args[1]);
        }

        if (args.length > 2) {
            NUM_MESSAGES_TO_SEND = Integer.parseInt(args[2]);
        }

        if (args.length > 3) {
            NUM_OF_TOPICS = Integer.parseInt(args[3]);
        }

        if (args.length > 5) {
            TOPIC_NAME = args[5];
        }

        if (index < 1) {
            index = 1;
        }

        if (index > NUM_OF_TOPICS) {
            index = NUM_OF_TOPICS;
        }
        final int offset = (index - 1) * NUM_OF_TOPICS;
        final int limit = index * NUM_OF_TOPICS;

        connectionFactory  = new ActiveMQSslConnectionFactory(BROKER_URL);
        connectionFactory.setUserName("admin");
        connectionFactory.setPassword("admin");

        connectionFactory.setKeyStore("file:///C:/Downloads/certs/client.ks");
        connectionFactory.setKeyStorePassword("password");
        connectionFactory.setTrustStore("file:///C:/Downloads/certs/client.ts");
        connectionFactory.setTrustStorePassword("password");
        long unixTime = System.currentTimeMillis() / 1000L;

        for (int i = 0; i < NUM_MESSAGES_TO_SEND; i++) {
           for (int j = offset; j <= limit-1; j++) {
               publishToTopic(TOPIC_NAME + ".j" + j, "message" + j);
               sent++;
               printStats("Sending...");
               publishToQueue(TOPIC_NAME + ".overall." + unixTime, "message" + j);
           }
        }

        try {
            if (c != null) {
                c.close();
            }
        } catch (JMSException e) {
            e.printStackTrace();
        }

    }

    public static void publishToTopic(String dest, String msg) {
        try {
            establishConnection();
            if (c != null) {
                Session session = c.createSession(NON_TRANSACTED, Session.AUTO_ACKNOWLEDGE);
                Destination destination = session.createTopic(dest);
                MessageProducer producer = session.createProducer(destination);
                producer.setDeliveryMode(DeliveryMode.PERSISTENT);
                TextMessage message = session.createTextMessage(msg);
                producer.send(message);
                producer.close();
                session.close();
            }
        } catch (JMSException e) {
            printStats("Failed while sending to topic" + dest + " ...");
            isConnected = false;
            publishToTopic(dest, msg);
        }
    }

    public static void publishToQueue(String dest, String msg) {
        try {
            establishConnection();
            if (c != null) {
                Session session = c.createSession(NON_TRANSACTED, Session.AUTO_ACKNOWLEDGE);
                Destination destination = session.createQueue(dest);
                MessageProducer producer = session.createProducer(destination);
                producer.setDeliveryMode(DeliveryMode.PERSISTENT);
                TextMessage message = session.createTextMessage(msg);
                producer.send(message);
                producer.close();
                session.close();
            }
        } catch (JMSException e) {
            printStats("Failed while sending to queue" + dest + "...");
            isConnected = false;
            publishToTopic(dest, msg);
        }
    }


    public static void delay(long mills) {
        try {
            Thread.sleep(mills);
        } catch (InterruptedException e1) {
            establishConnection();
        }
    }

    public static void establishConnection() {
        try {
            while (!isConnected) {
                c = connectionFactory.createConnection();
                c.start();
                isConnected = true;
                printStats("Connected...");
            }

        } catch (JMSException e) {
            printStats("Reconnecting...");
            isConnected = false;
            establishConnection();
        }
    }


    public static void printStats(String status) {
        try
        {
            final String os = System.getProperty("os.name");

            if (os.contains("Windows"))
            {
                Runtime.getRuntime().exec("cls");
            }
            else
            {
                Runtime.getRuntime().exec("clear");
            }
        }
        catch (final Exception e)
        {
            //  Handle any exceptions.
        }

        System.out.print(status + " \t\t\t| BROKER: " + BROKER_URL + "; Topic prefix: " +  TOPIC_NAME +  "; Topics: "  + NUM_OF_TOPICS  +  "; Messages to send: "  +  NUM_MESSAGES_TO_SEND +   "; Sent: " + sent + "\r");
    }
    */
}
