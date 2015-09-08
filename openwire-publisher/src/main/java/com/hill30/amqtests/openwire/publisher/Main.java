/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hill30.amqtests.openwire.publisher;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSslConnectionFactory;

import javax.jms.*;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class Main {
    private static final String BROKER_HOST = "ssl://10.0.1.103:%d";
    private static final int BROKER_PORT = 62616;
    private static  String BROKER_URL = String.format(BROKER_HOST, BROKER_PORT);
    private static final Boolean NON_TRANSACTED = false;

    private static int NUM_MESSAGES_TO_SEND = 1;
    private static int NUM_OF_THREADS = 10;
    private static int NUM_OF_TOPICS = 10000;
    private static String TOPIC_NAME = "T";

    private static int sent[] = new int[NUM_OF_THREADS];
    private static int index = 1;

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

        if (args.length > 4) {
            NUM_OF_THREADS = Integer.parseInt(args[4]);
            sent = new int[NUM_OF_THREADS];
        }

        if (args.length > 5) {
            TOPIC_NAME = args[5];
        }

        final int offset = NUM_OF_TOPICS*index; // 1 * 10000 = 10000
        final int limit = NUM_OF_TOPICS + offset; // 10000 + 10000


        final ActiveMQSslConnectionFactory connectionFactory = new ActiveMQSslConnectionFactory(BROKER_URL);

        connectionFactory.setUserName("admin");
        connectionFactory.setPassword("admin");

        connectionFactory.setKeyStore("file:///C:/Downloads/certs/client.ks");
        connectionFactory.setKeyStorePassword("password");

        connectionFactory.setTrustStore("file:///C:/Downloads/certs/client.ts");
        connectionFactory.setTrustStorePassword("password");

       // } else {
            //final ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("admin", "admin", BROKER_URL);
       // }


        ExecutorService es = Executors.newCachedThreadPool();
        long startTime = System.currentTimeMillis();

        for(int i = 0; i < NUM_OF_THREADS; i++) {
            es.execute(new Runnable() {
                @Override
                public void run() {
                    final long threadId = Thread.currentThread().getId()%NUM_OF_THREADS ;
                    connect(connectionFactory, new Function() {
                        @Override
                        public Object apply(Object o) {
                            int sent = 0;
                            Session session = (Session)o;

                            for (int i = 0; i < NUM_MESSAGES_TO_SEND; i++) {
                                for (int j = offset; j < limit; j++) {
                                    publishToTopic(session, TOPIC_NAME + ".j" + j, "Message: " + j);
                                    publishToQueue(session, "Total" + index + ".Topics." + NUM_OF_TOPICS + ".Messages." + NUM_MESSAGES_TO_SEND, String.valueOf(sent));
                                    sent++;
                                    printStats((int) threadId, sent);
                                }
                            }

                            publishToQueue(session, "Main" + index + ".Topics." + NUM_OF_TOPICS + ".Messages." + NUM_MESSAGES_TO_SEND, String.valueOf(sent));
                            return null;
                        }
                    });
                }
            });

        }

        try{
            BufferedReader br =
                    new BufferedReader(new InputStreamReader(System.in));

            String command;

            while((command=br.readLine())!=null){
                if (!command.trim().isEmpty())
                   System.out.print("\n" + command);

                if(command == "q") {
                    System.exit(0);
                }
            }

        }catch(IOException io){
            io.printStackTrace();
        }

        es.shutdown();

        try {
            boolean finished = es.awaitTermination(1, TimeUnit.MINUTES);
            if (finished) {
                long endTime = System.currentTimeMillis();
                long millis = (endTime - startTime);
                double seconds = millis/1000;

                String executionTimeString = String.format("%d min %d sec \n",
                        TimeUnit.MILLISECONDS.toMinutes(millis),
                        TimeUnit.MILLISECONDS.toSeconds(millis) -
                                TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(millis))
                );

                System.out.print("Execution time: " + executionTimeString);
                System.out.print("BROKER: " + BROKER_URL +
                        "\nTopic prefix: " +  TOPIC_NAME  +
                        "\nThreads: " +  NUM_OF_THREADS +
                        "\nTopics: "  + NUM_OF_TOPICS  +
                        "\nMessages to send: "  +  NUM_MESSAGES_TO_SEND );

                System.out.print("\nSent: " + Arrays.toString(sent) + " = "  + getSum() +  "\n");
                System.out.print("Throughput: " + (NUM_OF_TOPICS*NUM_OF_THREADS*NUM_MESSAGES_TO_SEND / seconds) + " messages/sec \n");

            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    public static void publishToTopic(Session session, String dest, String msg) {
        try {
            Destination destination = session.createTopic(dest);
            MessageProducer producer = session.createProducer(destination);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
            TextMessage message = session.createTextMessage(msg);
            producer.send(message);
            producer.close();
        } catch (JMSException e) {
            e.printStackTrace();
            publishToTopic(session, dest, msg);
        }
    }

    public static void publishToQueue(Session session, String dest, String msg) {
        try {
            Destination destination = session.createQueue(dest);
            MessageProducer producer = session.createProducer(destination);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
            TextMessage message = session.createTextMessage(msg);

            producer.send(message);
            producer.close();
        } catch (JMSException e) {
            e.printStackTrace();
            publishToQueue(session, dest, msg);
        }
    }


    public static void connect(ConnectionFactory connectionFactory, Function callback) {
        Connection connection = null;
        try {
            connection = connectionFactory.createConnection();
            connection.start();
            Session session = connection.createSession(NON_TRANSACTED, Session.AUTO_ACKNOWLEDGE);
            callback.apply(session);
            session.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void printStats(int index, int counter) {
        sent[index] = counter;
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

        System.out.print("BROKER: " + BROKER_URL + "; Topic prefix: " +  TOPIC_NAME + "; Threads: " +  NUM_OF_THREADS +  "; Topics: "  + NUM_OF_TOPICS  +  "; Messages to send: "  +  NUM_MESSAGES_TO_SEND +   "; Sending: " + Arrays.toString(sent) + " = " + getSum() + "\r");
    }

    public static int getSum() {
        int sum = 0;
        for (int t = 0; t < NUM_OF_THREADS; t++) {
            sum += sent[t];
        }
        return  sum;
    }



}
