package com.hill30.amqstresstest.periodicsub;
import java.io.IOException;
import java.util.ArrayList;
public class SubscriberRunnner {


    public static void main(final String[] args) {
        try {
            int startingTopicId = 1;
            int totalSubscribers = 1000;
            int globalCyclesCount =1000;
            String host = "127.0.0.1";
            String topicPrefix ="/mqtt_test_topic_";
            String user = "";
            String password = "";
            Short keepAliveTimeout = 30;
            int port = 1883;

            for(int iteration = 0; iteration <= globalCyclesCount; iteration++) {
                ArrayList<Subscriber> subscribersList = new ArrayList<Subscriber>();

                for (int topicId = startingTopicId; topicId <= startingTopicId + totalSubscribers; topicId++) {
                    Subscriber subscriber = new Subscriber(topicPrefix, topicId, user, password, host, port, keepAliveTimeout);
                    subscriber.run();
                    subscribersList.add(subscriber);
                    Thread.sleep(500);
                }

                Thread.sleep(10000);

                for (Subscriber thread : subscribersList) {
                    thread.disconnect();
                    Thread.sleep(500);
                }

                subscribersList.clear();
                subscribersList = null;
                Thread.sleep(10000);
            }

            System.in.read();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (Exception e){
            e.printStackTrace();
        }
        finally {
        }
    }

    private static String arg(String []args, int index, String defaultValue) {
        if( index < args.length )
            return args[index];
        else
            return defaultValue;
    }
}
