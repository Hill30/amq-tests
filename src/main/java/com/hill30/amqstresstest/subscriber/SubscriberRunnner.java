package com.hill30.amqstresstest.subscriber;

import java.io.IOException;
import java.util.Date;

/**
 * Created by azavarin on 4/7/2015.
 */
public class SubscriberRunnner {

    public static void main(final String[] args) {
        try {
            int startingTopicId = Integer.parseInt(arg(args,0,"0"));
            int totalSubscribers = Integer.parseInt(arg(args,1,"1"));
            String topicPrefix = arg(args, 2, "/mqtt_test_topic/");

            String user = "admin";
            String password = "admin";
            String host = "127.0.0.1";
            int port = 1883;

            System.out.println("Starting topic ID: " + startingTopicId);
            System.out.println("Total subscribers: " + totalSubscribers);
            System.out.println("Topic prefix: " + topicPrefix);

            for (int topicId = startingTopicId; topicId <= startingTopicId+totalSubscribers;topicId++){
                new Subscriber(topicPrefix, topicId, user, password, host, port).start();
                Thread.sleep(500);
            }

            System.in.read();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (Exception e){
            System.err.println((new Date()));
            e.printStackTrace();
        }
    }

    private static String arg(String []args, int index, String defaultValue) {
        if( index < args.length )
            return args[index];
        else
            return defaultValue;
    }
}
