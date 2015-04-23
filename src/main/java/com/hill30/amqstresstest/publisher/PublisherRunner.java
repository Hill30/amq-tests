package com.hill30.amqstresstest.publisher;

import com.hill30.amqstresstest.subscriber.Subscriber;

import java.util.UUID;

/**
 * Created by azavarin on 4/8/2015.
 */
public class PublisherRunner {

    public static void main(final String[] args) {

        final String user = "admin";
        final String password = "admin";
        final String host = "localhost";
        final int port = 1883;
        String topic = "/azavarin/1";
        String instanceId = UUID.randomUUID().toString();

        Subscriber subscriber = new Subscriber("/azavarin/", 1, user, password, host, port);

        try {
            Publisher p = new Publisher(user, password, host, port, topic);
            subscriber.start();

            for(int i = 0; i < 10; i++){
                p.publish(i + " " + instanceId);
                if(i >= 2){
                    p.disconnect();
                }
            }

            Thread.sleep(1000);
            subscriber.finish();

        } catch (Exception e){
            e.printStackTrace();
            System.exit(-1);
            subscriber.finish();
        }
    }
}
