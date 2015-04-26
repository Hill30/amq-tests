package com.hill30.amqstresstest.paho.subscriber;


import java.io.IOException;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;


public class SubscriberRunnner {

    public static Logger logger = Logger.getLogger("SubscriberLogger");
    static FileHandler fh;

    public static void main(final String[] args) {
        try {
            Format dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss");
            String sDate = dateFormatter.format(new Date());
            fh = new FileHandler("subscriber"+ sDate +".log");
            logger.addHandler(fh);
            SimpleFormatter formatter = new SimpleFormatter();
            fh.setFormatter(formatter);
            int startingTopicId = Integer.parseInt(arg(args,0,"0"));
            int totalSubscribers = Integer.parseInt(arg(args, 1, "1"));
            int globalCyclesCount = Integer.parseInt(arg(args, 2, "1"));
            String host = arg(args, 3, "127.0.0.1");
            String topicPrefix = arg(args, 4, "/mqtt_test_topic_");
            String user = arg(args, 5, "admin");
            String password = arg(args, 6, "admin");
            int port = 1883;

            logger.info("Paho subscriber started");
            logger.info("Starting topic ID: " + startingTopicId);
            logger.info("Total subscribers: " + totalSubscribers);
            logger.info("Topic prefix: " + topicPrefix);

            for(int iteration = 0; iteration <= globalCyclesCount; iteration++) {
                ArrayList<Subscriber> threadsList = new ArrayList<Subscriber>();
                logger.info("Cycle iteration " + iteration + " from " + globalCyclesCount);

                for (int topicId = startingTopicId; topicId <= startingTopicId + totalSubscribers; topicId++) {
                    Subscriber thread = new Subscriber(topicPrefix, topicId, user, password, host, port);
                    thread.start();
                    threadsList.add(thread);
                    Thread.sleep(500);
                }

                Thread.sleep(8000);

                for (Subscriber thread : threadsList) {
                    thread.disconnect();
                    thread.finish();
                    Thread.sleep(500);
                }

                threadsList.clear();
                threadsList = null;
                Thread.sleep(8000);
            }

            System.in.read();
        } catch (IOException e) {
            logger.log(Level.ALL, e.getMessage(), e);
            e.printStackTrace();
        } catch (InterruptedException e) {
            logger.log(Level.ALL, e.getMessage(), e);
            e.printStackTrace();
        } catch (Exception e){
            logger.log(Level.ALL, e.getMessage(), e);
            e.printStackTrace();
        }
        finally {
            SubscriberRunnner.logger.info("Execution finished");
        }
    }

    private static String arg(String []args, int index, String defaultValue) {
        if( index < args.length )
            return args[index];
        else
            return defaultValue;
    }
}
