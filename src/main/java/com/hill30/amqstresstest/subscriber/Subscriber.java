package com.hill30.amqstresstest.subscriber;

import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.UTF8Buffer;
import org.fusesource.mqtt.client.*;

import java.io.Console;
import java.net.URISyntaxException;
import java.util.Date;

/**
 * Created by azavarin on 4/7/2015.
 */
public class Subscriber extends Thread {

    private String topicPerfix;
    private int topicId;
    private String user;
    private String password;
    private String host;
    private int port;
    private boolean isWorking;

    public Subscriber(String topicPrefix, int topicId, String user, String password, String host, int port) {
        this.topicPerfix = topicPrefix;
        this.topicId = topicId;
        this.user = user;
        this.password = password;
        this.host = host;
        this.port = port;
    }

    @Override
    public void run() {
        System.out.println("Subscribing to topic: " + topic());

        MQTT mqtt = new MQTT();
        try {
            mqtt.setHost(host, port);
            mqtt.setUserName(user);
            mqtt.setCleanSession(false);
            mqtt.setClientId(topic());
            mqtt.setPassword(password);

            final CallbackConnection connection = mqtt.callbackConnection();
            connection.listener(new org.fusesource.mqtt.client.Listener() {
                long count = 0;
                long start = System.currentTimeMillis();

                public void onConnected() {
                    System.out.println((new Date())+ " " +  topicId + " connected");
                }

                public void onDisconnected() {
                    System.out.println((new Date()) + " " + topicId + " disconnected");
                }

                public void onFailure(Throwable value) {
                    value.printStackTrace();
                    System.exit(-2);
                }
                public void onPublish(UTF8Buffer topic, Buffer msg, Runnable ack) {

                    String body = msg.utf8().toString();

                    System.out.println((new Date()) + "New message:" + body);

                    if( "SHUTDOWN".equals(body)) {
                        long diff = System.currentTimeMillis() - start;
                        System.out.println(String.format("Received %d in %.2f seconds", count, (1.0*diff/1000.0)));
                        connection.disconnect(new Callback<Void>() {
                            @Override
                            public void onSuccess(Void value) {
                                System.exit(0);
                            }
                            @Override
                            public void onFailure(Throwable value) {
                                value.printStackTrace();
                                System.exit(-2);
                            }
                        });
                    } else {
                        if( count == 0 ) {
                            start = System.currentTimeMillis();
                        }
//                        if( count % 100 == 0 ) {
//                            System.out.println(String.format("Received %d messages.", count));
//                        }
                        count ++;
                    }
                    ack.run();
                }
            });
            connection.connect(new Callback<Void>() {
                @Override
                public void onSuccess(Void value) {
                    Topic[] topics = {new Topic(topic(), QoS.EXACTLY_ONCE)};
                    connection.subscribe(topics, new Callback<byte[]>() {
                        public void onSuccess(byte[] qoses) {
                        }
                        public void onFailure(Throwable value) {
                            value.printStackTrace();
                            System.exit(-2);
                        }
                    });
                }
                @Override
                public void onFailure(Throwable value) {
                    value.printStackTrace();
                    System.exit(-2);
                }
            });

            // Wait forever..
            synchronized (Subscriber.class) {
                isWorking = true;
                while(isWorking)
                    Subscriber.class.wait();
            }

        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void finish(){
        isWorking = false;
    }

    private String topic(){
        return topicPerfix+topicId;
    }
}
