package com.hill30.amqstresstest.periodicsub;

import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.UTF8Buffer;
import org.fusesource.mqtt.client.*;

import java.net.URISyntaxException;
import java.util.Date;
import java.util.logging.Level;


public class SubscriberSync {
    private String topicPerfix;
    private int topicId;
    private String user;
    private String password;
    private String host;
    private int port;
    private boolean isWorking;
    private CallbackConnection connection;

    public SubscriberSync(String topicPrefix, int topicId, String user, String password, String host, int port) {
        this.topicPerfix = topicPrefix;
        this.topicId = topicId;
        this.user = user;
        this.password = password;
        this.host = host;
        this.port = port;
    }

    public void disconnect() {
        connection.disconnect(null);
    }


    public void start() {
        SubscriberRunnner.logger.info("Subscribing to topic: " + topic());

        MQTT mqtt = new MQTT();
        try {
            mqtt.setHost(host, port);
            mqtt.setUserName(user);
            mqtt.setCleanSession(false);
            mqtt.setClientId(topic());
            mqtt.setPassword(password);

            connection = mqtt.callbackConnection();
            connection.listener(new Listener() {
                long count = 0;
                long start = System.currentTimeMillis();

                public void onConnected() {
                    SubscriberRunnner.logger.info((new Date()) + " " + topicId + " connected");
                }

                public void onDisconnected() {
                    SubscriberRunnner.logger.info((new Date()) + " " + topicId + " disconnected");
                }

                public void onFailure(Throwable value) {
                    value.printStackTrace();
                    System.exit(-2);
                }
                public void onPublish(UTF8Buffer topic, Buffer msg, Runnable ack) {

                    String body = msg.utf8().toString();

                    SubscriberRunnner.logger.info((new Date()) + "New message:" + body);

                    if( "SHUTDOWN".equals(body)) {
                        long diff = System.currentTimeMillis() - start;
                        SubscriberRunnner.logger.info(String.format("Received %d in %.2f seconds", count, (1.0 * diff / 1000.0)));
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
                        count ++;
                    }
                    ack.run();
                }
            });
            connection.connect(new Callback<Void>() {
                @Override
                public void onSuccess(Void value) {
                    Topic[] topics = { new Topic(topic(), QoS.EXACTLY_ONCE )};
                    connection.subscribe(topics, new Callback<byte[]>() {
                        public void onSuccess(byte[] qoses) {
                        }
                        public void onFailure(Throwable value) {
                            value.printStackTrace();
                            SubscriberRunnner.logger.log(Level.ALL, value.getMessage(), value);
                            disconnect();
                        }
                    });
                }
                @Override
                public void onFailure(Throwable value) {
                    value.printStackTrace();
                    disconnect();
                }
            });

        } catch (URISyntaxException e) {
            e.printStackTrace();
            SubscriberRunnner.logger.log(Level.ALL, e.getMessage(), e);
        } catch (Exception e) {
            SubscriberRunnner.logger.log(Level.ALL, e.getMessage(), e);
        }
    }

    private String topic(){
        return topicPerfix+topicId;
    }
}
