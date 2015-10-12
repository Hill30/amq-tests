
package com.hill30.amqtests.openwire.publisher;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.util.Enumeration;
import java.util.concurrent.TimeUnit;

/**
 * @author <a href="http://www.christianposta.com/blog">Christian Posta</a>
 */
public class Subscriber {
    private static final String BROKER_HOST = "tcp://10.0.1.132:%d";
    private static final int BROKER_PORT = 61616;
    private static final String BROKER_URL = String.format(BROKER_HOST, BROKER_PORT);
    private static final Boolean NON_TRANSACTED = false;
    private static final long DELAY = 100;

    public static void main(String[] args) {

        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("admin", "admin", BROKER_URL);
        Connection connection = null;

        try {
            connection = connectionFactory.createConnection();
            connection.start();

            Session session = connection.createSession(NON_TRANSACTED, Session.AUTO_ACKNOWLEDGE);
            Queue destination = session.createQueue("Topic.overall.1443705457");

            /*QueueBrowser browser = session.createBrowser(destination);
            Enumeration enumeration = browser.getEnumeration();
            System.out.println("Enum: " + enumeration.toString());
            while (enumeration.hasMoreElements()) {
                TextMessage message = (TextMessage) enumeration.nextElement();
                //System.out.println("Browsing: " + message.getText());
                TimeUnit.MILLISECONDS.sleep(DELAY);
            }*/

            // Create a MessageConsumer from the Session to the Topic or Queue
            MessageConsumer consumer = session.createConsumer(destination);
            int i = 0;
            // Wait for a message
            while(true) {
                Message message = consumer.receive(1000000000);
                if (message instanceof TextMessage) {
                    i++;
                    TextMessage textMessage = (TextMessage) message;
                    String text = textMessage.getText();
                    System.out.println("Received: " + text);
                } else {
                    //System.out.println("Received: " + message);
                    break;
                }
            }
            System.out.println("Received: " + i);
            consumer.close();
            session.close();

        } catch (Exception e) {
            System.out.println("Caught exception!");
        }
        finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (JMSException e) {
                    System.out.println("Could not close an open connection...");
                }
            }
        }
    }
}