package jmsplaying;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.io.IOException;
import java.util.Date;

import static java.lang.Thread.sleep;

public class Receiver implements MessageListener, ExceptionListener {

    private Connection connection;
    private Session session;
    private final ConnectionFactory connectionFactory;
    private volatile boolean stopped = false;

    public static void main(String[] args) throws JMSException, IOException {
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");

        final Receiver receiver = new Receiver(connectionFactory);

        new Thread(new Runnable() {
            @Override
            public void run() {
                receiver.connectWithRetry();
            }
        }).start();


        System.in.read();
        log("Stopping....");
        receiver.stop();
        log("Exiting");
    }

    public Receiver(ConnectionFactory connectionFactory) throws JMSException {
        this.connectionFactory = connectionFactory;
    }

    private void connect() throws JMSException {
        synchronized (this) {
            connection = connectionFactory.createConnection();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic topic = session.createTopic("my.topic");
            MessageConsumer consumer = session.createConsumer(topic);
            consumer.setMessageListener(this);
            connection.setExceptionListener(this);
            connection.start();
        }
    }

    @Override
    public void onMessage(Message message) {
        TextMessage textMessage = (TextMessage) message;
        try {
            String text = textMessage.getText();
            log("Received: " + text);
        } catch (JMSException e) {
            log("Error: " + e);
        }
    }

    public void stop() throws JMSException {
        synchronized (this) {
            stopped = true;
            if (connection != null) {
                connection.setExceptionListener(null);
                try {
                    connection.stop();
                    connection.close();
                } catch (Exception ex) {
                    log("Exception while closing connection. Doesn't matter. " + ex);
                }
            }
        }
    }

    private static void log(String message) {
        String threadId = "[" + Thread.currentThread().getName() + "] ";
        String time = new Date().toString() + ": ";
        System.out.println(threadId + time + message);
    }

    @Override
    public void onException(JMSException exception) {
        connectWithRetry();
    }

    private void connectWithRetry() {
        while (!stopped) {
            try {
                log("Trying to reconnect; stopped = " + stopped);
                connect();
                break;
            } catch (JMSException e) {
                log("Error while handling error: " + e);
                try {
                    sleep(5000);
                } catch (InterruptedException e1) {
                    throw new RuntimeException(e1);
                }
            }
        }
    }

}
