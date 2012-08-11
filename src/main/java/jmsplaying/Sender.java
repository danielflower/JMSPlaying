package jmsplaying;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.util.Date;

import static java.lang.Thread.sleep;

public class Sender implements ExceptionListener {
    private volatile Session session;
    private volatile MessageProducer producer;
    private final ConnectionFactory connectionFactory;

    public static void main(String[] args) throws JMSException, InterruptedException {
        ConnectionFactory cf = new ActiveMQConnectionFactory("tcp://localhost:61616");
        Sender sender = new Sender(cf);
        sender.run();
    }

    public Sender(ConnectionFactory connectionFactory) throws JMSException, InterruptedException {
        this.connectionFactory = connectionFactory;
        connectWithRetry();
    }

    private void run() throws JMSException, InterruptedException {
        while (true) {
            try {
                String message = "Hello at " + new Date();
                long start = System.currentTimeMillis();
                synchronized (this) {
                    producer.send(session.createTextMessage(message));
                }
                log("Sent " + message + " in " + (System.currentTimeMillis() - start) + "ms");
                sleep(1000);
            } catch (Exception ex) {
                log("Exception while sending.  Will sleep a bit. " + ex);
                Thread.sleep(2000);
            }
        }
    }

    private void log(String message) {
        String threadId = "[" + Thread.currentThread().getName() + "] ";
        String time = new Date().toString() + ": ";
        System.out.println(threadId + time + message);
    }

    @Override
    public void onException(JMSException exception) {
        log("Detected exception " + exception);
        connectWithRetry();
    }


    private void connect() throws JMSException {
        synchronized (this) {
            Connection connection = connectionFactory.createConnection();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            producer = session.createProducer(session.createTopic("my.topic"));
            connection.setExceptionListener(this);
            connection.start();
        }
    }

    private void connectWithRetry() {
        while (true) {
            try {
                log("Trying to reconnect");
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
