import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * rabbitmq消费者
 *
 * @author chenxinnunu@gmail.com
 * @date 2019/2/15 16:00
 */
public class RabbitConsumer {
    private static final String QUEUE_NAME = "queue_demo";
    private static final String IP_ADDRESS = "119.29.222.73";
    private static final int PORT = 5672;

    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
        Address[] addresses = new Address[]{
                new Address(IP_ADDRESS, PORT)
        };
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername("admin");
        factory.setPassword("123456");
        //创建连接；这里的连接方式和生产者的demo略有不同，注意区别
        Connection connection = factory.newConnection(addresses);
        //创建信道
        final Channel channel = connection.createChannel();
        //设置客户端最多接收未被ack（确认）的消息个数
        channel.basicQos(64);
        //接收消息一般通过实现Consumer接口或者继承DefaultConsumer类实现
        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag,
                                       Envelope envelope,
                                       AMQP.BasicProperties properties,
                                       byte[] body) throws IOException {
                System.out.println("recv message: " + new String(body));
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                //显式的确认消息已被成功接收，false表示不自动确认
                channel.basicAck(envelope.getDeliveryTag(), false);
            }
        };
        //这里是推（push）模式消费，推模式会不断的收到RabbitMQ的推送消息，直到取消队列订阅或者受到basicQos的限制
        // 拉（pull）模式消费调用Basic.Get,拉模式单条额获取消息，
        channel.basicConsume(QUEUE_NAME, consumer);
        //等待回调函数执行完毕之后，关闭资源
        TimeUnit.SECONDS.sleep(5);
        channel.close();
        connection.close();
    }
}
