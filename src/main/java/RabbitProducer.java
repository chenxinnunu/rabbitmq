import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * rabbitmq消息生产者
 *
 * @author chenxinnunu@gmail.com
 * @date 2019/2/15 15:19
 */
public class RabbitProducer {
    private static final String EXCHANGE_NAME = "exchange_demo";
    private static final String ROUTING_KEY = "routingkey_demo";
    private static final String QUEUE_NAME = "queue_demo";
    private static final String IP_ADDRESS = "119.29.222.73";
    private static final int PORT = 5672;//RabbitMQ服务默认端口号为5672
    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(IP_ADDRESS);
        factory.setPort(PORT);
        factory.setUsername("admin");
        factory.setPassword("123456");
        //创建连接
        Connection connection = factory.newConnection();
        //创建信道
        Channel channel = connection.createChannel();
        //创建一个type="direct"、持久化的、非自动删除的交换器
        //参数1：交换器名称，2：交换器类型，3：是否持久化，4：是否自动删除，5：其他一些结构化参数
        channel.exchangeDeclare(EXCHANGE_NAME, "direct", true, false, null);
        //创建一个持久化、非排他的、非自动删除的队列
        //参数1：队列名称，2：是否持久化，3：是否排他，4：是否自动删除，5：队列的其他参数
        channel.queueDeclare(QUEUE_NAME, true, false, false, null);
        //将交换器与队列通过路由键绑定
        //这里本该使用BindingKey，但是使用了ROUTING_KEY，就是说交换器类型为dircet时，
        //ROUTING_KEY和BindingKey是同一个东西，要完全匹配，在topic交换器类型下，两者是模糊匹配
        //大多数情况下习惯性地将 BindingKey 写成 RoutingKey
        //参数1：队列名称，2：交换器的名称，3：用来绑定队列和交换器的路由键
        channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, ROUTING_KEY);
        String message = "Hello World!";
        //发送消息
        //参数1：交换器名称，2：路由键，3：消息的基本属性集，4：消息体，真正发送的消息
        //还有mandatory和immediate两个参数，3.0版本之后去掉了immediate参数，
        //mandatory为true时，如果消息没有匹配到队列，会将消息返回给生产者,false时表示直接丢弃
        channel.basicPublish(EXCHANGE_NAME, ROUTING_KEY, /*true,*/
                MessageProperties.PERSISTENT_TEXT_PLAIN,
                message.getBytes());
        //通过调用channel.addReturnListener添加ReturnListener监昕器获取到没有被正确路由到合适队列的消息
 /*       channel.addReturnListener(new ReturnListener() {
                                      public void handleReturn(int replyCode, String replyText,
                                                               String exchange, String routingKey,
                                                               AMQP.BasicProperties basicProperties,
                                                               byte[] body) throws IOException {
                                          String message = new String(body);
                                          System.out.println( "Basic.Return 返回的结果是: "+message);
                                      }
                                  });*/
            //关闭资源
        channel.close();
        connection.close();
        //如果设置了 mandatory 参数，那么需要添加 ReturnListener 的编程逻辑，生产者的代码将
        //变得复杂。如果既不想复杂化生产者的编程逻辑，又不想消息丢失，
        // 那么可以使用备份交换器，
        // 这样可以将未被路由的消息存储在 RabbitMQ 中，再在需要的时候去处理这些消息。
    }
}
