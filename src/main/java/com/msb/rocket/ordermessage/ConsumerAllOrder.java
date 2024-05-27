package com.msb.rocket.ordermessage;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.List;

//集群消费：
public class ConsumerAllOrder {
    public static void main(String[] args) throws Exception {
        // 实例化消费者--推模式--订阅模式
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("order_consumer");
        // 指定Namesrv地址信息.
        consumer.setNamesrvAddr("127.0.0.1:9876");
        //集群模式消费（默认就是，所以可以不用写）
        consumer.setMessageModel(MessageModel.CLUSTERING);
        // 订阅Topic
        consumer.subscribe("Order_Topic_Dong", "*"); //tag  tagA|TagB|TagC
        //这里是消费者从哪里开始
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);//从最早的偏移量开始消费

        // 注册回调函数(顺序消费模式)，处理消息
        consumer.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs,
                                                            ConsumeOrderlyContext context) {
                try {
                    for(MessageExt msg : msgs) {
                        String topic = msg.getTopic();
                        String msgBody = new String(msg.getBody(), "utf-8");
                        String tags = msg.getTags();
                        System.out.println("收到消息：" + " topic :" + topic + " ,tags : " + tags + " ,msg : " + msgBody);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                }
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });

        //启动消息者
        consumer.start();
        //注销Consumer
        //consumer.shutdown();
        System.out.printf("Consumer Started.%n");
    }
}
