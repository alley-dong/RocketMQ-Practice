package com.msb.rocket.normal;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.List;

/**
 * 集群消费：并发监听 - 测试不同消费者组下 会接受所有的生产者的消息。如果消费组相同 则会平分生产者的消息。
 */
public class BalanceComuser2 {
    public static void main(String[] args) throws Exception {
        // 实例化消费者--推模式--订阅模式
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("dcl_balance_consumer");
        // 指定Namesrv地址信息.
        consumer.setNamesrvAddr("127.0.0.1:9876");
        //集群模式消费（默认就是，所以可以不用写）
        consumer.setMessageModel(MessageModel.CLUSTERING);
        //最大重试次数
        consumer.setMaxReconsumeTimes(10);
        // 订阅Topic
        consumer.subscribe("TopicTestDong-Cluster1", "*"); //tag  tagA|TagB|TagC
        //这里是消费者从哪里开始
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);//从最早的偏移量开始消费

        // 注册回调函数(并发消费模式)，处理消息
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                            ConsumeConcurrentlyContext context) {
                try {

                    for(MessageExt msg : msgs) {
                        String topic = msg.getTopic();
                        String msgBody = new String(msg.getBody(), "utf-8");
                        String tags = msg.getTags();
                        System.out.println("收到消息：" + " topic :" + topic + " ,tags : " + tags +
                                " ,msg : " + msgBody);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                    //程序返回 Null或者直接抛出异常，对于RocketMQ来说都是走重试
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS; //消费成功
//                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
        });

        //启动消息者
        consumer.start();
        //注销Consumer
        //consumer.shutdown();
        System.out.printf("Consumer Started.%n");
    }
}
