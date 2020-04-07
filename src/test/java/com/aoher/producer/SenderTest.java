package com.aoher.producer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThat;
import static org.springframework.kafka.test.assertj.KafkaConditions.key;
import static org.springframework.kafka.test.hamcrest.KafkaMatchers.hasValue;

@RunWith(SpringRunner.class)
@SpringBootTest
@DirtiesContext
public class SenderTest {

    private static final Logger log = LoggerFactory.getLogger(SenderTest.class);

    private static final String SENDER_TOPIC = "foo.t";

    @Autowired
    private Sender sender;

    private KafkaMessageListenerContainer<String, String> container;
    private BlockingQueue<ConsumerRecord<String, String>> records;

    @ClassRule
    public static final KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, SENDER_TOPIC);

    @Before
    public void setUp() throws Exception {
        Map<String, Object> consumerProperties = KafkaTestUtils.consumerProps("sender", "false", embeddedKafka);
        DefaultKafkaConsumerFactory<String, String> consumerFactory =
                new DefaultKafkaConsumerFactory<>(consumerProperties);

        ContainerProperties containerProperties = new ContainerProperties(SENDER_TOPIC);

        container = new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);
        records = new LinkedBlockingQueue<>();

        container.setupMessageListener((MessageListener<String, String>) record -> {
            log.debug("test-listener received message='{}'", record);
            records.add(record);
        });

        container.start();
        ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());
    }

    @After
    public void tearDown() {
        container.stop();
    }

    @Test
    public void send() throws InterruptedException {
        String greeting = "Hello Spring Kafka Sender!";
        sender.send(greeting);

        ConsumerRecord<String, String> received = records.poll(10, TimeUnit.SECONDS);
        assertThat(received, hasValue(greeting));
        assertThat(received).has(key(null));
    }
}