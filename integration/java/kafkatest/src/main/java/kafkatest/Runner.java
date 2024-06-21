/*
 * Copyright 2024 The Tektite Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafkatest;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class Runner {

    public static void main(String[] args) {

        String testName = null;
        String serverAddress = null;
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if ("-testname".equals(arg) && i != args.length - 1) {
                testName = args[i + 1];
            } else if ("-address".equals(arg) && i != args.length - 1) {
                serverAddress = args[i + 1];
            }
        }
        if (testName == null) {
            System.err.println("no -testName arg provided");
            System.exit(1);
        }
        if (serverAddress == null) {
            System.err.println("no -address arg provided");
            System.exit(1);
        }

        try {
            new Runner().run(testName, serverAddress);
        } catch (Exception e) {
            System.err.println("failed to execute test " + e);
            e.printStackTrace();
            System.exit(1);
        }
    }

    private void run(String testName, String serverAddress) throws Exception {
        switch (testName) {
            case "producer_endpoint": {
                runProducerEndpointTest(serverAddress);
                return;
            }
            case "consumer_endpoint": {
                runConsumerEndpointTest(serverAddress);
                return;
            }
            default: {
                throw new Exception("invalid testName: " + testName);
            }
        }
    }

    private void runProducerEndpointTest(String serverAddress) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddress);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Producer<String, String> producer = new KafkaProducer<>(props);

        int numMessages = 10;

        // topic1 has one partition so offsets should come back in sequence - we check this.
        sendMessages("topic1", numMessages, producer, false);

        sendMessages("topic2", 10, producer, false);
        sendMessages("topic3", 10, producer, false);

        sendMessages("table1", 10, producer, false);
        sendMessages("table2", 10, producer, false);

        System.out.println("sent messages");

        producer.close();
    }

    private void runConsumerEndpointTest(String serverAddress) throws Exception {

        System.out.println("in consumer endpoint test with serverAddress: " + serverAddress);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddress);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Producer<String, String> producer = new KafkaProducer<>(props);

        int numMessages = 100;

        // topic1 has one partition so offsets should come back in sequence - we check this.
        sendMessages("topic1", numMessages, producer, false);

        System.out.println("sent messages");

        producer.close();

        // Create consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddress);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test_group");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // Create Kafka consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        System.out.println("created consumer");

        // Subscribe to the topic
        consumer.subscribe(Collections.singleton("topic1"));

        System.out.println("subscribed topic");

        // Poll for new messages
        int numReceived = 0;
        while (numReceived < numMessages) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            if (records == null) {
                break;
            }
            for (ConsumerRecord<String, String> record : records) {
                if (record.offset() != numReceived) {
                    throw new Exception("invalid offset");
                }
                System.out.println("Received message: " +
                        "Topic: " + record.topic() +
                        ", Partition: " + record.partition() +
                        ", Offset: " + record.offset() +
                        ", Key: " + record.key() +
                        ", Value: " + record.value());
                numReceived++;
            }

        }
        System.out.println("Done");
        consumer.close();
    }

    private void sendMessages(String topic, int numMessages, Producer<String, String> producer, boolean checkOffsets) throws Exception {
        for (int i = 0; i < numMessages; i++) {
            String key = String.format("key%05d", i);
            String value = String.format("value%05d", i);
            RecordMetadata meta = producer.send(new ProducerRecord<>(topic, key, value)).get();
            if (checkOffsets) {
                if (!meta.hasOffset()) {
                    throw new Exception("record has no offset");
                }
                if (meta.offset() != (long)i) {
                    throw new Exception("invalid offset");
                }
                System.out.println("got offset " + i);
            }
        }
    }
}
