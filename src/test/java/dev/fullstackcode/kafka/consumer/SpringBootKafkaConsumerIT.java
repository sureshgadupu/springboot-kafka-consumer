package dev.fullstackcode.kafka.consumer;


import dev.fullstackcode.kafka.consumer.dto.Employee;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;


@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@DirtiesContext
@ExtendWith(OutputCaptureExtension.class)
public class SpringBootKafkaConsumerIT {
    private static final Logger logger =  LoggerFactory.getLogger(SpringBootKafkaConsumerIT.class);

    static KafkaContainer kafka;

    static {
        kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"));
        kafka.start();
    }

    @Autowired
    private KafkaAdmin admin;




    @Test
    public void testPublishEmployee(CapturedOutput output) throws IOException, InterruptedException, ExecutionException {
        // first create the create-employee-events topic
        String topicName = "create-employee-events";
        NewTopic  topic1 =  TopicBuilder.name(topicName).build();

        AdminClient client = AdminClient.create(admin.getConfigurationProperties());
        client.createTopics( Collections.singletonList(topic1));

        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        KafkaProducer<Integer, Employee> producer = new KafkaProducer(props);

        Employee emp = new Employee();
        emp.setId(1);
        emp.setName("Test");

        producer.send(new ProducerRecord<>(topicName, emp.getId(), emp)).get();

        Thread.sleep(1000);
        Assertions.assertThat(output).contains("Employee{id=1, name='Test'}");


    }

    @DynamicPropertySource
    public static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers",kafka::getBootstrapServers);


    }

}
