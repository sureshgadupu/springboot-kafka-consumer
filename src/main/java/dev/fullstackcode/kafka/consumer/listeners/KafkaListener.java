package dev.fullstackcode.kafka.consumer.listeners;

import dev.fullstackcode.kafka.consumer.dto.Employee;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.stereotype.Component;


@Component
//@EnableKafka
public class KafkaListener {


    @org.springframework.kafka.annotation.KafkaListener(groupId ="groups", topics = "create-employee-events" )
    public void listen(Employee data) {
        System.out.println(data);

    }
}
