package pl.ynleborg.kafka.reliableconsumers;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumer {

    @KafkaListener(topics = "${topic.main}")
    public void consume(String message) {
        System.out.println("Consumed message: " + message);
    }
}