package pl.ynleborg.kafka.reliableconsumers;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class KafkaConsumer {

    @KafkaListener(topics = "${topic.main}",
            containerFactory = "messageKafkaListenerFactory")
    public void consumeJson(Message message) {
        log.info("Consumed Message = {}", message);
        if (message.getAction() != null && message.getAction().startsWith("retry")) {
            log.warn("Message is broken, sending to retry topic");
        }
    }
}