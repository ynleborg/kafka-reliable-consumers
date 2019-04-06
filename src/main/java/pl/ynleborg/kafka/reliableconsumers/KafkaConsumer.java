package pl.ynleborg.kafka.reliableconsumers;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import java.text.SimpleDateFormat;
import java.util.Date;

@Service
@Slf4j
@AllArgsConstructor
public class KafkaConsumer {
    private KafkaTemplate<String, Message> kafkaTemplate;

    private ObjectMapper objectMapper;

    @Value("${topic.retry}")
    private String topicRetry;

    @KafkaListener(topics = "${topic.main}")
    public void consumeFromMainTopic(String message,
                                     @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
                                     @Header(KafkaHeaders.OFFSET) String offset) {
        log.info("consumeFromMainTopic [key={}, offset={}, message={}]", key, offset, message);
        Message serializedMessage;
        try {
            serializedMessage = objectMapper.readValue(message, Message.class);
            if (serializedMessage.getAction() != null && serializedMessage.getAction().startsWith("retry")) {
                copyMessageToRetry(key, serializedMessage);
            }
        } catch (Exception e) {
            log.error("Cannot handle message {}", e.getMessage(), e);
        }
    }

    private void copyMessageToRetry(String key, Message serializedMessage) {
        log.warn("Message is broken, sending to {}", topicRetry);
        serializedMessage.setStatus("Retrying of " + key);
        kafkaTemplate.send(topicRetry, new SimpleDateFormat("yyyy-MM-dd").format(new Date()), serializedMessage);
    }
}
