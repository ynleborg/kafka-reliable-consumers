package pl.ynleborg.kafka.reliableconsumers.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import pl.ynleborg.kafka.reliableconsumers.Message;
import pl.ynleborg.kafka.reliableconsumers.RetryableMessage;

import java.text.SimpleDateFormat;
import java.util.Date;

import static pl.ynleborg.kafka.reliableconsumers.KafkaConfiguration.POSTMAN_RESOURCE_URL;

@Service
@Slf4j
@AllArgsConstructor
public class KafkaConsumer {
    private KafkaTemplate<String, String> kafkaTemplate;

    private ObjectMapper objectMapper;

    private RestTemplate restTemplate;

    @Value("${topic.retry}")
    private String topicRetry;

    @Value("${topic.out}")
    private String topicOut;


    @KafkaListener(topics = "${topic.main}")
    public void consumeFromMainTopic(String message,
                                     @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
                                     @Header(KafkaHeaders.OFFSET) String offset) throws JsonProcessingException {
        log.info("Consume from main topic [key={}, offset={}, message={}]", key, offset, message);
        Message serializedMessage;
        try {
            serializedMessage = objectMapper.readValue(message, Message.class);
            restTemplate.getForEntity(POSTMAN_RESOURCE_URL + serializedMessage.getAction(), String.class);
            kafkaTemplate.send(topicOut, objectMapper.writeValueAsString(serializedMessage));
            log.info("Done processing [key={}, offset={}]", key, offset);
        } catch (Exception e) {
            log.error("Cannot handle message: {}", e.getMessage());
            copyMessageToRetry(message, key, offset);
        }
    }

    private void copyMessageToRetry(String originalMessage, String originalKey, String originalOffset) throws JsonProcessingException {
        String key = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
        log.warn("Copying message [target={}, key={}]", topicRetry, key);
        kafkaTemplate.send(topicRetry, key, objectMapper.writeValueAsString(RetryableMessage.builder()
                .originalMessage(originalMessage)
                .originalKey(originalKey)
                .originalOffset(originalOffset)
                .build()));
    }
}
