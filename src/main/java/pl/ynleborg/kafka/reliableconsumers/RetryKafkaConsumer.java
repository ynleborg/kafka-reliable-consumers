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
import org.springframework.web.client.RestTemplate;

import java.text.SimpleDateFormat;
import java.util.Date;

import static pl.ynleborg.kafka.reliableconsumers.KafkaConfiguration.POSTMAN_RESOURCE_URL;

@Service
@Slf4j
@AllArgsConstructor
public class RetryKafkaConsumer {
    private KafkaTemplate<String, String> kafkaTemplate;

    private ObjectMapper objectMapper;

    public RestTemplate restTemplateForRetrying;

    @Value("${topic.dlq}")
    private String topicDlq;

    @KafkaListener(topics = "${topic.retry}")
    public void consumeFromRetryTopic(String message,
                                      @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
                                      @Header(KafkaHeaders.OFFSET) String offset) {
        log.info("Consume from retry topic [key={}, offset={}, message={}]", key, offset, message);
        Message serializedMessage;
        long loop = 1;
        boolean success = false;
        while (loop <= 3 && !success) {
            try {
                serializedMessage = objectMapper.readValue(message, Message.class);
                Thread.sleep(loop * 500);
                log.info("Retrying message in loop {}", loop);
                restTemplateForRetrying.getForEntity(POSTMAN_RESOURCE_URL + serializedMessage.getAction(), String.class);
                success = true;
                log.info("Done processing [key={}, offset={}]", key, offset);
            } catch (Exception e) {
                log.error("Cannot handle message {}", e.getMessage());
            }
            loop++;
        }
        if (!success) {
            copyMessageToDlq(message);
        }
    }

    private void copyMessageToDlq(String message) {
        String key = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
        log.warn("Copying message [target={}, key={}]", topicDlq, key);
        kafkaTemplate.send(topicDlq, key, message);
    }
}