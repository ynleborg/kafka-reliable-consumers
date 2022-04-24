package pl.ynleborg.kafka.reliableconsumers.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import pl.ynleborg.kafka.reliableconsumers.Message;
import pl.ynleborg.kafka.reliableconsumers.RetryableMessage;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;

import static pl.ynleborg.kafka.reliableconsumers.KafkaConfiguration.POSTMAN_RESOURCE_URL;

@Service
@Slf4j
@AllArgsConstructor
public class RetryKafkaConsumer {
    private KafkaTemplate<String, String> kafkaTemplate;

    private ObjectMapper objectMapper;

    private RestTemplate restTemplateForRetrying;

    @Value("${topic.dlq}")
    private String topicDlq;

    @KafkaListener(topics = "${topic.retry}")
    public void consumeFromRetryTopic(String message,
                                      @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
                                      @Header(KafkaHeaders.OFFSET) String offset) {
        log.info("Consume from retry topic [key={}, offset={}, message={}]", key, offset, message);
        RetryableMessage retryableMessage;
        String exceptionClass = null;
        long loop = 1;
        boolean success = false;
        while (loop <= 3 && !success) {
            try {
                retryableMessage = objectMapper.readValue(message, RetryableMessage.class);
                Message originalMessage = objectMapper.readValue(retryableMessage.getOriginalMessage(), Message.class);
                Thread.sleep(loop * 500);
                log.info("Retrying message in loop {}", loop);
                restTemplateForRetrying.getForEntity(POSTMAN_RESOURCE_URL + originalMessage.getAction(), String.class);
                success = true;
                log.info("Done processing [key={}, offset={}]", key, offset);
            } catch (Exception e) {
                Thread.currentThread().interrupt();
                log.error("Cannot handle message: {}", e.getMessage());
                exceptionClass = e.getClass().getName();
            }
            loop++;
        }
        if (!success) {
            copyMessageToDlq(message, exceptionClass);
        }
    }

    private void copyMessageToDlq(String message, String exceptionClass) {
        String key = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
        log.warn("Copying message [target={}, key={}]", topicDlq, key);
        ProducerRecord<String, String> dlqMessage = new ProducerRecord<>(topicDlq, key, message);
        dlqMessage.headers().add("exception", exceptionClass.getBytes(StandardCharsets.UTF_8));
        kafkaTemplate.send(dlqMessage);
    }

}
