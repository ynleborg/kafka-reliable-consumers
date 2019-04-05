package pl.ynleborg.kafka.reliableconsumers;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Date;

@Service
@Slf4j
@AllArgsConstructor
public class KafkaConsumer {
    private static final Format FORMATTER = new SimpleDateFormat("yyyy-MM-dd");

    private KafkaTemplate<String, Message> kafkaTemplate;

    private ObjectMapper objectMapper;

    @Value("${topic.retry}")
    private String topicRetry;

    @Value("${topic.dlq}")
    private String topicDlq;

    @KafkaListener(topics = "${topic.main}")
    public void consumeFromMainTopic(String message) {
        log.info("Consumed Message = {}", message);
        Message serializedMessage;
        try {
            serializedMessage = objectMapper.readValue(message, Message.class);
            if (serializedMessage.getAction() != null && serializedMessage.getAction().startsWith("retry")) {
                log.warn("Message is broken, sending to {}", topicRetry);
                serializedMessage.setStatus("RETRAYABLE");
                kafkaTemplate.send(topicRetry, FORMATTER.format(new Date()), serializedMessage);
            }
        } catch (Exception e) {
            log.error("Cannot handle message {}", e.getMessage(), e);
        }
    }

    @KafkaListener(topics = "${topic.retry}")
    public void consumeFromRetryTopic(String message) {
        log.info("Consumed Message = {}", message);
        Message serializedMessage;
        try {
            serializedMessage = objectMapper.readValue(message, Message.class);
            long retries = Long.parseLong(serializedMessage.getAction().replace("retry", "0"));
            long loop = 0;
            while (loop <= retries) {
                log.info("Retrying message in loop {}", loop);
                loop++;
                Thread.sleep(loop * 1000);
            }
            log.warn("Message is completely broken, sending to {}", topicDlq);
            serializedMessage.setStatus("BROKEN");
            kafkaTemplate.send(topicDlq,  FORMATTER.format(new Date()), serializedMessage);

        } catch (Exception e) {
            log.error("Cannot handle message {}", e.getMessage(), e);
        }
    }

    @KafkaListener(topics = "${topic.dlq}")
    public void consumeFromDlqTopic(String message) {
        log.error("Consumed Message and Triggering onDuty = {}", message);
    }
}