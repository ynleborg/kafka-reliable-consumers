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
public class RetryKafkaConsumer {
    private KafkaTemplate<String, Message> kafkaTemplate;

    private ObjectMapper objectMapper;

    @Value("${topic.dlq}")
    private String topicDlq;

    @KafkaListener(topics = "${topic.retry}")
    public void consumeFromRetryTopic(String message,
                                      @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
                                      @Header(KafkaHeaders.OFFSET) String offset) {
        log.info("consumeFromRetryTopic [key={}, offset={}, message={}]", key, offset, message);
        Message serializedMessage;
        try {
            serializedMessage = objectMapper.readValue(message, Message.class);
            long loop = 1;
            while (loop <= getNumberOfRetries(serializedMessage)) {
                Thread.sleep(loop * 1000);
                log.info("Retrying message in loop {}", loop);
                loop++;
            }
            copyMessageToDlq(serializedMessage);

        } catch (Exception e) {
            log.error("Cannot handle message {}", e.getMessage(), e);
        }
    }

    private void copyMessageToDlq(Message serializedMessage) {
        log.info("Message is completely broken, sending to {}", topicDlq);
        kafkaTemplate.send(topicDlq, new SimpleDateFormat("yyyy-MM-dd").format(new Date()), serializedMessage);
    }

    private long getNumberOfRetries(Message serializedMessage) {
        return Long.parseLong(serializedMessage.getAction().replace("retry", "0"));
    }

}