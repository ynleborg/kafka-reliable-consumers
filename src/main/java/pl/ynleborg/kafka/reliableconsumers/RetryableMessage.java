package pl.ynleborg.kafka.reliableconsumers;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RetryableMessage {

    private String originalMessage;

    private String originalKey;

    private String originalOffset;
}
