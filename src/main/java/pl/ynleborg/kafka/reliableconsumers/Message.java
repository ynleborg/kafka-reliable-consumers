package pl.ynleborg.kafka.reliableconsumers;

import lombok.Data;

@Data
public class Message {
    String id;
    String action;
}
