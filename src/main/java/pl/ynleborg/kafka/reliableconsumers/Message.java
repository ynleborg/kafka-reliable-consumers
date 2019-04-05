package pl.ynleborg.kafka.reliableconsumers;

import lombok.Data;

import java.util.Date;

@Data
public class Message {
    String id;
    Long order;
    Date createdAt;
    String action;
    String status;
}
