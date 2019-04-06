# kafka-reliable-consumers
kafka-reliable-consumers

ok message
```
{
    "id" : "{{uuid}}",
    "order" : {{count}},
    "createdAt" : "{{timestamp}}",
    "action" : "foo"
}
```

retry message
```
{
    "id" : "{{uuid}}",
    "order" : {{count}},
    "createdAt" : "{{timestamp}}",
    "action" : "retry3"
}
```