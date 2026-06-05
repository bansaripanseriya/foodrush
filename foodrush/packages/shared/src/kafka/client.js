const kafka = require('kafka-node');

class kafkaConsumer {
    // constructor to initialize the Kafka client
    construcor(clientId, brokers) {
        this.kafka = new kafka({
            clientId,
            brokers : brokers.split(','),
            logLevel : kafka.LOG_LEVEL.WARN,
            retry : {
                initialRetryTime : 100,
                maxRetryTime : 1000,
                retries : 3,
            },
        });
    }
}

async function createProducer(topic) {
    // create a producer for the given topic
    const producer = this.kafka.producer({
        allowAutoTopicCreation : true,
        transactionTimeout : 10000,
    });

    await producer.connect();
    console.log(`Producer connected to topic ${topic}`);
    return producer;
}   

async function createConsumer(groupId) {
    // create a consumer for the given groupId
    const consumer = this.kafka.consumer({
        groupId,
        autoCommit : true,
        fetchMaxWaitMs : 1000,
        fetchMaxBytes : 1024 * 1024,
    });

    await consumer.connect();
    console.log(`Consumer connected to topic ${topic} in group ${groupId}`);
    return consumer;
}