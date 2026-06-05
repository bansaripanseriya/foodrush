const { kafka, logLevel } = require('kafkajs')

class KafkaClient {
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

    async createProducer(topic) {
        // create a producer for the given topic
        const producer = this.kafka.producer({
            allowAutoTopicCreation : true,
            transactionTimeout : 10000,
        });

        await producer.connect();
        console.log(`Producer connected to topic ${topic}`);
        return producer;
    }   

    async createConsumer(groupId) {
        // create a consumer for the given groupId
        const consumer = this.kafka.consumer({
            groupId,
            autoCommit : true,
            sessionTimeout : 10000,
            heartbeatInterval : 3000
        });

        await consumer.connect();
        console.log(`Consumer connected to topic ${topic} in group ${groupId}`);
        return consumer;
    }s

}

module.exports = { kafkaClient };