// This file contains the constants for the Kafka topics

const TOPICS = {
    ORDER_CREATED : 'order.created',
    ORDER_STATUS_UPDATED : 'order.status.updated',
    ORDER_CANCELLED : 'order.cancelled',
    DELIVERY_ASSIGNED : 'delivery.assigned',
    DELIVERY_STATUS_UPDATED : 'delivery.status.updated',
    PAYMENT_PROCESSED : 'payment.processed',
    RESTAURANT_INDEXED : 'restaurant.indexed',
    NOTIFICATION_SEND : 'notification.send',
};

module.exports = {kafkaClient, TOPICS};