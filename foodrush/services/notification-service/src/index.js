const { KafkaClient } = require('@foodrush/shared/kafka/client');
const { TOPICS } = require('@foodrush/shared/kafka/topics');

const kafkaClient = new KafkaClient('notification-service', process.env.KAFKA_BROKERS);

const transporter = nodemailer.createTransporter({
    host: proces.env.SMTP_HOST,
    port: 587,
    auth: {
        user: process.env.SMTP_USER,
        pass: process.env.SMTP_PASS,
    }
});

const handlers = {
    [TOPICS.ORDER_CREATED]: async (event) => {
        console.log(`[Notifications] Order ${event.orderId} created, sending confirmation`);
        await transporeter.sendMail({
            from: 'no-reply@foodrush.app',
            to: event.userEmail,
            subject: `Order #${event.orederId} confirmed!`,
            html: `<h2>Your order is confirmed</h2><p>Total: ₹${event.total}</p>`,
        });
    },

    [TOPICS.ORDER_STATUS_UPDATED]: async (event) => {
        const messages = {
            confirmed:  'Restaurant has confirmed your order! 🍽️',
            preparing:  'Your food is being prepared 👨‍🍳',
            ready:      'Order ready! Driver on the way 🛵',
            picked_up:  'Your order is on its way! 🚀',
            delivered:  'Delivered! Enjoy your meal 🎉',
        };

        if(messages[event.status]) {
            console.log(`[Notifications] Push: ${messages[event.status]}`);
            // In Production: call FCM/APNS here
        }
    },

    [TOPICS.DELIVERY_ASSIGNED]: async (event) => {
        console.log(`[Notifications] Driver ${event.driverId} assigned to order ${event.orderId}`);
    },

};

async function startConsumer() {
    const consumer = await kafkaClient.createConsumer('notification-service-group');

    await consumer.subscribe({
        topic: Object.keys(handlers),
        fromBeginning: false,
    });

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            try {
                const event = JSON.parse(message.value.toString());
                const handler = handlers[topic];
            } catch(err) {
                console.error(`[Notifications] Failed to process ${topic}:`, err);
                // In production: push to dead-letter queue
            }
        },
    });

    console.log(`[Notifications] Consumer running, waiting for events...`);
}

startConsumer().catch(console.error);