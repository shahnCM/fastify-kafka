const { Kafka } = require('kafkajs')

const kafka = new Kafka({
    clientId: 'nodejs-kafka-test',
    brokers: ['kafka:9092'] //['Ip:Port']
})

const consumer = kafka.consumer({ groupId: 'test-group' })

consumer.connect()
    .then(_ => consumer.subscribe({ topic: 'test-topic', fromBeginning: true }))
    .then(_ => consumer.run({ eachMessage: async ({ topic, partition, message }) => console.log('MSG: >>>', message.value.toString()) }))
