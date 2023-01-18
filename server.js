const crypto            = require('crypto');
const kafkaJs           = require('kafkajs')
const fastifyEnv        = require('@fastify/env')
const fastify           = require('fastify')({ logger: false })

const UNAUTHORIZED      = '::UNAUTHORIZED::'
const KAFKA_CONN_ERR    = '::KAFKA CONNECTION ERROR::'

/**
 * env Schema for Fastify Env
 */
const envSchema = {
    type: 'object',
    required: [
        'APP_ENV',
        'APP_HOST',
        'APP_PORT',
        'SPELL_SECRET',
        'KAFKA_HOST',
        'KAFKA_PORT',
        'KAFKA_BROKER_1',
        'KAFKA_CLIENT_ID',
        'KAFKA_TOPIC'
    ],
    properties: {
        APP_ENV:            { type: 'string', default: 'dev' },
        APP_HOST:           { type: 'string', default: '0.0.0.0' },
        APP_PORT:           { type: 'number', default: 8000 },
        SPELL_SECRET:       { type: 'string', default: 'secret' },
        SPELL_TIMEOUT:      { type: 'number', default: 30 },
        KAFKA_HOST:         { type: 'string', default: 'kafka' },
        KAFKA_PORT:         { type: 'number', default: 9092 },
        KAFKA_BROKER_1 :    { type: 'string', default: 'kafka:9092' },
        KAFKA_CLIENT_ID:    { type: 'string', default: 'bl-kafka' },
        KAFKA_TOPIC:        { type: 'string', default: 'test-topic' },
    }
};

/**
 * Object
 * Env Configurations
 */
const envOptions = {
    confKey: 'config',
    dotenv: true,
    schema: envSchema,
    data: process.env
};

/**
 * Keeps Kafka client objec available throughout the APP CONTEXT
 * @param {*} _ 
 * @returns 
 */
const kafkaDecorater = _ => fastify.decorate('kafka', kafkaProducerInitiate())

/**
 * Starts Fastify App Server
 * @param {*} _ 
 * @returns NULL
 */
const fastifyServerStart = _ => fastify.listen({ host: fastify.config.APP_HOST, port: fastify.config.APP_PORT }, err => err && fastify.log.error(err) & process.exit(1))

/**
 * Initiates Kafka Producer
 * @param {*} _ 
 * @returns Promise
 */
const kafkaProducerInitiate = _ => {
    try { return (new kafkaJs.Kafka({ clientId: fastify.config.KAFKA_CLIENT_ID, brokers: [fastify.config.KAFKA_BROKER_1] })).producer() } 
    catch(e) { console.log(KAFKA_CONN_ERR, e) }
}

/**
 * Checks token and timestamp
 * @param {*} spell 
 * @returns Promise
 */
const authOk = spell => new Promise((res, rej) => {
    const [signature, timestamp] = spell.split('.')
    signature === crypto
        .createHmac('sha256', fastify.config.SPELL_SECRET)
        .update(timestamp)
        .digest('hex')
    && Math.round(Date.now()/1000 - timestamp/1000) <= fastify.config.SPELL_TIMEOUT
    ? res(true)
    : rej(UNAUTHORIZED)
})

/**
 * Produces message to kafka broker
 * @param {*} message 
 * @returns Promise
 */
const produceMessage = message => fastify
    .kafka
    .connect()
    .then(_ => fastify.kafka.send({
        topic: fastify.config.KAFKA_TOPIC,
        messages: [
            { value: JSON.stringify(message) },
        ],
    }))
    .then(_ => fastify.kafka.disconnect())
    .catch(e => console.log(KAFKA_CONN_ERR, e))

/**
 * SERVER 
 * HOST: localhost
 * PORT: 8000
 * Initiates Kafka clients before starting server
 */    
fastify.register(fastifyEnv, envOptions).ready(err => err ? console.log(err) : kafkaDecorater() & fastifyServerStart())

/**
 * GET SAMPLE TOKEN
 * INPUT: NONE
 * OUTPUT: hMac concatenated with timestamp by '.'
 * OUTPUT FORMAT: 
 * {
 *      spell: hMac.timestamp 
 * }
 */
fastify.get('/', (req, rep) => {
    const timestamp = Date.now()
    return rep.code(200).send({ spell: crypto.createHmac('sha256', fastify.config.SPELL_SECRET).update(`${timestamp}`).digest('hex') + `.${timestamp}` })
})

/**
 * PRODUCE MESSAGE THROUGH KAFKA PRODUCER
 * INPUT: ANY JSON OBJECT
 * OUTPUT: OK
 * OUTPUT FORMAT: 
 * {
 *      status: 'OK' 
 * }
 */
fastify.post('/', (req, rep) => rep.code(200).send({ status: 'OK' }) & authOk(req.headers['spell']).then(_ => produceMessage(req.body)).catch(e => console.log(e)))
