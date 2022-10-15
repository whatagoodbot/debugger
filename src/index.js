import { connect } from 'mqtt'
import fs from 'fs'

const mqttOptions = {
  port: process.env.MQTT_PORT,
  host: `mqtt://${process.env.MQTT_HOSTNAME}`,
  clientId: 'mqttjs_' + Math.random().toString(16).substr(2, 8),
  username: process.env.MQTT_USERNAME,
  password: process.env.MQTT_PASSWORD,
  keepalive: 60,
  reconnectPeriod: 1000,
  protocolId: 'MQIsdp',
  protocolVersion: 3,
  clean: true,
  encoding: 'utf8'
}

const client = connect(mqttOptions.host, mqttOptions)
const listenTopic = `${process.env.NODE_ENV}/broadcast`

client.on('connect', () => {
  client.subscribe(listenTopic, (err) => {
    if (err) return console.log(err, 'err')
  })
})

client.on('error', (error) => {
  writeToFile(`${new Date()}, ${error}`)
  console.log(error)
})

client.on('message', async (topic, message) => {
  const originalMessage = message.toString()
  const payload = JSON.parse(originalMessage)

  writeToFile(`${topic} - ${payload.messageId}, ${payload.meta.topic}, ${payload.message}, ${new Date()}, ${originalMessage}`)
})

const writeToFile = fileContent => {
  fs.appendFile('./broadcast.log', `${fileContent}\n`, err => {
    if (err) console.log('error',err)
  })
}