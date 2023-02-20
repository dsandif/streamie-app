const express = require("express")
const mongodb = require("mongodb")
const amqp = require('amqplib')
const bodyParser = require("body-parser")

if (!process.env.DBHOST) {
    throw new Error("Please specify the databse host using environment variable DBHOST.")
}

if (!process.env.DBNAME) {
    throw new Error("Please specify the name of the database using environment variable DBNAME")
}

if (!process.env.RABBIT) {
    throw new Error("Please specify the name of the RabbitMQ host using environment variable RABBIT")
}

const {
  DBHOST,
  DBNAME,
  RABBIT
} = process.env

//
// Connect to the database.
//
async function connectDb() {
  const client = await mongodb.MongoClient.connect(DBHOST)
  return client.db(DBNAME)
}

//
// Connect to the RabbitMQ server.
//
async function connectRabbit() {
  console.log(`Connecting to RabbitMQ server at ${RABBIT}.`)

  const messagingConnection = await amqp.connect(RABBIT)

  console.log("Connected to RabbitMQ.")
  return await messagingConnection.createChannel() // Create a RabbitMQ messaging channel.
    
}

//
// Setup event handlers.
//
function setupHandlers(app, db, messageChannel) {

    const historyCollection = db.collection("videos")

    // .:: Other Routes ::.
    function consumeViewedMessage(msg) { // Handler for coming messages.
      const parsedMsg = JSON.parse(msg.content.toString())
      console.log("Received a 'viewed' message:")
      console.log(JSON.stringify(parsedMsg, null, 4))
      console.log("Acknowledging message was handled.")

      messageChannel.ack(msg) // If there is no error, acknowledge the message.
    }

    return messageChannel.assertExchange("viewed", "fanout") // Assert that we have a "viewed" exchange.
        .then(() => {
            return messageChannel.assertQueue("", { exclusive: true }) // Create an anonyous queue.
        })
        .then(response => {
            const queueName = response.queue
            console.log(`Created queue ${queueName}, binding it to "viewed" exchange.`)
            return messageChannel.bindQueue(queueName, "viewed", "") // Bind the queue to the exchange.
                .then(() => {
                    return messageChannel.consume(queueName, consumeViewedMessage) // Start receiving messages from the anonymous queue.
                })
        })
}

//
// Start the HTTP server.
//
function startHttpServer(db, messageChannel) {
    return new Promise(resolve => {
        const app = express()
        app.use(bodyParser.json())
        setupHandlers(app, db, messageChannel)

        const port = process.env.PORT && parseInt(process.env.PORT) || 3000
        app.listen(port, () => {
            resolve()
        })
    })
}

//
// Application entry point.
//
async function main() {
  const db = await connectDb()
  const messageChannel = await connectRabbit()
  return await startHttpServer(db, messageChannel)
}

main()
  .then(() => console.log("Microservice online."))
  .catch(err => {
      console.error("Microservice failed to start.")
      console.error(err && err.stack || err)
  })