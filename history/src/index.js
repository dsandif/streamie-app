const express = require("express")
const mongodb = require("mongodb")
const bodyParser = require('body-parser')
const amqp = require("amqplib")

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


function connectDb() {
  return mongodb.MongoClient.connect(DBHOST) 
      .then(client => {
          return client.db(DBNAME)
      })
}

//
// Connect to the RabbitMQ server.
//
async function connectRabbit() {
  console.log(`Connecting to RabbitMQ server at ${RABBIT}.`)

  const messagingConnection = await amqp.connect(RABBIT) // Connect to the RabbitMQ server.
    
  console.log("Connected to RabbitMQ.")
  return await messagingConnection.createChannel() // Create a RabbitMQ messaging channel.
    
}

function setupHandlers(app, db, messageChannel){
  const videosCollection = db.collection("videos")

  //.::Other routes::.

  function consumeViewedMessage(msg) { // Handler for coming messages.
    console.log("Received a 'viewed' message")

    const parsedMsg = JSON.parse(msg.content.toString()) // Parse the JSON message.
    
    return videosCollection.insertOne({ videoPath: parsedMsg.videoPath }) // Record the "view" in the database.
        .then(() => {
            console.log("message was handled.")
            
            messageChannel.ack(msg) // If there is no error, acknowledge the message.
        })
  }

  return messageChannel.assertExchange("viewed", "fanout") // assert that we have a viewed exchange
    .then(()=>{
      return messageChannel.assertQueue("", {exclusive : true}) // creates an anonymous queue
    })
    .then((response)=>{
      const {queue: queueName} = response
      console.log(`created ${queueName}, binding it to viewed exchange`)
      return messageChannel.bindQueue(queueName, "viewed", "") //Binds the queue to the exchange
        .then(()=>{
          return messageChannel.consume(queueName, consumeViewedMessage)
        })

    })
  // return messageChannel.assertQueue("viewed", {}) // Assert that we have a "viewed" queue.
  //     .then(() => {
  //         console.log("Asserted that the 'viewed' queue exists.")
  //         return messageChannel.consume("viewed", consumeViewedMessage) // Start receiving messages from the "viewed" queue.
  //     })
}

function startHttpServer(db, messageChannel){
  return new Promise(resolve=>{
    const app = express()
    app.use(bodyParser.json()) // middleware for JSON body for HTTP requests.
    setupHandlers(app, db, messageChannel)

    const port = process.env.PORT && parseInt(process.env.PORT) || 3000
    app.listen(port,()=>{
      resolve() // resolve the promise
    })
  })
}

function main(){
  console.log("History service")
  return connectDb(DBHOST)
  .then(db=>{
    return connectRabbit().then((messageChannel) =>{
      return startHttpServer(db, messageChannel)
    })
  })
}

main()
  .then(()=> console.log("service online ????????????"))
  .catch(err=>{
    console.error("Microservice failed to start")
    console.error(err && err.stack || err)
  })