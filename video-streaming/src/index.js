const express = require('express')
const http = require('http')
const mongodb = require("mongodb")
const amqp = require('amqplib')

if (!process.env.PORT) {
  throw new Error("Please specify the port number for the HTTP server with the environment variable PORT.")
}

if (!process.env.VIDEO_STORAGE_HOST) {
    throw new Error("Please specify the host name for the video storage microservice in variable VIDEO_STORAGE_HOST.")
}

if (!process.env.VIDEO_STORAGE_PORT) {
    throw new Error("Please specify the port number for the video storage microservice in variable VIDEO_STORAGE_PORT.")
}

if (!process.env.RABBIT) {
  throw new Error("Please specify the name of the RabbitMQ host using environment variable RABBIT")
}

const {
  PORT,
  VIDEO_STORAGE_HOST,
  DBHOST,
  DBNAME,
  RABBIT
 } = process.env
const VIDEO_STORAGE_PORT = parseInt(process.env.VIDEO_STORAGE_PORT)


//
// Connect to the RabbitMQ server.
//
async function connectRabbit() {
  console.log(`Connecting to RabbitMQ server at ${RABBIT}.`)

  const connection = await amqp.connect(RABBIT) // Connect to the RabbitMQ server.
    
  console.log("Connected to RabbitMQ.")
  return await connection.createChannel()
}

//
// Send the viewed msg to the history microservice.
//
function sendViewedMessage(messageChannel, videoPath) {
  console.log(`Publishing message on "viewed" queue.`)

  const msg = { videoPath: videoPath }
  const jsonMsg = JSON.stringify(msg)
  messageChannel.publish("", "viewed", Buffer.from(jsonMsg)) // Publish message to the viewed queue.
}

//
// Setup event handlers.
//
function setupHandlers(app, messageChannel, db) {
  app.get("/video", (req, res) => { // Route for streaming video.
    const videosCollection = db.collection("videos")
    const videoId = new mongodb.ObjectId(req.query.id)
    
    console.log('fetching video')
    console.log(`Forwarding video requests to ${VIDEO_STORAGE_HOST}:${VIDEO_STORAGE_PORT}.`)

    videosCollection.findOne({ _id: videoId })
      .then(videoRecord => {
        if (!videoRecord) {
          res.sendStatus(404)
          return
        }

        console.log(`translated id to path ${videoRecord.videoPath}.`)

        const videoReqData = {
          host: VIDEO_STORAGE_HOST,
          port: VIDEO_STORAGE_PORT,
          path: `/video?path=${videoRecord.videoPath}`,
          method: 'GET',
          headers: req.headers
        }

        const forwardRequest = http.request(
          videoReqData,
          forwardResponse => {
            res.writeHeader(forwardResponse.statusCode, forwardResponse.headers)
            forwardResponse.pipe(res)
          }
        )

        req.pipe(forwardRequest)
        sendViewedMessage(messageChannel, videoRecord.videoPath) // Send viewed message to history microservice
      })
      .catch(err => {
        console.error("Database query failed")
        console.error(err && err.stack || err)
        res.sendStatus(500)
      })
  })
}

//
// Start the HTTP server.
//
function startHttpServer(messageChannel, db) {
  return new Promise(resolve => { // Wrap in a promise so we can be notified when the server has started.
      const app = express()
      setupHandlers(app, messageChannel, db)

      const port = process.env.PORT && parseInt(process.env.PORT) || 3000
      app.listen(port, () => {
          resolve()
      })
  })
}

// //
// // Send direct message to the history microservice.
// //
// function sendViewedMessage(videoPath) {
//   const postOptions = {
//       method: "POST",
//       headers: {
//           "Content-Type": "application/json",
//       },
//   }
//   const requestBody = {
//       videoPath: videoPath 
//   }

//   const req = http.request( // Send the message
//       "http://history/viewed",
//       postOptions
//   )

//   req.on("close", () => {
//       console.log("Sent 'viewed' message to history microservice.")
//   })

//   req.on("error", (err) => {
//       console.error("Failed to send 'viewed' message!")
//       console.error(err && err.stack || err)
//   })

//   req.write(JSON.stringify(requestBody)) // Write the body to the request.
//   req.end() // End the request.
// }

//
// Application entry point.
//
async function main() {
  const client = await mongodb.MongoClient
    .connect(DBHOST)
  const db = client.db(DBNAME)
  const messageChannel = await connectRabbit()
  return await startHttpServer(messageChannel, db)
}

main()
.then(()=> console.log(`video streaming microservice up and listening on port ${PORT} 🚀🚀🚀`))
.catch(err=>{
  console.error("Microservice failed to start.")
  console.error(err && err.stack || err)
})