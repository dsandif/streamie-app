const express = require('express')
const http = require('http')
const mongodb = require("mongodb")
const amqp = require('amqplib')

const app = express()

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
function connectRabbit() {
  console.log(`Connecting to RabbitMQ server at ${RABBIT}.`)

  return amqp.connect(RABBIT) // Connect to the RabbitMQ server.
      .then(connection => {
          console.log("Connected to RabbitMQ.")
          return connection.createChannel()
      })
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
function setupHandlers(app, messageChannel) {
  app.get("/video", (req, res) => { // Route for streaming video.

      const videoPath = "./videos/SampleVideo_1280x720_1mb.mp4"
      fs.stat(videoPath, (err, stats) => {
          if (err) {
              console.error("An error occurred ")
              res.sendStatus(500)
              return
          }
  
          res.writeHead(200, {
              "Content-Length": stats.size,
              "Content-Type": "video/mp4",
          })
  
          fs.createReadStream(videoPath).pipe(res)
          sendViewedMessage(messageChannel, videoPath) // Send message to "history" microservice that this video has been "viewed".
      })
  })
}

// function main(){
//   return mongodb.MongoClient.connect(DBHOST)
//     .then(client=>{
//       const db = client.db(DBNAME)
//       const videosCollection = db.collection("videos")

//       app.get('/video',(req, res)=>{
//         const videoId = new mongodb.ObjectId(req.query.id)

//         console.log('fetching video')
//         console.log(`Forwarding video requests to ${VIDEO_STORAGE_HOST}:${VIDEO_STORAGE_PORT}.`)
        
//         videosCollection.findOne({_id: videoId})
//           .then(videoRecord =>{
//             if(!videoRecord){
//               res.sendStatus(404)
//               return
//             }
            
//             console.log(`translated id to path ${videoRecord.videoPath}.`)

//             const videoReqData = {
//               host: VIDEO_STORAGE_HOST,
//               port: VIDEO_STORAGE_PORT,
//               path: `/video?path=${videoRecord.videoPath}`, // Video path is hard-coded for the moment. Should show color-bars.
//               method: 'GET',
//               headers: req.headers
//             }

//             const forwardRequest = http.request( // Forward the request to the video storage microservice.
//               videoReqData,
//               forwardResponse => {
//                 res.writeHeader(forwardResponse.statusCode, forwardResponse.headers)
//                 forwardResponse.pipe(res)
//               }
//             )
            
//             req.pipe(forwardRequest)
//             sendViewedMessage(videoRecord.videoPath) // Send viewed message to history microservice
//           })
//           .catch(err=>{
//             console.error("Database query failed")
//             console.error(err && err.stack || err)
//             res.sendStatus(500)
//           })
//       })

//       app.get('/',(req, res)=>{
//         res.send("Streamie app")
//       })
      
//       app.listen(PORT, () =>{
//         console.log(`microservice online 🚀🚀🚀`)
//       })
//   })
// }

//
// Start the HTTP server.
//
function startHttpServer(messageChannel) {
  return new Promise(resolve => { // Wrap in a promise so we can be notified when the server has started.
      const app = express()
      setupHandlers(app, messageChannel)

      const port = process.env.PORT && parseInt(process.env.PORT) || 3000
      app.listen(port, () => {
          resolve()
      })
  })
}

// //
// // Send viewed message to the history microservice.
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
function main() {
  return connectRabbit()
      .then(messageChannel => {
          return startHttpServer(messageChannel)
      })
}

main()
.then(()=> console.log(`video streaming microservice up and listening on port ${PORT} 🚀🚀🚀`))
.catch(err=>{
  console.error("Microservice failed to start.")
  console.error(err && err.stack || err)
})