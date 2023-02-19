const express = require('express')
const http = require('http')
const mongodb = require("mongodb");

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

const {
  PORT,
  VIDEO_STORAGE_HOST,
  DBHOST,
  DBNAME,
 } = process.env
const VIDEO_STORAGE_PORT = parseInt(process.env.VIDEO_STORAGE_PORT)

function main(){
  return mongodb.MongoClient.connect(DBHOST)
    .then(client=>{
      const db = client.db(DBNAME)
      const videosCollection = db.collection("videos")

      app.get('/video',(req, res)=>{
        console.log(`Forwarding video requests to ${VIDEO_STORAGE_HOST}:${VIDEO_STORAGE_PORT}.`)
        const videoId = new mongodb.ObjectId(req.query.id)
        
        videosCollection
          .findOne({_id: videoId})
            .then(videoRecord =>{
              if(!videoRecord){
                res.sendStatus(404)
                return
              }
            
            const videoReqData = {
              host: VIDEO_STORAGE_HOST,
              port: VIDEO_STORAGE_PORT,
              path: `/video?path=${videoRecord.videoPath}`, // Video path is hard-coded for the moment. Should show color-bars.
              method: 'GET',
              headers: req.headers
            }

            const forwardRequest = http.request( // Forward the request to the video storage microservice.
              videoReqData,
              forwardResponse => {
                  res.writeHeader(forwardResponse.statusCode, forwardResponse.headers)
                  forwardResponse.pipe(res)
              }
            )
          
            req.pipe(forwardRequest)
          })
          .catch(err=>{
            console.error("Database query failed")
            console.error(err && err.stack || err);
            res.sendStatus(500);
          })
      })

      app.get('/',(req, res)=>{
        res.send("Streamie app")
      })
      
      app.listen(PORT, () =>{
        console.log(`microservice online 🚀🚀🚀`)
      })
  })
}

main()
.then(()=> console.log(`video streaming microservice up and listening on port ${PORT} 🚀🚀🚀`))
.catch(err=>{
  console.error("Microservice failed to start.")
  console.error(err && err.stack || err)
})