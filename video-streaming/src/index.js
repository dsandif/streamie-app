const express = require('express')
const http = require('http')

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
  VIDEO_STORAGE_HOST } = process.env
const VIDEO_STORAGE_PORT = parseInt(process.env.VIDEO_STORAGE_PORT)

app.get('/',(req, res)=>{
  res.send("Streamie app")
})

app.get('/video',(req, res)=>{
  console.log(`Forwarding video requests to ${VIDEO_STORAGE_HOST}:${VIDEO_STORAGE_PORT}.`)
  const videoReqData = {
    host: VIDEO_STORAGE_HOST,
    port: VIDEO_STORAGE_PORT,
    path: '/video?path=bmw_love.mp4', // Video path is hard-coded for the moment.
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

  // original file system location
  // //stats retrieves the video file size
  // fs.stat(path="filepath",(err, stats)=>{
  //   if(err){
  //     console.error("An error occurred fetching the video.")
  //     res.sendStatus(500)
  //     return
  //   }

  //   //send response header to the browser
  //   res.writeHead(200, {
  //     "Content-Length" : stats.size,
  //     "Content-Type": "video/mp4"
  //   })

  //   //stream video to the browser
  //   fs.createReadStream(path).pipe(res)
  // })
})

app.listen(port, () =>{
  console.log(`video streaming microservice up and listening on port ${port} 🚀🚀🚀`)
})