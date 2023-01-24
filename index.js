const express = require('express')
const fs = require("fs");

const app = express()

if (!process.env.PORT) {
  throw new Error("Please specify the port number for the HTTP server with the environment variable PORT.");
}
const port = process.env.PORT

app.get('/',(req, res)=>{
  res.send("Streamie app")
})

app.get('/video',(req, res)=>{
  //stats retrieves the video file size
  fs.stat(path="filepath",(err, stats)=>{
    if(err){
      console.error("An error occurred fetching the video.")
      res.sendStatus(500)
      return
    }

    //send response header to the browser
    res.writeHead(200, {
      "Content-Length" : stats.size,
      "Content-Type": "video/mp4"
    })

    //stream video to the browser
    fs.createReadStream(path).pipe(res)
  })
})

app.listen(port, () =>{
  console.log(`Streamie listening on port ${port} 🚀🚀🚀`)
})