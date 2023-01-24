const express = require('express')
const fs = require("fs");

const app = express()
const port = 3000

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