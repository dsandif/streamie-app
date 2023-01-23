const express = require('express')

const app = express()
const port = 3000

app.get('/',(req, res)=>{
  res.send("Streamie app")
})

app.listen(port, () =>{
  console.log(`Streamie listening on port ${port} 🚀🚀🚀`)
})