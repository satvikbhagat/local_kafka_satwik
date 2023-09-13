"use strict";

const express = require("express");
const bodyParser = require("body-parser");
var cors = require("cors");
const app = express();
require('dotenv').config();
const port = process.env.PORT || 5000;

// const { saamsDataConsumer } = require('./consumer')

module.exports = {
  app: app,
};

app.use(cors());

app.use(express.static("public"));
app.use(bodyParser.json({ limit: "2mb" }));
app.use(
  bodyParser.urlencoded({
    extended: true,
  })
);

app.get("/", (request, response) => {
  response.json({ info: "SAAMS CONTROLLER" });
});

const { sendAckTosaams } = require('./producer')

app.post("/saamsController/message", async (req, res) => {
  const reqBody = req.body;
  console.log("reqbody",reqBody)
   
  await sendAckTosaams(reqBody)
  return res.status(200).json('success');
})

//saamsDataConsumer();

// const { SaamsControllerDataConsumer } = require("./resourceConsumer")
// SaamsControllerDataConsumer();

app.listen(port, () => {
  console.log(
    `Server running on port ${port}. with db details ${process.env.SAAMS_CONTROLLER} and env is ${process.env.NODE_ENV}`
  );
});
