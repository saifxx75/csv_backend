require('dotenv').config();
const express = require('express');
const mongoose = require('mongoose');
const WebSocket = require('ws');
const multer = require('multer');
const path = require('path');
const CsvModel = require('./modal/csvModal');
const { Readable } = require('stream');
const csvParser = require('csv-parser');


const app = express();
const port = process.env.PORT || 3000;

// Connect to MongoDB
mongoose.connect(process.env.MONGO_URL, {
  useNewUrlParser: true,
  useUnifiedTopology: true,
}).then(() => {
  console.log('Connected to MongoDB');
}).catch(err => {
  console.error('Error connecting to MongoDB:', err);
});

// Set up Multer for file uploads
const storage = multer.memoryStorage();
const upload = multer({
  storage: storage,
  fileFilter: (req, file, cb) => {
    if (path.extname(file.originalname) !== '.csv') {
      return cb(new Error('Only CSV files are allowed'), false);
    }
    cb(null, true);
  }
});

// WebSocket server setup
const wss = new WebSocket.Server({ port: 5000 });
global.wss = wss;

wss.on('connection', function connection(ws) {
  console.log("WS connection arrived");
  ws.on('message', function incoming(message) {
    console.log('received: %s', message);
  });

  ws.send('this is a message');
});


const server = app.listen(port, () => {
  console.log(`Server running at http://localhost:${port}/`);
});

server.on('upgrade', (request, socket, head) => {
  wss.handleUpgrade(request, socket, head, (ws) => {
    wss.emit('connection', ws, request);
  });
});

const socket = new WebSocket('ws://localhost:3000');
socket.on('open', function open() {
  console.log('Connected to WebSocket server');
});

socket.on('message', function incoming(data) {
  console.log('Received message from server:', data);
});

socket.on('error', function error(err) {
  console.error('WebSocket error:', err);
});

socket.on('close', function close() {
  console.log('Disconnected from WebSocket server');
});

app.use(express.json());
// API to handle CSV file upload
app.post('/upload', upload.single('file'), async (req, res) => {
  try {
    const { requestId } = req.body;

    if (!requestId) {
      return res.status(400).json({ status: 400, message: 'requestId is required' });
    }

    if (!req.file) {
      return res.status(400).json({ status: 400, message: 'CSV file is required' });
    }

    const wsClient = Array.from(wss.clients).find(client => client.readyState === WebSocket.OPEN);
    if (!wsClient) {
      return res.status(500).json({ status: 500, message: 'No WebSocket client connected' });
    }

    const results = [];
    const readable = new Readable();
    readable._read = () => { };
    readable.push(req.file.buffer);
    readable.push(null);

    readable.pipe(csvParser())
      .on('data', (data) => {
        results.push(data);
        if (results.length % 1000 === 0) {
          wsClient.send(JSON.stringify({ requestId, data: results.slice(-1000) }));
        }
      })
      .on('end', async () => {
        if (results.length > 0) {
          wsClient.send(JSON.stringify({ requestId, data: results.slice(-results.length % 1000) }));
        }
        const jsonData = JSON.stringify(req.file) 
        const csvData = new CsvModel({ requestId, file: jsonData });
        await csvData.save();
        res.status(200).json({ status: 200, message: 'File processed and saved successfully' });
      })
      .on('error', (error) => {
        console.error('Error parsing CSV:', error);
        res.status(500).json({ status: 500, message: 'Error processing file', error: error.message });
      });
  } catch (err) {
    console.error('Error handling request:', err);
    res.status(500).json({ status: 500, message: 'Internal Server Error', error: err.message });
  }
});


app.get('/csvData/:requestId', async (req, res) => {
  const { requestId } = req.params
  try {
      const csv = await CsvModel.findOne({ "requestId": requestId })
      if (csv) {
          return res.json({ status: 200, "message": "Csv file ", "data": csv })
      } else {
          return res.json({ status: 404, "message": "Data not found" })
      }
  } catch (err) {
      console.error('Error handling request:', err);
      res.status(500).json({ status: 500, message: 'Internal Server Error', error: err.message });
  }
})