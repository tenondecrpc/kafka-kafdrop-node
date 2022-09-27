import * as dotenv from 'dotenv';
import express from 'express';
import http from 'http';
import cors from 'cors';

dotenv.config();

let server;
let readyForRequests;

const setup = async (config) => {
  // Setup express
  const app = express();
  app.use(express.json());
  app.use(cors({
		origin: config.corsAllowlist
	}));


  // Setup server
  server = http.Server(app);

  // Setup routers
  app.get('/', (req, res) => {
    res.json({message: 'Hello Docker with Kafka'});
  });
  app.get("/healthz", (req, res) => {
		readyForRequests ? res.sendStatus(200) : res.sendStatus(503);
	});

	// Start listening for connections
	server.listen(process.env.SERVER_PORT, () => readyForRequests = true);
};

const shutdown = async () => {
	// Signal LB (via health check route) that we are shutting down
	readyForRequests = false;
	await new Promise(r => setTimeout(r, 20000));	// Wait time must be > pod readiness probe failure threshold

	// Stop accepting new connections and drain existing ones
	if(server){
		await new Promise(r => server.close(r));
	}
};


export default { setup, shutdown };