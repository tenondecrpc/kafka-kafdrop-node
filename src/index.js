import * as dotenv from 'dotenv';
import app from './app.js';
import kafkaEvents from './components/kafkaEvents.js';

dotenv.config();

const start = async () => {
	// Setup kafkaEvents module
	await kafkaEvents.setup({
		brokers: process.env.KAFKA_BROKERS.split(","),
		username: process.env.KAFKA_USERNAME,
		password: process.env.KAFKA_PASSWORD
  });
  	// Setup web server
	app.setup({
		kafkaEvents: kafkaEvents,
		corsAllowlist: process.env.CORS_ALLOWLIST.split(",")
	});
};

const shutdown = async () => {
	await kafkaEvents.shutdown("consumer");
	await Promise.all([
		app.shutdown(),
	]);
	await Promise.all([
		kafkaEvents.shutdown("producer"),
		// models.disconnect(),
	]);
	["SIGTERM", "SIGINT", "SIGUSR2"].map(signal => {
		process.removeListener(signal, interruptionHandler);
	});
}

const interruptionHandler = async (signal) => {
  await shutdown().finally(() => process.kill(process.pid, signal));
};


start().catch(
  async (error) => {
    // logger.error(error);
    await shutdown();
    process.exitCode = 1;
  }
);
