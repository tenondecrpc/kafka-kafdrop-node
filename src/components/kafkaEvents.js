import { Kafka, logLevel } from 'kafkajs';
import packageJson from '../../package.json' assert { type: 'json' };

let config;
let consumer;
let producer;
let emittingFromQueue;

const onTestTopic = () => {
	console.log('onTestTopic');
};


// Set handlers
// handlers.topic.action = handler
const handlers = {
  some: {
    "test-topic": onTestTopic,
  },
};

// _config: {
// 	brokers: [String],
//	username: String (optional),
//	password: String (optional)
// }
const setup = async (_config) => {
	// Make config available module-wide
	config = _config;

	// Setup kafka client
	let sasl;
	let ssl;
	if(config.username && config.password){
		sasl = {
			mechanism: "PLAIN",
			username: config.username,
			password: config.password
		};
		ssl =  true;
	}
	const kafka = new Kafka({
		clientId: "kafkajs",
		brokers: config.brokers,
		sasl: sasl,
		ssl: ssl,
		connectionTimeout: 3000,
		requestTimeout: 30000,
		retry: {
			maxRetryTime: 30000,
			initialRetryTime: 300,
			retries: 5
		},
		logLevel: logLevel.WARN,
		logCreator: () => ({namespace, level, label, log}) => {
			let logFunction;
			if(level == logLevel.ERROR){ logFunction = logger.error; }
			if(level == logLevel.WARN){ logFunction = logger.warn; }
			if(level == logLevel.INFO){ logFunction = logger.info; }
			if(level == logLevel.DEBUG){ logFunction = logger.info; }
			if(level == logLevel.NOTHING){ logFunction = logger.error; }
			if(!logFunction){ logFunction = logger.error; }
			logFunction(`KafkaJS ${label} [${namespace}] ${JSON.stringify(log)}`);
		}
	});

	// Setup producer
	producer = kafka.producer();
	await producer.connect();

	// Setup consumer
	consumer = kafka.consumer({
		groupId: packageJson.name,
		allowAutoTopicCreation: true // TODO: false by default
	});
	await consumer.connect();
	for(let topic of Object.keys(handlers)){
		await consumer.subscribe({topic: topic, fromBeginning: true});
	}
	await consumer.run({
		partitionsConsumedConcurrently: 1,
		eachBatchAutoResolve: false,
		autoCommitInterval: 5000,
		eachBatch: async ({batch, resolveOffset, commitOffsetsIfNecessary, heartbeat, isRunning, isStale}) => {
			for(let message of batch.messages){
				if (!isRunning() || isStale()){ break; }
				try {
					// Parse message
					const {action, payload} = JSON.parse(message.value);

					// Process event
					await processEvent({
						topic: batch.topic,
						partition: batch.partition,
						offset: message.offset,
						action: action,
						timestamp: parseInt(message.timestamp),
						payload: payload
					});

					// Mark offset as processed
					resolveOffset(message.offset);

					// Commit offsets if time is right
					await commitOffsetsIfNecessary();

					// Send heartbeat to kafka if time is right
					await heartbeat();
				} catch(error) {
					// Log processing error
					logger.error(error);

					// Pause consumption from this topic-partition
					consumer.pause([{
						topic: batch.topic,
						partitions: [batch.partition]
					}]);

					// Resume consumption from this topic-partition after a while
					setTimeout(() => {
						consumer.resume([{
							topic: batch.topic,
							partitions: [batch.partition]
						}]);
					}, 10000);

					// Throw for kafkajs to use exponential backoff on retries
					throw error;
				}
			}
		}
	});
}

// topic: String
// messages: [{
// 	id: String
// 	action: String
// 	key: String
// 	timestamp: Number
// 	payload: Object
// }]
const emit = async ({topic, messages}) => {
	await producer.send({
		topic: topic,
		messages: messages.map(({key, timestamp, id, action, payload}) => ({
			key: key,
			timestamp: timestamp,
			value: JSON.stringify({id, action, payload})
		}))
	});
}

const emitFromQueue = async (amount=50) => {
	// Do nothing if instance is already emitting
	if(emittingFromQueue){ return; }
	emittingFromQueue = true;

	// Catch and report all errors here so the caller isnt bothered
	// Event emitting is retriable via a scheduler route
	try {
		// Attempt to get a service wide emitting lock
		const producedEventLock = await models.ProducedEventLock.findOneAndUpdate({
			$or: [
				{emitting: false},
				{emitting: true, lastEmitAttempt: {$lt: moment().subtract(30, "seconds")}}
			]
		}, {
			lastEmitAttempt: Date.now(),
			emitting: true
		});

		// Stop here if unable to get lock
		if(!producedEventLock){
			emittingFromQueue = false;
			return;
		}

		// Get ProducedEvents
		const producedEvents = await models.ProducedEvent
			.find({})
			.sort({timestamp: 1})
			.limit(amount)
			.lean();

		// Stop here if no ProducedEvent is found
		if(producedEvents.length == 0){
			await models.ProducedEventLock.updateOne({_id: producedEventLock._id}, {$set: {emitting: false}});
			emittingFromQueue = false;
			return;
		}

		// Classify events by topic
		const producedEventsByTopic = {};
		for(let producedEvent of producedEvents){
			if(!Array.isArray(producedEventsByTopic[producedEvent.topic])){
				producedEventsByTopic[producedEvent.topic] = [];
			}
			producedEventsByTopic[producedEvent.topic].push(producedEvent);
		}

		// Make one emitting request per topic
		for(let topic of Object.keys(producedEventsByTopic)){
			await emit({
				topic: topic,
				messages: producedEventsByTopic[topic].map(producedEvent => ({
					id: producedEvent._id.toString(),
					action: producedEvent.action,
					key: producedEvent.key,
					timestamp: +producedEvent.timestamp,
					payload: producedEvent.payload
				}))
			});
		}

		// Create db transaction
		const session = await models.ProducedEvent.startSession();
		await session.withTransaction(async function(){

			// Release service wide lock
			await models.ProducedEventLock.updateOne({_id: producedEventLock._id}, {
				$set: {emitting: false}
			}).session(session);

			// Remove ProducedEvents from storage
			await models.ProducedEvent.deleteMany({_id: {$in: producedEvents.map(pe => pe._id)}}).session(session);
		});
	}catch(error){
		logger.error(error);
	}

	// Release instance lock
	emittingFromQueue = false;
}

const shutdown = async (component) => {
	if(component == "consumer"){
		await consumer.disconnect();
	}else if(component == "producer"){
		await producer.disconnect();
	}else{
		await consumer.disconnect();
		await producer.disconnect();
	}
}

const processEvent = async ({topic, partition, offset, action, timestamp, payload}) => {
	// Do nothing if no handler is present for this topic-action combination
	if(!handlers[topic][action]){ return; }

	// Do nothing if event was already processed
	const consumedEvent = await models.ConsumedEvent.findOne({
		topic: topic,
		partition: partition
	});
	if(consumedEvent && consumedEvent.offset >= offset){ return; }

	// Run handler
	await handlers[topic][action](topic, partition, offset, action, timestamp, payload);
}

export default { setup, shutdown };
