import express from 'express';

const app = express();

app.get('/', (req, res) => {
  res.json({message: 'Hello Docker with Kafka'});
});

const port = process.env.PORT || 3010;
app.listen(port, () => console.info(`App listening on http://localhost:${port}`));