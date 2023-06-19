const { InfluxDB } = require('@influxdata/influxdb-client');
const express = require('express');
const { url, token, org } = require('./env');

const port = process.env.PORT || 3000;
const app = express();
const queryApi = new InfluxDB({ url, token }).getQueryApi(org);

const getOHLCV = (exchange, symbol, tf) =>
  new Promise((resolve, reject) => {
    const data = [];
    const fluxQuery = `            
      from(bucket:"${exchange}")
        |> range(start:0)                
        |> filter(fn: (r) => r._measurement == "ohlcv" and r._field == "close" and r.symbol == "${symbol}")                        
        |> aggregateWindow(every: ${tf}, fn: mean, createEmpty: false)     
        |> map(fn: (r) => ({time: uint(v: r._time), value: r._value}))           
        |> limit(n: 100)        
        `;
    queryApi.queryRows(fluxQuery, {
      next(row, tableMeta) {
        const { time, value } = tableMeta.toObject(row);
        data.push({ time, value });
      },
      error(err) {
        reject(err);
      },
      complete() {
        resolve(data);
      }
    });
  });

app.use(express.static('public'));

app.get('/api/:tf', async (req, res) => {
  const { tf = '4h' } = req.params;
  const data = await getOHLCV('kucoin', 'ORAI/USDT', tf);
  res.json(data);
});

app.listen(port, () => {
  console.log(`Example app listening on port ${port}`);
});
