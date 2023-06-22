const { InfluxDB } = require('@influxdata/influxdb-client');
const express = require('express');
const { url, token, org } = require('./env');

const port = process.env.PORT || 3000;
const app = express();
const queryApi = new InfluxDB({ url, token }).getQueryApi(org);

const getOHLCV = (exchange, symbol, tf, limit = 100) =>
  new Promise((resolve, reject) => {
    const data = [];
    const fluxQuery = `filterTable = (field, fn) => from(bucket:"${exchange}")
    |> range(start:0)              
    |> filter(fn: (r) => r._measurement == "ohlcv" and r._field == field and r.symbol == "${symbol}")                  
    |> aggregateWindow(every: ${tf}, createEmpty: false, fn: fn)
    |> limit(n: ${limit})                              

open = filterTable(field: "open", fn: first)             
close = filterTable(field: "close", fn: last)                   
low = filterTable(field: "low", fn: min)    
high = filterTable(field: "high", fn: max)   
volume = filterTable(field: "volume", fn: sum)

union(tables: [open, close, low, high, volume])
    |> map(fn: (r) => ({r with time: uint(v: r._time) / uint(v: 1000000000)})) 
    |> drop(columns: ["_measurement", "symbol", "tf", "_time", "_start", "_stop"])
    |> pivot(rowKey: ["time"], columnKey: ["_field"], valueColumn: "_value")                           
        `;
    queryApi.queryRows(fluxQuery, {
      next(row, tableMeta) {
        const { result, table, ...item } = tableMeta.toObject(row);
        data.push(item);
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
