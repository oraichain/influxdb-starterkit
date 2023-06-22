const { InfluxDB } = require('@influxdata/influxdb-client');
const express = require('express');
const { url, token, org } = require('./env');

const port = process.env.PORT || 3000;
const app = express();
const queryApi = new InfluxDB({ url, token }).getQueryApi(org);

const getOHLCV = async (exchange, symbol, tf, limit = 100) => {
  const fluxQuery = `
  extract = (field, fn) => from(bucket:"${exchange}")
    |> range(start:0)              
    |> filter(fn: (r) => r._field == field and r.symbol == "${symbol}")                  
    |> aggregateWindow(every: ${tf}, createEmpty: false, fn: fn)
    |> limit(n: ${limit})                              

  union(tables: [
    extract(field: "open", fn: first), 
    extract(field: "close", fn: last), 
    extract(field: "low", fn: min), 
    extract(field: "high", fn: max), 
    extract(field: "volume", fn: sum)
  ])
    |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")    
`;
  const data = await queryApi.collectRows(fluxQuery);
  return data.map(({ _time, open, close, low, high, volume }) => ({ time: new Date(_time).getTime() / 1000, open, close, low, high, volume }));
};

app.use(express.static('public'));

app.get('/api/:tf', async (req, res) => {
  const { tf = '4h' } = req.params;
  const data = await getOHLCV('kucoin', 'ORAI/USDT', tf);
  res.json(data);
});

app.listen(port, () => {
  console.log(`Example app listening on port ${port}`);
});
