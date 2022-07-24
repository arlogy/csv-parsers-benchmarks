const fs = require("fs");
const csvParse = require("csv-parse");
const csvParser = require("csv-parser");
const dekkai = require("dekkai/dist/umd/dekkai");
const fastCsv = require("fast-csv");
const Papa = require("papaparse");

const Jsu = require("jsupack");
const JsuCsvPsr = Jsu.CsvParser;

async function benchmark(name, func, { cycles = 10 }) {
  let elapsed = Infinity;
  try {
    await func(); // warm up

    const start = Date.now();
    for (let i = 0; i < cycles; ++i) {
      await func();
    }
    elapsed = (Date.now() - start) / cycles;
    console.log(`${name}: ${elapsed.toFixed(2)} ms`);
  } catch (e) {
    console.error(`${name}: crashed`, e);
  }

  return { name, elapsed };
}

async function parseManual(fileName) {
  const lines = (
    await fs.promises.readFile(fileName, { encoding: "utf8" })
  ).split("\n");
  lines.splice(0, 1);
  return lines.map((line) => line.split(","));
}

function parseCsvStream(fileName, csv) {
  return new Promise((resolve) => {
    let header = true;
    const data = [];
    fs.createReadStream(fileName)
      .pipe(csv())
      .on("data", (line) => {
        if (header) {
          header = false;
        } else {
          data.push(line);
        }
      })
      .on("end", () => {
        resolve(data);
      })
      .on("finish", () => {
        resolve(data);
      });
  });
}

async function parseDekkai(fileName) {
  const fd = await new Promise((resolve, reject) => {
    fs.open(fileName, (err, fd) => {
      if (err) {
        reject(err);
      } else {
        resolve(fd);
      }
    });
  });

  const data = [];
  const table = await dekkai.tableFromLocalFile(fd);

  await table.forEach((row) => {
    const arr = [];
    row.forEach((v) => arr.push(v));
    data.push(arr);
  });

  return data;
}

async function parseJsu(fileName, smartRegex) {
  const fileContent = (
    await fs.promises.readFile(fileName, { encoding: "utf8" })
  );

  const parser = new JsuCsvPsr({ smartRegex });
  await parser.readChunk(fileContent);
  parser.flush();

  let records = parser.getRecordsRef();
  records = records.slice(1); // ignore header line as in parseCsvStream()
  return records;
}

async function benchmarkParsers({ name, fileName, rows, quotes, cycles }) {
  const expectedSum = (rows * (rows - 1)) / 2;

  const checkLines = (lines) => {
    const sum = lines.reduce((p, x) => p + +x[0], 0);
    if (sum !== expectedSum) throw new Error("Test Failed. Sum: " + sum);
  };

  const benData = []; // benchmark data

  console.log(`Running ${name}`);

  if (!quotes) {
    benData.push(await benchmark(
      "String.split",
      async () => {
        const lines = await parseManual(fileName);
        checkLines(lines);
      },
      { cycles }
    ));
  }

  if (rows <= 10000) {
    // it crashes the whole process on 100k
    benData.push(await benchmark(
      "dekkai",
      async () => {
        const lines = await parseDekkai(fileName);
        checkLines(lines);
      },
      { cycles }
    ));
  }

  benData.push(await benchmark(
    "papaparse",
    async () => {
      const lines = await parseCsvStream(fileName, () =>
        Papa.parse(Papa.NODE_STREAM_INPUT, {
          fastMode: !quotes,
        })
      );
      checkLines(lines);
    },
    { cycles }
  ));

  benData.push(await benchmark(
    "csv-parser",
    async () => {
      const lines = await parseCsvStream(fileName, () =>
        csvParser({ headers: false })
      );
      checkLines(lines);
    },
    { cycles }
  ));

  benData.push(await benchmark(
    "csv-parse",
    async () => {
      const lines = await parseCsvStream(fileName, csvParse);
      checkLines(lines);
    },
    { cycles }
  ));

  benData.push(await benchmark(
    "fast-csv",
    async () => {
      const lines = await parseCsvStream(fileName, () =>
        fastCsv.parse({ headers: false })
      );
      checkLines(lines);
    },
    { cycles }
  ));

  benData.push(await benchmark(
    "jsu-smart-on",
    async () => {
      const lines = await parseJsu(fileName, true);
      checkLines(lines);
    },
    { cycles }
  ));

  benData.push(await benchmark(
    "jsu-smart-off",
    async () => {
      const lines = await parseJsu(fileName, false);
      checkLines(lines);
    },
    { cycles }
  ));

  let rankData = benData.map(d => Object.assign({}, d)); // copy data
  rankData.sort((a, b) => a.elapsed - b.elapsed);

  // set custom ranking positions knowing that rankData is sorted in ascending
  // order; custom ranking positions are introduced because speed variations of
  // a few milliseconds tend to change between runs, and a speed gain of 250 ms
  // is hardly noticeable
  const samePosExpected = (a, b) => a.elapsed - b.elapsed <= 250;
  const samePosForAll = (data, limit, p, d) => {
    for(let j = 0; j <= limit; ++j) {
      if(data[j].pos === p && !samePosExpected(d, data[j])) return false;
    }
    return true;
  };
  if(rankData.length !== 0) rankData[0].pos = 1;
  for(let i = 1; i < rankData.length; ++i) {
    const currData = rankData[i], prevData = rankData[i-1];
    if(samePosExpected(currData, prevData) && samePosForAll(rankData, i-1, prevData.pos, currData))
      currData.pos = prevData.pos;
    else currData.pos = prevData.pos + 1;
  }

  console.log(
    "Ranking parsers:",
    rankData.map(d => `${d.pos}. ${d.name} (${d.elapsed})`).join(" / ")
  );
}

module.exports = {
  benchmarkParsers,
};
