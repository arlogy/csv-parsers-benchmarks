# csv-parsers-benchmarks
Benchmarks of popular CSV parsers:
* Simple String.split (only non-quoted data)
* [PapaParse](https://www.papaparse.com/)
* [csv-parser](https://www.npmjs.com/package/csv-parser)
* [csv-parse](https://csv.js.org/parse/)
* [dekkai](https://www.npmjs.com/package/dekkai)
* [fast-csv](https://www.npmjs.com/package/fast-csv)

Additional CSV parsers tested:
* [jsu](https://github.com/arlogy/jsu/blob/main/doc) &rarr; CSV parser
* [udsv](https://github.com/leeoniya/uDSV)

The tests run on generated data files with 10/100 columns and 10k/100k rows, both quoted and unquoted. 
The first column is numeric and used for the ``sum`` validation. The rest are just strings and not used in the tests directly.

## Run
* Install Node.js
* `git clone <project_git_uri>`
* `cd <project_dir>`
* `npm install` (install the dependencies of the forked repository)
* `npm install jsupack` (install jsu separately so we don't have to hardcode the version or modify `package*.json` and `yarn.lock`)
* `npm install udsv` (install udsv separately for the same reasons)
* `node src/index.js`

## Results
_Note: this section does not apply to the jsu CSV parser, whose implementation and benchmarks are detailed [here](https://github.com/arlogy/algos/blob/main/csv_parser/design_and_implementation_of_a_streaming_csv_parser.md). It also does not apply to udsv, whose benchmarks are available at the same link._

Benchmarked on i5-8350U/16Gb RAM/SSD running Ubuntu 20.04

### Non-Quoted CSV files
![Non-Quoted CSV Parser Benchmarks](results/non_quoted.png)

PapaParse was running in fast mode. Dekkai was crashing the whole process on 100k rows tests, so there are no results. We've also added a simple ``String.split`` approach here for comparison with full-featured compliant parsers. It should be treated only as a baseline.

### Quoted CSV Files
![Quoted CSV Parser Benchmarks](results/quoted.png)

All parsers performed a little worse on quoted data. Dekkai was crashing again on 100k rows tests and was disabled. PapaParse was running with fast mode disabled. ``String.split`` won't work for the quoted data in general and implementing proper parsing is not that trivial. 

Read more in our [CSV Parsers Comparison](https://leanylabs.com/blog/js-csv-parsers-benchmarks/) (this excludes jsu and udsv).
