# FX-BEAM

FX-Beam is a data processing pipeline to process tick and OHLCV data.

Main features of FX-Beam:
* Take in tick data in CSV and convert it to OHLCV data
* Take in tick data in JSON and convert to OHLCV data
* Take in tick data from different instruments and group per instrument
* [FUTURE] Take in OHLCV in CSV and convert to OHLCV data with a different window
* [FUTURE] Take in OHLCV in JSON and convert to OHLCV data with a different window
* [FUTURE] Support streaming
* [FUTURE] Support API format to not save to file but be used in another pipeline

<hr>
<br>

#### Example to run without instrument column:
```
python run_fxbeam.py -i <input_file> -o <output_file>
```

#### Example to run with instrument column:
```
python run_fxbeam.py -i <input_file> -o <output_file> -ic <instrument_column>
```