# Project Title

Data collector

## Description

Data collector is a module that develope for collect data from Dukascopy. It will download by date range. It is part of the AlgoTrading project.
Source support:
- Dukascopy
Output format:
- parquet
- csv (in development)

## Getting Started

### Dependencies

* Require Python >= 3.6, pip3. It doesn't test with other versions.

### Installing

* Run the following command to install all nescessary packages:
```
pip install -r requirements.txt
```

### Executing program

* How to run the program
* Step-by-step bullets
```
python main.py -c 'EURUSD' -s '2020-01-01' -e '2020-12-31' -d 'tickdata' -n 4
```

The command above will download the data from 2020-01-01 to 2020-12-31 and save in the tickdata folder.

Folder tickdata structure in the format: tickdata/{currency}/{YYYY}/{MM}/{YYYY}-{MM}-{DD}/{HH}.parquet

## Authors

Contributors names and contact info

- [@Quan Pham](https://twitter.com/QuanLab) Owner and contributer

## Version History

* 0.1
    * Initial Release

## License

This project is licensed under the MIT License - see the LICENSE.md file for details

## Acknowledgments

Updating...