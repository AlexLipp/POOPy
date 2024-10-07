# POOPy

**POOPy** = **P**ollution discharge monitoring with **O**bject **O**riented **Py**thon

- [Description](#description)
- [Installation](#installation)
  - [Dependencies](#dependencies)
  - [API Keys](#api-keys)
  - [Testing](#testing)
- [Usage](#usage)
    - [Examples](#examples)


## Description

This is a Python package for interfacing with Event Duration Monitoring (EDM) devices maintained by English (and Welsh!) Water Companies. This package was ostensibly developed to provide the back-end for [SewageMap.co.uk](https://github.com/AlexLipp/thames-sewage) but may be generically useful for those exploring the impact of sewage discharges on rivers. Currently, `POOPy` supports data from the following water companies: 
- [Thames Water](https://github.com/AlexLipp/POOPy/blob/main/poopy/companies.py#L13)
- [Welsh Water/Dŵr Cymru](https://github.com/AlexLipp/POOPy/blob/main/poopy/companies.py#L511)

It can be used, for example to make figures like...

... this one showing the stretches of the Thames downstream of active sewage discharges at the shown time...

![map](https://github.com/AlexLipp/POOPy/assets/10188895/395732dc-54c1-403e-b681-be3bece7f7e7)

...or this one which shows the discharge history of a specific monitor...

![bourton_on_the_water](https://github.com/AlexLipp/POOPy/assets/10188895/feeb6035-78f0-4c48-b3f2-bd1d18f2ce96)

...or this one which shows the number of live monitors deployed by Thames Water through time and whether they were discharging...

![image](https://github.com/AlexLipp/POOPy/assets/10188895/8c631231-bf9c-406e-a393-4d1a72d355b3)

## Installation

Install this package by running the following command (replacing `[LOCAL DIRECTORY]` with the directory you wish to install the package into).


```bash
git clone https://github.com/AlexLipp/POOPy.git [LOCAL DIRECTORY]
pip install .
```

### Dependencies

The package requires standard scientific Python packages (e.g. `numpy`, `pandas`, `matplotlib`) as well as the following packages:

- [GDAL](https://gdal.org/download.html) - Required to manipulate geospatial datasets.
- [pytest](https://docs.pytest.org/en/stable/) - For running the test suite [_optional_, see [Testing](#testing)].
### API Keys

To access the data for the following water companies, you will need to obtain API keys from the relevant water company by registering with their developer portal: 

- [Thames Water](https://data.thameswater.co.uk/s/)

From these portals you will obtain `client_id` and `client_secret` keys which are required to access the datasets. `POOPy` will look for these keys in the _environment variables_ of your system. Specifically, it will look for the following variables which must be set in your system environment: 

| Key                        | Environment Variable  |
|------------------------------------|-----------------------|
| Thames Water client ID  | `TW_CLIENT_ID`        |
| Thames Water 'secret' ID  | `TW_CLIENT_SECRET`    |

How to set these environment variables will depend on your operating system. For example, on a Unix-based system, you could add the following lines to your `.bashrc` or `.bash_profile` file: 

```bash
export TW_CLIENT_ID="your_client_id"
export TW_CLIENT_SECRET="your_client_secret"
```

### Testing 

A test script is provided in the `tests` folder. To run the tests, you will need to install the [`pytest` package](https://docs.pytest.org/en/stable/). If installed, the tests can be run from the command line by navigating to the folder in which the package is installed and simply running the command: 

```bash
pytest
```
This will run the tests and provide a summary of the results. If all tests pass, the package has been installed correctly and behaving as expected. 


## Usage

Once installed, the package can be imported into Python scripts using standard import commands, such as:
```python
import poopy
```
or 
```python
from poopy.companies import ThamesWater
```

### Examples

Examples of how to use the package (using the `ThamesWater` class as an example) are given in the `examples` folder in the form of interactive python Jupyter noteboooks: 
- [Investigating the *current* status of sewer overflow spilling](https://github.com/AlexLipp/POOPy/blob/main/examples/current_status.ipynb)
- [Investigating the *historical* status of sewer overflow spilling](https://github.com/AlexLipp/POOPy/blob/main/examples/historical_status.ipynb)
