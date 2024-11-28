# POOPy

**POOPy** = **P**ollution discharge monitoring with **O**bject **O**riented **Py**thon

- [Description](#description)
- [Installation](#installation)
  - [Dependencies](#dependencies)
  - [API Keys](#api-keys)
  - [Testing](#testing)
- [Usage](#usage)
    - [Examples](#examples)
    - [Gallery](#gallery)
- [Credit](#credit)
- [Disclaimer](#disclaimer)

## Description

This is a Python package for interfacing with live data from Event Duration Monitoring (EDM) devices maintained by English and Welsh Water Companies. This package was ostensibly developed to provide the back-end for [SewageMap.co.uk](https://github.com/AlexLipp/thames-sewage) but may be generically useful for those **exploring the impact of sewage discharges on rivers**. Currently, `POOPy` supports data from all of the major water and sewerage companies: 

| Water Company                        | `WaterCompany` Object Name  |
|------------------------------------|-----------------------|
| Thames Water  | `ThamesWater`        |
| Welsh Water/Dŵr Cymru   | `WelshWater`    |
| Southern Water | `SouthernWater` | 
| Anglian Water | `AnglianWater` |
| United Utilities | `UnitedUtilities` |
| Severn Trent | `SevernTrent` |
| Yorkshire Water | `YorkshireWater` |
| Northumbrian Water | `NorthumbrianWater` |
| South West Water | `SouthWestWater` |
| Wessex Water | `WessexWater` |


Different water companies share their [live EDM data](https://www.streamwaterdata.co.uk/pages/storm-overflows-data) via APIs with different formats. This is obviously confusing and means that **it is hard to access national data simultaneously** and ultimately understand their potential impact on the environment. `POOPy` solves this problem by **encapsulating** relevant information about EDM monitors maintained by different water companies into a **standardised interface**. This interface (represented by the `WaterCompany` and `Monitor` classes) makes it very easy to, for instance, quickly identify monitors that are discharging, have discharged in the last 48 hours or are offline. `POOPy` combines this information with **key meta-data about the monitor** such as location and the watercourse it discharges into. Additionally, `POOPy` provides a basic approache to explore the 'impact' of discharges on the environment, using a simple hydrological model to identify **river sections downstream of sewage discharges** in real-time. `POOPy` could easily be extended to consider more complicated ways of exploring the 'impact' of sewage spills (e.g., [dynamic river flow](https://github.com/AlexLipp/thames-sewage/issues/31)).

Where historical information on CSO discharges are available (currently only provided as an API by Thames Water), `POOPy` processes this information making it very **easy to query the spill history of a particular monitor**. For instance, to calculate the total hours of sewage discharge from a given monitor over a given timeframe. Experimentally, `POOPy` also has **capabilities to 'build' histories of sewage spills** from repeated queries to the current status of a monitor, _even if (in the case of most water companies) this information is not made readily accessible_.   

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
pytest --disable-warnings
```
This will run the tests and provide a summary of the results. If all tests pass, the package has been installed correctly and behaving as expected. Note that the `--disable-warnings` flag is used to suppress the many warnings that `POOPy` generates, these are mostly informative rather than disastrous (e.g., indicating when an input data-stream is ambiguous), but can be overwhelming _en masse_. 


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

Examples of how to use the package (using the `ThamesWater` class as an example) are given in the `examples` folder in the form of interactive python Jupyter noteboooks. Note that whilst `ThamesWater` is used as an example, the same operations apply to **all** of the water companies supported by `POOPy` (with the exception of the historical data operations which are currently only supported by Thames Water):
- [Investigating the *current* status of sewer overflow spilling](https://github.com/AlexLipp/POOPy/blob/main/examples/current_status.ipynb)
- [Investigating the *historical* status of sewer overflow spilling](https://github.com/AlexLipp/POOPy/blob/main/examples/historical_status.ipynb)

### Gallery

`POOPy` can be used, for example to make figures like...

... this one showing the stretches of the Thames downstream of active sewage discharges at the shown time...

![map](https://github.com/AlexLipp/POOPy/assets/10188895/395732dc-54c1-403e-b681-be3bece7f7e7)

...or this one which shows the discharge history of a specific monitor...

![bourton_on_the_water](https://github.com/AlexLipp/POOPy/assets/10188895/feeb6035-78f0-4c48-b3f2-bd1d18f2ce96)

...or this one which shows the number of live monitors deployed by Thames Water through time and whether they were discharging...

![image](https://github.com/AlexLipp/POOPy/assets/10188895/8c631231-bf9c-406e-a393-4d1a72d355b3)


## Credit

If you use these scripts, or the data, please reference its source. For instance: 

> "Data generated using the POOPy software (`github.com/AlexLipp/POOPy`)"

## Disclaimer
Whilst every effort has been made to ensure accuracy, this is experimental software and errors may occur and I assume no responsibility or liability for any such errors. If you see any issues please contact me, or raise an `Issue' above. This code is [licensed under the GNU General Public License 3.0](https://github.com/AlexLipp/poopy?tab=GPL-3.0-1-ov-file#readme)
