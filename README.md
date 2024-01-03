# POOPy

**P**oo discharge monitoring with **O**bject **O**riented **Py**thon

## Description

This is a Python package for interfacing with Event Duration Monitoring (EDM) devices maintained by English Water Companies. This package was ostensibly developed to provide the back-end for [SewageMap.co.uk](https://github.com/AlexLipp/thames-sewage) but may be generically useful for those exploring the impact of sewage discharges on rivers. It can be used, for example to make figures like... 

... this one showing the stretches of the Thames downstream of active sewage discharges at the shown time...  

![map](https://github.com/AlexLipp/POOPy/assets/10188895/395732dc-54c1-403e-b681-be3bece7f7e7)

...or this one which shows the discharge history of a specific monitor...

![bourton_on_the_water](https://github.com/AlexLipp/POOPy/assets/10188895/feeb6035-78f0-4c48-b3f2-bd1d18f2ce96)

...or this one which shows the number of live monitors deployed by Thames Water through time and whether they were discharging...

![image](https://github.com/AlexLipp/POOPy/assets/10188895/8c631231-bf9c-406e-a393-4d1a72d355b3)

## Installation

Install this package by running the following command (replacing `[LOCAL DIRECTORY]` with the directory you wish to install the package into).
Note that this requires `Cython` to be installed (for example, `conda install -c anaconda cython`).

```bash
git clone https://github.com/AlexLipp/POOPy.git [LOCAL DIRECTORY]
pip install .
```

## Usage 

Once installed, the package can be imported into Python scripts using the following command.

```python
import poopy
```

Some examples of use are given in the `examples` folder.
