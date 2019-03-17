# rhizopus

Rhizopus is a Python trading simulation framework and a backtesting tool. It
can be used to construct broker simulators for backtesting with historical 
data, as well as for live trading. It's main idea is to provide a simple unified 
interface for both backtesting and live trading.

## Features

* Support for multiple currencies.
* Bid-ask spreads.
* Easy integration of any type to transaction costs, e.g. fixed transaction fees.

## Installation

Clone this repository and call `pip install` from the main directory:

    git clone https://github.com/jwergieluk/rhizopus.git
    cd rhizopus
    pip install -e .

rhizopus does not depend on any other python package 
outside of the Python standard library.

## License

rhizopus is released unter GNU GENERAL PUBLIC LICENSE Version 3. 
See LICENSE file for details.

Copyright (c) 2016--2019 Julian Wergieluk