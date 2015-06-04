Overview
--------
Command line tools for getting data in and out of Elasticsearch: esload
(load data into es) and esdump (dump data out of es). Connect the two
with a pipe and profit.


Installation
------------

    pip install --upgrade https://github.com/mkocikowski/estools/archive/master.zip
    esload --help
    esdump --help


Load data into Elasticsearch
----------------------------

Load data from file `data1.json` (one document per line) into index
`index1` with document type `doctype1`, on localhost:

    esload index1 doctype1 < data1.json


Dump data from Elasticsearch
----------------------------

Dump data from index `index1` document type `doctype1` into file:

	esdump index1 doctype1 > data1.json


License
-------

The project uses [the MIT license](http://opensource.org/licenses/MIT):

    Copyright (c) 2013 Mikolaj Kocikowski

    Permission is hereby granted, free of charge, to any person obtaining a
    copy of this software and associated documentation files (the "Software"),
    to deal in the Software without restriction, including without limitation
    the rights to use, copy, modify, merge, publish, distribute, sublicense,
    and/or sell copies of the Software, and to permit persons to whom the
    Software is furnished to do so, subject to the following conditions:

    The above copyright notice and this permission notice shall be included in
    all copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
    THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
    LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
    FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
    DEALINGS IN THE SOFTWARE.

