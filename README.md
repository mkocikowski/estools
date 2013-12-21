Installation
------------

    pip install https://github.com/mkocikowski/estools/archive/dev.zip
    esload --help

Install and run  Elasticsearch
------------------------------

If you don't have Elasticsearch on your machine: 

    wget https://download.elasticsearch.org/elasticsearch/elasticsearch/elasticsearch-0.90.8.tar.gz
    tar -zxvf elasticsearch-0.90.8.tar.gz
    ./elasticsearch-0.90.8/bin/elasticsearch -f

If you get a bunch of errors talking about network stuff, it is likely that
there is another node / es cluster on the same subnet. Edit the
"elasticsearch-0.90.8/config/elasticsearch.yml" file, uncommenting the
following line: 

    discovery.zen.ping.multicast.enabled: false

Load data into Elasticsearch
----------------------------

You have a file 'data.json' which has 1 document per line, and you want to
load it into the 'foo' index on the local ES node:

    esload my_index data.json

You will see a bunch of output, listing document ids, which look something
like this: "wYPyMUWFRiuP0ffzNawRLA". To access an indexed document: 

    curl localhost:9200/my_index/doc/wYPyMUWFRiuP0ffzNawRLA

Looking through the indexed data is a different story, I recommend the Chrome
plugin [sense](https://github.com/bleskes/sense), or the cli
[elsec](https://github.com/mkocikowski/elsec).


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
    