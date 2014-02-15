Overview
--------
Set of command line tools for getting data in and out of Elasticsearch.
Currently, there are 2 tools: esload (load data into es) and esdump (dump data
out of es). Connect the two with a pipe and profit. 

Installation
------------

    pip install https://github.com/mkocikowski/estools/archive/dev.zip
    esload --help
    esdump --help

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

You have a file 'data1.json' which has 1 document per line, and you want to
load it into the 'my_index' index on the local ES node:

    esload my_index data1.json

If you have data in file 'data2.json' in Cloudfiles container 'foo' in
Rackspace region 'DFW', then you can load this data with: 

    esload my_index cf://DFW/foo/data2.json

You can combine data from multiple sources by listing multiple URIs: 

    esload my_index data1.json cf://DFW/foo/data2.json

Load data from Kafka
--------------------
Note that you have to quote the URI to not confuse the shell:

    esload my_index "kafka://192.168.44.11:9093,192.168.44.11:9094;;mytopic"
    esload my_index "kafka://192.168.44.11:9093,192.168.44.11:9094;mygroup;mytopic"
    


Copying data
------------
You can dump data from an ES index with the 'esdump' command. You can then
pipe that data into 'esload': 

    esdump source_index | esload target_index

Run 'esdump -h' and 'esload -h' for information on how to connect to remote
hosts - this is how you can copy data between machines.

View data is Elasticsearch
--------------------------

As you are loading the data, you will see a bunch of output, listing document
ids, which look something like this: "wYPyMUWFRiuP0ffzNawRLA". To access an
indexed document: 

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
    
