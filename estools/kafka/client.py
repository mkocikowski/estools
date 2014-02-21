import re
import time

import kafka.consumer


def from_path(path):    
    hosts, topic = re.search(r"(?:kafka://)(.*)(?:;)(.*$)", path).groups()
    return (hosts, topic)
    

def get_lines(path, failfast=False): 
    hosts, topic = from_path(path)
    with kafka.consumer.KafkaConsumer(hosts=hosts, group="", topic=topic, whence=kafka.consumer.WHENCE_TAIL, failfast=failfast) as consumer:
        while True: 
            messages = consumer.fetch()
            for m in messages:
                yield m
            time.sleep(1)

