import re
import time

import kafka.consumer


def from_path(path):    
    hosts, group, topic = re.search(r"(?:kafka://)(.*)(?:;)(.*)(?:;)(.*$)", path).groups()
    if not group: group = None
    return (hosts, group, topic)
    

def get_lines(path, failfast=False): 
    hosts, group, topic = from_path(path)
    with kafka.consumer.KafkaConsumer(hosts, group, topic, failfast=failfast) as consumer:
        while True: 
            messages = consumer.fetch()
            for m in messages:
                yield m
            time.sleep(1)

