import xml.etree.cElementTree as ET
from pprint import pprint as pp
import csv
import logging
from logging.handlers import RotatingFileHandler
import os
import sys

logger = logging.getLogger(__name__)


def init_my_logging():
    """
    Configuring logging for a SON-like appearance when running locally
    """
    logger.setLevel(logging.DEBUG)
    log_file_name = os.path.splitext(os.path.realpath(__file__))[0] + '.log'
    handler = RotatingFileHandler(log_file_name, maxBytes=10 * pow(1024, 2), backupCount=3)
    log_format = "%(asctime)-15s [{}:%(name)s:%(lineno)s:%(funcName)s:%(levelname)s] %(message)s".format(
        os.getpid())
    handler.setLevel(logging.DEBUG)
    try:
        from colorlog import ColoredFormatter
        formatter = ColoredFormatter(log_format)
    except ImportError:
        formatter = logging.Formatter(log_format)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    # set a format which is simpler for console use
    formatter = logging.Formatter('%(message)s')
    # tell the handler to use this format
    console.setFormatter(formatter)
    # add the handler to the root logger
    logger.addHandler(console)


def get_tag(elem):
    return elem.tag.split('}')[-1]


if __name__ == '__main__':
    assert len(sys.argv) > 1, 'Specify filename/mask for xml file(s) to proceed'
    logger.info(f'Working on {sys.argv[1]}...')
    path = []
    data = {}
    path_to_object = None
    begin_time = None
    init_my_logging()
    for event, elem in ET.iterparse(sys.argv[1], events=("start", "end")):
        
        tag = get_tag(elem)
        if event == 'start':
            if tag == 'measCollec':
                begin_time = elem.get('beginTime')
            elif tag == 'granPeriod':
                data = {'begin_time': begin_time}
                data['duration'] = elem.get('duration')
                data['end_time'] = elem.get('endTime')
            elif tag == 'measType':
                data[elem.get('p')] = elem.text
            elif tag == 'measValue':
                path_to_object = elem.get('measObjLdn')
            elif tag == 'r':
                data[path_to_object+":"+elem.get('p')] = elem.text 
        elif event == 'end':
            if tag == 'measInfo':
                path.append(data)
                data = {}
                path_to_object = None
                begin_time = None

    fieldnames = ['path_to_object', 'start_date', 'end_date_time', 'duration', 'PM_name', 'value']
    report_filename = sys.argv[1]+'.csv'
    with open(report_filename, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

    with open(report_filename, 'a', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        for p in path:
            for k, v in p.items():
                xpath = {}
                if k.startswith('Manage'):
                    xpath['path_to_object'] = k.split(':')[0]
                    xpath['start_date'] = path[0]['begin_time']
                    xpath['end_date_time'] = p['end_time']
                    xpath['duration'] = p['duration'][2:-1]
                    xpath['PM_name'] = p[k.split(':')[1]]
                    xpath['value'] = v
                    writer.writerow(xpath)
        logger.info(f'Added row {len(path)} records to csv')
