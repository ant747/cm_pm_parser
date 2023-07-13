import csv

import gzip
from io import BytesIO
from collections import deque
import logging
from logging.handlers import RotatingFileHandler
import os
from lxml import etree
from tinydb import TinyDB, Query
from tinydb.storages import MemoryStorage

logger = logging.getLogger(__name__)

schema_filename = 'parse_test_pm_nokia.py.csv'
path_to_object = deque()
report = []
find_tag = ['CellShutdownSwitch', 'StartTime', 'StopTime', 'UENumThd', 'DlPrbThd', 'DlPrbOffset',
            'UlPrbThd', 'CarrShutdownTrigJudgePrd',
            'InterFreqMlbThd', 'LoadOffset', 'OverlapInd']
find_number = ['M8020C6', 'M40002C2', 'M40002C1', 'M40002C0', 'M8020C10', 'M8020C9', 'M8020C8', 'M8020C7', 'M8011C96',
               'M8011C97',
               'M8011C98', 'M8011C99', 'M8011C100', 'M8011C101', 'M8011C102', 'M8011C103', 'M8011C104',
               'M8011C105', 'M8011C106', 'M8011C107', 'M8011C90', 'M8011C91', 'M8011C92', 'M8011C93', 'M8011C94',
               'M8011C95']
fieldnames = ['path_to_object','SON_Name','Parametr_id','Default_Value','Note']


# DONE: add gz support
# DONE: report filename based on input filename
# DONE: add support of files list as input argument
# DONE: add name of file to report
# DONE: fix bug with path identification

# TODO: add managed element as a column in the report
# TODO: add cell name into report
# 	    vsDataEUtranCellTDD
# 	    vsDataEUtranCellFDD

# TODO: add multiprocessing
# TODO: big files support
# TODO: add expected value for each policy in schema
# TODO: add policy id for each line of report
# TODO: add filter for path_to_object
# TODO: add default None values for target attributes (don't care if it was found or not)
# TODO: remove duplicates in schema on schema load
# TODO: add app_name into schema and report
# TODO: support filter of  shcema by app_name for script to collect specific app data only


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


def get_tag_without_schema(elem):
    tag = elem.tag.split('}')[-1]
    return tag


if __name__ == '__main__':
    init_my_logging()


    def read_xml(xml_filename):
        lst = []
        if xml_filename.split('.')[-1] == 'gz':
            xml_filename = gzip.open(xml_filename)
        data = {}
        logger.info(f'Working on {xml_filename}...')
        context = etree.iterparse(xml_filename, events=("start", "end"))
        path_to_object = ''
        mo_name = ''
        Parametr_id = ''
        for event, elem in context:
            tag = get_tag_without_schema(elem)
            if event == 'start':
                if elem.attrib.get('distName', ''):
                    path_to_object = elem.attrib.get('distName', '')
                if elem.attrib.get('dateTime', ''):
                    start_date = elem.attrib.get('dateTime')
                if elem.attrib.get('class',''):
                    mo_name = elem.attrib.get('class','')
                if elem.attrib.get('name', ''):
                    Parametr_id = elem.attrib.get('name')
                    if Parametr_id in find_tag:
                        data['Parametr_id'] = Parametr_id
                        data['value'] = elem.text
            if event == 'end':
                if data.get('Parametr_id',''):
                    data['path_to_object'] = path_to_object
                    data['mo_name'] = mo_name
                    lst.append(data)
                    data = {}
                elem.clear()


        return lst


    def initialize_output(report_filename):
        with open(report_filename, "w", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()


    def add_data_to_csv(path, report_filename):
        with open(report_filename, "a", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            for p in path:
                writer.writerow(p)
            logger.info(f"Added row {len(path)} records to csv")


    def process_directory(directory):
        for root, dirs, files in os.walk(directory):
            for file in files:
                file_path = os.path.join(root, file)
                lst = read_xml(file_path)
                file_name = file + '.csv'
                initialize_output(file_name)
                add_data_to_csv(lst, file_name)

            for folder in dirs:
                folder_path = os.path.join(root, folder)
                process_directory(folder_path)


    process_directory('/home/uventus/Works/NSN/NSN-LTE-PM files/NSN-PM files/Nokia')
