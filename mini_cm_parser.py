import csv
import json
import sys
import glob
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

schema_filename = 'mini_cm_parser_schema.csv'
path_to_object = deque()
report = []

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


class ReportFilterer(object):
    def __init__(self, schema_filename, report):
        self.schema_filename = schema_filename
        self.schema = self._read_schema_from_file()

        self.db = TinyDB(storage=MemoryStorage)
        self._save_report_to_db(report)
        self.filtered_report = []

    def _save_report_to_db(self, report):
        self.db.insert_multiple([r for r in report])

    def _read_schema_from_file(self):
        with open(self.schema_filename, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            schema = [row for row in reader]
        return schema

    def _read_hardcoded_schema(self):
        schema_json_content = '[{"object_type":"^vsDataCellSleepFunction$","object_id":".*","attribute":"^sleepMode$"},{"object_type":"^vsDataCellSleepFunction$","object_id":".*","attribute":"^covCellDlPrbWakeUpThreshold$"},{"object_type":"^vsDataCellSleepFunction$","object_id":".*","attribute":"^capCellDlPrbSleepThreshold$"},{"object_type":"^vsDataCellSleepFunction$","object_id":".*","attribute":"^sleepStartTime$"},{"object_type":"^vsDataCellSleepFunction$","object_id":".*","attribute":"^sleepEndTime$"},{"object_type":"^vsDataCellSleepFunction$","object_id":".*","attribute":"^capCellRrcConnSleepThreshold$"},{"object_type":"^vsDataCellSleepFunction$","object_id":".*","attribute":"^covCellRrcConnWakeUpThreshold$"},{"object_type":"^vsDataCellSleepFunction$","object_id":".*","attribute":"^coverageCellDiscovery$"},{"object_type":"^vsDataCellSleepFunction$","object_id":".*","attribute":"^capCellSleepMonitorDurTimer$"},{"object_type":"^vsDataCellSleepFunction$","object_id":".*","attribute":"^covCellWakeUpMonitorDurTimer$"},{"object_type":"^vsDataCellSleepFunction$","object_id":".*","attribute":"^isAllowedMsmOnCovCell$"},{"object_type":"^vsDataCellSleepFunction$","object_id":".*","attribute":"^capCellSleepProhibitInterval$"},{"object_type":"^vsDataCellSleepFunction$","object_id":".*","attribute":"^covCellWakeUpMonitorDurTHigh$"},{"object_type":"^vsDataEUtranCellRelation$","object_id":".*","attribute":"^sleepModeCovCellCandidate$"},{"object_type":"^vsDataEUtranCellRelation$","object_id":".*","attribute":"^sleepModeCoverageCell$"},{"object_type":"^vsDataEUtranCellRelation$","object_id":".*","attribute":"^sleepModeCapacityCell$"},{"object_type":"^vsDataCellSleepFunction$","object_id":".*","attribute":"^covCellDlPrbWakeUpThresHigh$"},{"object_type":"^vsDataCellSleepFunction$","object_id":".*","attribute":"^covCellRrcConnWakeUpThresHigh$"},{"object_type":"^vsDataEUtranFreqRelation$","object_id":".*","attribute":"^cellSleepCovCellMeasOn$"},{"object_type":"^vsDataCellSleepFunction$","object_id":".*","attribute":"^SleepState$"},{"object_type":"^vsDataCellSleepFunction$","object_id":".*","attribute":"^SleepProhibitStartTime$"},{"object_type":".*MimolSleepFunction$","object_id":".*","attribute":"^sleepMode$"},{"object_type":"^vsDataGUtrancellRelation$","object_id":".*","attribute":"^essEnabled$"},{"object_type":"^vsDataGUtrancellRelation$","object_id":".*","attribute":"^essCellScPairs_gNBessLocalScId$"},{"object_type":"^vsDataGUtrancellRelation$","object_id":".*","attribute":"^essCellScPairs_essScPairId$"},{"object_type":"^vsDataSectorCarrier$","object_id":".*","attribute":"^essScPairId$"},{"object_type":"^vsDataSectorCarrier$","object_id":".*","attribute":"^essScLocalId$"},{"object_type":"^vsDataSectorCarrier$","object_id":".*","attribute":"^administrativeState$"},{"object_type":"^vsDataEUtranCellFDD$","object_id":".*","attribute":"^administrativeState$"},{"object_type":"^vsDataFeatureState$","object_id":"^CXC4011958$","attribute":"^serviceState$"},{"object_type":"^vsDataFeatureState$","object_id":"^CXC4011958$","attribute":"^featureState$"},{"object_type":"^vsDataFeatureState$","object_id":"^CXC4011958$","attribute":"^licenseState$"},{"object_type":"^vsDataFeatureState$","object_id":"^CXC4011958$","attribute":"^description$"}]'
        schema = json.loads(schema_json_content)
        return schema

    def apply_policy(self):
        for i, policy in enumerate(self.schema):
            Line = Query()
            q1 = policy.get('object_type').strip()
            q2 = policy.get('object_id').strip()
            q3 = policy.get('attribute').strip()
            policy_report = self.db.search(
                (Line.object_type.matches(q1)) & (Line.object_id.matches(q2)) & (Line.attribute.matches(q3))
            )
            if policy_report:
                self.filtered_report.extend(policy_report)

        return self.filtered_report


class GeneralContainer(object):
    def __init__(self, tag):
        self.tag = tag
        self.dc_id = None
        self.dc_path_node = None
        self.vs_data_type = None
        self.attributes = dict()

    def feed(self, event, elem):
        if event != 'start':
            return
        if self.dc_id is None and elem.get('id'):
            self.dc_id = elem.get('id')
            self.vs_data_type = get_tag_without_schema(elem)
            self.dc_path_node = f'{self.vs_data_type}={self.dc_id}'

    def __repr__(self):
        return f'{self.__class__.__name__}({self.vs_data_type}={self.dc_id}, id={id(self)})'

    def __str__(self):
        return self.__repr__()


class VsDataContainer(GeneralContainer):
    def __init__(self, tag):
        super().__init__(tag)

    def feed(self, event, elem):
        if event != 'start':
            return
        tag = get_tag_without_schema(elem)
        self.dc_id = elem.get('id') if self.dc_id is None and elem.get('id') else self.dc_id
        if tag == 'vsDataType':
            self.vs_data_type = elem.text
            self.dc_path_node = f'{self.vs_data_type}={self.dc_id}'
        if tag == self.vs_data_type:
            for child in elem:
                tag = get_tag_without_schema(child)
                self.attributes[tag] = child.text


class ReportHandler(object):
    def __init__(self, xml_filename, chunk_size):
        self.report_filename = f'{xml_filename}.csv'
        self.chunk_size = chunk_size
        self.report = []
        self.fieldnames = ['path_to_object', 'ManagedElement', 'cell', 'object_type', 'object_id', 'attribute', 'value']
        self.init_report_file()

    def init_report_file(self):
        with open(self.report_filename, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=self.fieldnames)
            writer.writeheader()

    @property
    def is_chunk_full(self):
        return len(self.report) >= self.chunk_size

    def save_report_chunk(self):
        filterer = ReportFilterer(schema_filename, self.report)
        filtered_report = filterer.apply_policy()
        if filtered_report:
            with open(self.report_filename, 'a', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=self.fieldnames)
                for line in filtered_report:
                    writer.writerow(line)
        logger.info(f'report contain {len(self.report)} lines. filtered_report contain {len(filtered_report)} '
                    f'lines and they were added to {self.report_filename}')
        self.report = []

    def add_current_dc_to_report(self, path_to_object):
        report_lines = self.get_report_lines(path_to_object)
        self.report.extend(report_lines)

    def get_report_lines(self, path_to_object):
        report_lines = []
        common_line_data = {}
        common_line_data['path_to_object'] = ','.join([o.dc_path_node for o in path_to_object])
        current_dc = path_to_object[-1]
        mo = next((dc for dc in path_to_object if dc.vs_data_type == 'ManagedElement'), None)
        common_line_data['ManagedElement'] = mo.dc_id if mo else None

        cell = next((dc for dc in path_to_object if dc.vs_data_type in ('vsDataEUtranCellTDD', 'vsDataEUtranCellFDD')),
                    None)
        common_line_data['cell'] = cell.dc_id if cell else None

        common_line_data['object_type'] = current_dc.vs_data_type
        common_line_data['object_id'] = current_dc.dc_id

        for k, v in current_dc.attributes.items():
            line = dict(common_line_data)
            line['attribute'] = k
            line['value'] = v
            report_lines.append(line)

        return report_lines


if __name__ == '__main__':
    assert len(sys.argv) > 1, 'Specify filename/mask for xml or xml.gz file(s) to proceed'
    init_my_logging()

    for xml_filename in glob.glob(sys.argv[1]):
        logger.info(f'Working on {xml_filename}...')
        report_handler = ReportHandler(xml_filename=xml_filename, chunk_size=pow(10, 5))
        context = etree.iterparse(xml_filename, events=("start", "end"))

        for event, elem in context:
            tag = get_tag_without_schema(elem)
            if event == 'start':
                if tag == 'VsDataContainer':
                    dc = VsDataContainer(tag)
                    path_to_object.append(dc)
                elif elem.get('id'):
                    dc = GeneralContainer(tag)
                    path_to_object.append(dc)
            if not path_to_object:
                continue
            current_dc = path_to_object[-1]
            # print(event, tag, len(path_to_object), current_dc)
            if event == 'start':
                current_dc.feed(event, elem)
            elif event == 'end':
                if tag == current_dc.tag:
                    if current_dc.attributes:
                        report_handler.add_current_dc_to_report(path_to_object)
                        if report_handler.is_chunk_full:
                            report_handler.save_report_chunk()
                    popped = path_to_object.pop()
                    # print(len(path_to_object), popped.dc_path_node)

        report_handler.save_report_chunk()
