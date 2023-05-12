import json
from collections import namedtuple
import logging
import pkg_resources
from ..flex_logger.logger import BreadCrumbsLogDecorator
logger = BreadCrumbsLogDecorator(logging.getLogger(__name__))

base_fields = ['band_id', 'band', 'tech', 'min', 'max']
cdma_fields = base_fields + ['ul_min', 'ul_max', 'name']
lte_fields = cdma_fields + ['multiplexing', 'dl_only']
nr_fields = lte_fields
all_band_fields = set(nr_fields)


class BandRangesMixin(object):
    __slots__ = ()
    def __new__(cls, *args, **kwargs):
        all_kwargs = {key: kwargs.get(key, None) for key in cls._fields}
        return super(BandRangesMixin, cls).__new__(cls, *args, **all_kwargs)

    def __contains__(self, item):
        return self.min <= item <= self.max

    def to_json(self, fields=None):
        return {x: getattr(self, x) for x in self._fields if (not fields or x in fields)}


class NrBandRanges(BandRangesMixin, namedtuple('NrBandRanges', nr_fields)):
    pass


class LteBandRanges(BandRangesMixin, namedtuple('LteBandRanges', lte_fields)):
    pass


class BandProvider(object):

    def __init__(self, techs=None):
        if techs is None:
            techs = ['nr', 'lte']
        self._data = {}
        self._techs = techs
        super(BandProvider, self).__init__()

    def _get_data(self):
        if not self._data:
            data = json.loads(pkg_resources.resource_string(__name__, './bands.json'))
            self._data = {}
            for tech, bands in data.iteritems():
                if tech in self._techs:
                    frequency_class = globals()[tech[:1].upper() + tech[1:] + 'BandRanges']
                    for band_id, band in bands.iteritems():
                        band['tech'] = tech
                        band['band_id'] = int(band_id)
                        self._data[int(band_id)] = frequency_class(**band)
        return self._data

    def get(self, band_id=None, tech=None, freq_dl=None):
        '''
        Returns all bands matching specified criteria (None means the criteria is not specified).
        freq_dl matches bands that have this freq_dl in their range.
        '''
        all_bands = self._get_data()
        # filter by band_id
        if band_id is not None:
            band = all_bands.get(band_id)
            result = [band] if band else []
        else:
            result = all_bands.values()
        # filter by tech
        if tech is not None:
            result = [x for x in result if x.tech == tech]
        # filter by freq_dl
        if freq_dl is not None:
            result = [x for x in result if freq_dl in x]
        return result

    def get_band_by_freq_dl(self, tech, freq_dl):
        bands = self.get(tech=tech, freq_dl=freq_dl)
        if not bands:
            logger.warn('frequency : {} does not match any band'.format(freq_dl))
            return None
        return bands[0]

    def get_band_number_by_freq_dl(self, tech, freq_dl):
        band = self.get_band_by_freq_dl(tech, freq_dl)
        if tech != "gsm":
            return int(band.band) if band else None
        else:
            # For GSM, the band is not a number, but a string.
            return band.band if band else None

    def get_multiplexing(self, tech, freq_dl):
        band = self.get(tech=tech, freq_dl=freq_dl)
        if not band:
            return None
        try:
            # fdd and tdd are defined as lower case - value must be one of ['fdd', 'tdd']
            multiplexing = (band[0].multiplexing).lower()
        except:
            logger.warn('frequency : {} does not have multiplexing for technology {}'.format(freq_dl, tech))
            multiplexing = None
        return multiplexing


band_provider = BandProvider()