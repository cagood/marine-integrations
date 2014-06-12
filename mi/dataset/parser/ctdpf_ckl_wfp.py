#!/usr/bin/env python

"""
@package mi.dataset.parser.ctdpf_ckl_wfp
@file marine-integrations/mi/dataset/parser/ctdpf_ckl_wfp.py
@author cgoodrich
@brief Parser for the ctdpf_ckl_wfp dataset driver
Release notes:

Initial Release
"""

__author__ = 'cgoodrich'
__license__ = 'Apache 2.0'

import re
import struct

from mi.core.log import get_logger
log = get_logger()
from mi.core.common import BaseEnum
from mi.core.instrument.data_particle import DataParticle, DataParticleKey
from mi.core.exceptions import SampleException, DatasetParserException

from mi.dataset.parser.wfp_c_file_common import WfpCFileCommonParser, WfpMetadataParserDataParticleKey
from mi.dataset.parser.wfp_c_file_common import DATA_RECORD_BYTES, TIME_RECORD_BYTES
from mi.dataset.parser.ctdpf_ckl_wfp_particles import CtdpfCklWfpRecoveredDataParticle
from mi.dataset.parser.ctdpf_ckl_wfp_particles import CtdpfCklWfpTelemeteredDataParticle
from mi.dataset.parser.ctdpf_ckl_wfp_particles import CtdpfCklWfpRecoveredMetadataParticle
from mi.dataset.parser.ctdpf_ckl_wfp_particles import CtdpfCklWfpTelemeteredMetadataParticle
from mi.dataset.parser.ctdpf_ckl_wfp_particles import DataParticleType
from mi.dataset.parser.ctdpf_ckl_wfp_particles import CtdpfCklWfpDataParticleKey


class CtdpfCklWfpParser(WfpCFileCommonParser):

    def __init__(self,
                 config,
                 state,
                 stream_handle,
                 state_callback,
                 publish_callback,
                 exception_callback,
                 filesize,
                 *args, **kwargs):

#        log.info(config)
        particle_classes_dict = config.get('particle_classes_dict')
        self._instrument_data_particle_class = particle_classes_dict.get('instrument_data_particle_class')
        self._metadata_particle_class = particle_classes_dict.get('metadata_particle_class')

        super(CtdpfCklWfpParser, self).__init__(config,
                                                state,
                                                stream_handle,
                                                state_callback,
                                                publish_callback,
                                                exception_callback,
                                                filesize,
                                                *args, **kwargs)

    def extract_metadata_particle(self, raw_data, timestamp):
        """
        Class for extracting the metadata data particle
        @param raw_data raw data to parse, in this case a tuple of the time string to parse and the number of records
        @param timestamp timestamp in NTP64
        """
        sample = self._extract_sample(self._metadata_particle_class, None, raw_data, timestamp)
        return sample

    def extract_data_particle(self, raw_data, timestamp):
        """
        Class for extracting the data sample data particle
        @param raw_data the raw data to parse
        @param timestamp the timestamp in NTP64
        """
        sample = self._extract_sample(self._instrument_data_particle_class, None, raw_data, timestamp)
        return sample
