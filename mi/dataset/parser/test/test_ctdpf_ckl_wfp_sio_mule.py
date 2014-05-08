#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_ctdpf_ckl_wfp_sio_mule
@file marine-integrations/mi/dataset/parser/test/test_ctdpf_ckl_wfp_sio_mule.py
@author cgoodrich
@brief Test code for a ctdpf_ckl_wfp_sio_mule data parser
"""
import os
import struct
import ntplib
from StringIO import StringIO

from nose.plugins.attrib import attr

from mi.core.log import get_logger
log = get_logger()
from mi.core.exceptions import SampleException
from mi.idk.config import Config

from mi.dataset.test.test_parser import ParserUnitTestCase
from mi.dataset.dataset_driver import DataSetDriverConfigKeys
from mi.dataset.parser.ctdpf_ckl_wfp_sio_mule import CtdpfCklWfpSioMuleParser
from mi.dataset.parser.ctdpf_ckl_wfp_sio_mule import CtdpfCklWfpSioMuleDataParticle,\
    CtdpfCklWfpSioMuleMetadataParticle

# Data stream which contains a decimation factor
TEST_DATA_wdf = b'\x01\x57\x43\x31\x32\x33\x36\x38\x32\x30\x5f' + \
                 '\x30\x30\x33\x36\x48\x35\x31\x46\x32\x35\x42' + \
                 '\x44\x43\x5f\x31\x34\x5f\x35\x45\x34\x36\x02' + \
                 '\x00\x1a\x88\x03\xe3\x3b\x00\x03\xeb\x0a\xc8' + \
                 '\x00\x1a\x8c\x03\xe2\xc0\x00\x03\xeb\x0a\x81' + \
                 '\x00\x1a\x90\x03\xe1\x5b\x00\x03\xeb\x0a\x65' + \
                 '\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff' + \
                 '\x52\x4e\x75\x82\x52\x4e\x76\x9a\x30\x30\x03'

# Data stream which does not contain a decimation factor
TEST_DATA_ndf = b'\x01\x57\x43\x31\x32\x33\x36\x38\x32\x30\x5f' + \
                 '\x30\x30\x33\x34\x48\x35\x31\x46\x32\x35\x42' + \
                 '\x44\x43\x5f\x31\x34\x5f\x39\x41\x32\x38\x02' + \
                 '\x00\x1a\x88\x03\xe3\x3b\x00\x03\xeb\x0a\xc8' + \
                 '\x00\x1a\x8c\x03\xe2\xc0\x00\x03\xeb\x0a\x81' + \
                 '\x00\x1a\x90\x03\xe1\x5b\x00\x03\xeb\x0a\x65' + \
                 '\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff' + \
                 '\x52\x4e\x75\x82\x52\x4e\x76\x9a\x03'

# Data stream which does not have enough timestamp bytes
TEST_DATA_bts = b'\x01\x57\x43\x31\x32\x33\x36\x38\x32\x30\x5f' + \
                 '\x30\x30\x33\x33\x48\x35\x31\x46\x32\x35\x42' + \
                 '\x44\x43\x5f\x31\x34\x5f\x31\x45\x36\x33\x02' + \
                 '\x00\x1a\x88\x03\xe3\x3b\x00\x03\xeb\x0a\xc8' + \
                 '\x00\x1a\x8c\x03\xe2\xc0\x00\x03\xeb\x0a\x81' + \
                 '\x00\x1a\x90\x03\xe1\x5b\x00\x03\xeb\x0a\x65' + \
                 '\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff' + \
                 '\x52\x4e\x75\x82\x52\x4e\x76\x03'

# Data stream which has no timestamp bytes
TEST_DATA_nts = b'\x01\x57\x43\x31\x32\x33\x36\x38\x32\x30\x5f' + \
                 '\x30\x30\x32\x43\x48\x35\x31\x46\x32\x35\x42' + \
                 '\x44\x43\x5f\x31\x34\x5f\x33\x31\x39\x37\x02' + \
                 '\x00\x1a\x88\x03\xe3\x3b\x00\x03\xeb\x0a\xc8' + \
                 '\x00\x1a\x8c\x03\xe2\xc0\x00\x03\xeb\x0a\x81' + \
                 '\x00\x1a\x90\x03\xe1\x5b\x00\x03\xeb\x0a\x65' + \
                 '\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff' + \
                 '\x03'

# Data stream which has a bad end-of-profile marker
TEST_DATA_beop = b'\x01\x57\x43\x31\x32\x33\x36\x38\x32\x30\x5f' + \
                  '\x30\x30\x33\x34\x48\x35\x31\x46\x32\x35\x42' + \
                  '\x44\x43\x5f\x31\x34\x5f\x45\x34\x43\x32\x02' + \
                  '\x00\x1a\x88\x03\xe3\x3b\x00\x03\xeb\x0a\xc8' + \
                  '\x00\x1a\x8c\x03\xe2\xc0\x00\x03\xeb\x0a\x81' + \
                  '\x00\x1a\x90\x03\xe1\x5b\x00\x03\xeb\x0a\x65' + \
                  '\xff\xff\xff\xff\xff\xfe\xff\xff\xff\xff\xff' + \
                  '\x52\x4e\x75\x82\x52\x4e\x76\x9a\x03'

# Data stream which has no end-of-profile marker
TEST_DATA_neop = b'\x01\x57\x43\x31\x32\x33\x36\x38\x32\x30\x5f' + \
                  '\x30\x30\x33\x34\x48\x35\x31\x46\x32\x35\x42' + \
                  '\x44\x43\x5f\x31\x34\x5f\x39\x34\x37\x41\x02' + \
                  '\x00\x1a\x88\x03\xe3\x3b\x00\x03\xeb\x0a\xc8' + \
                  '\x00\x1a\x8c\x03\xe2\xc0\x00\x03\xeb\x0a\x81' + \
                  '\x00\x1a\x90\x03\xe1\x5b\x00\x03\xeb\x0a\x65' + \
                  '\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00' + \
                  '\x52\x4e\x75\x82\x52\x4e\x76\x9a\x03'

# Data stream which has no data records
TEST_DATA_ndr = b'\x01\x57\x43\x31\x32\x33\x36\x38\x32\x30\x5f' + \
                 '\x30\x30\x31\x33\x48\x35\x31\x46\x32\x35\x42' + \
                 '\x44\x43\x5f\x31\x34\x5f\x32\x42\x33\x30\x02' + \
                 '\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff' + \
                 '\x52\x4e\x75\x82\x52\x4e\x76\x9a\x03'

# Data stream which is way too short (no data, bad eop)
TEST_DATA_wts = b'\x01\x57\x43\x31\x32\x33\x36\x38\x32\x30\x5f' + \
                 '\x30\x30\x31\x31\x48\x35\x31\x46\x32\x35\x42' + \
                 '\x44\x43\x5f\x31\x34\x5f\x31\x43\x44\x33\x02' + \
                 '\xff\xff\xff\xff\xff\xff\xff\xff\xff\x52\x4e' + \
                 '\x75\x82\x52\x4e\x76\x9a\x03'

# Actual data contained in the data streams above
EXPECTED_TIME_STAMP = (1380873602, 1380873882, 3.0, '00')
EXPECTED_TIME_STAMP_ndf = (1380873602, 1380873882, 3.0, 0)

EXPECTED_VALUES_1 = (6792, 254779, 1003)
EXPECTED_VALUES_2 = (6796, 254656, 1003)
EXPECTED_VALUES_3 = (6800, 254299, 1003)

RESOURCE_PATH = os.path.join(Config().base_dir(), 'mi',
                             'dataset', 'driver', 'ctdpf_ckl',
                             'wfp_sio_mule', 'resource')


@attr('UNIT', group='mi')
class CtdpfCklWfpSioMuleParserUnitTestCase(ParserUnitTestCase):
    """
    ctdpf_ckl_wfp_sio_mule Parser unit test suite
    """
    def state_callback(self, file_ingested):
        """ Call back method to watch what comes in via the position callback """
        self.file_ingested_value = file_ingested

    def pub_callback(self, pub):
        """ Call back method to watch what comes in via the publish callback """
        self.publish_callback_value = pub

    def exception_callback(self, exception):
        """ Callback method to watch what comes in via the exception callback """
        self.exception_callback_value = exception

    def setUp(self):

        ParserUnitTestCase.setUp(self)

        self.config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.ctdpf_ckl_wfp_sio_mule',
            DataSetDriverConfigKeys.PARTICLE_CLASS: ['CtdpfCklWfpSioMuleDataParticle',
                                                     'CtdpfCklWfpSioMuleMetadataParticle']
            }

        # Define test data particles and their associated timestamps which will be
        # compared with returned results
        timefields = struct.unpack('>II', '\x52\x4e\x75\x82\x52\x4e\x76\x9a')
        start_time = int(timefields[0])
        end_time = int(timefields[1])
        
        time_increment = float(end_time - start_time) / 3.0

        self.start_timestamp = self.calc_timestamp(start_time, time_increment, 0)
        self.particle_meta = CtdpfCklWfpSioMuleMetadataParticle(EXPECTED_TIME_STAMP,
                                                                internal_timestamp=self.start_timestamp)
        self.particle_meta_ndf = CtdpfCklWfpSioMuleMetadataParticle(EXPECTED_TIME_STAMP_ndf,
                                                                internal_timestamp=self.start_timestamp)
        self.particle_a = CtdpfCklWfpSioMuleDataParticle(EXPECTED_VALUES_1,
                                                         internal_timestamp=self.start_timestamp)

        self.timestamp_2 = self.calc_timestamp(start_time, time_increment, 1)
        self.particle_b = CtdpfCklWfpSioMuleDataParticle(EXPECTED_VALUES_2,
                                                         internal_timestamp=self.timestamp_2)

        timestamp_3 = self.calc_timestamp(start_time, time_increment, 2)
        self.particle_c = CtdpfCklWfpSioMuleDataParticle(EXPECTED_VALUES_3,
                                                         internal_timestamp=timestamp_3)

# uncomment to generate yml
#self.particle_to_yml(self.particle_meta)
#self.particle_to_yml(self.particle_a)
#self.particle_to_yml(self.particle_b)
#self.particle_to_yml(self.particle_c)

        self.file_ingested_value = None
        self.state_callback_value = None
        self.publish_callback_value = None

    def calc_timestamp(self, start, increment, sample_idx):
        new_time = start + (increment * sample_idx)
        return float(ntplib.system_to_ntp_time(new_time))

    def assert_result(self, result, particle, ingested):
        self.assertEqual(result, [particle])
        self.assertEqual(self.file_ingested_value, ingested)
        self.assert_(isinstance(self.publish_callback_value, list))
        self.assertEqual(self.publish_callback_value[0], particle)

    @staticmethod
    def particle_to_yml(self, particle):
        """
        This is added as a testing helper, not actually as part of the parser tests. Since the same particles
        will be used for the driver test it is helpful to write them to .yml in the same form they need in the
        results.yml files here.
        """
        particle_dict = particle.generate_dict()
        # open write append, if you want to start from scratch manually delete this file
        fid = open('particle.yml', 'a')
        fid.write('  - _index: 1\n')
        fid.write('    internal_timestamp: %f\n' % particle_dict.get('internal_timestamp'))
        fid.write('    particle_object: %s\n' % particle.__class__.__name__)
        fid.write('    particle_type: %s\n' % particle_dict.get('stream_name'))
        for val in particle_dict.get('values'):
            if isinstance(val.get('value'), float):
                fid.write('    %s: %16.20f\n' % (val.get('value_id'), val.get('value')))
            else:
                fid.write('    %s: %s\n' % (val.get('value_id'), val.get('value')))
        fid.close()

    def test_simple_with_decimation_factor(self):
        """
        Read test data. Should detect that there is a decimation factor in the data.
        Check that the data matches the expected results.
        """
        log.debug('CAG TEST: WITH DECIMATION FACTOR')
        stream_handle = StringIO(TEST_DATA_wdf)
        self.parser =  CtdpfCklWfpSioMuleParser(self.config, None, stream_handle,
                                                self.state_callback, self.pub_callback, self.exception_callback)
        # next get records
        result = self.parser.get_records(4)

        self.assertEqual(result, [self.particle_meta,
                                  self.particle_a,
                                  self.particle_b,
                                  self.particle_c])

    def test_simple_with_no_decimation_factor(self):
        """
        Read test data. Should detect that there is NO decimation factor in the data.
        Check that the data matches the expected results.
        """
        log.debug('CAG TEST: NO DECIMATION FACTOR')
        stream_handle = StringIO(TEST_DATA_ndf)
        self.parser =  CtdpfCklWfpSioMuleParser(self.config, None, stream_handle,
                                                self.state_callback, self.pub_callback, self.exception_callback)
        # next get records
        result = self.parser.get_records(4)

        self.assertEqual(result, [self.particle_meta_ndf,
                                  self.particle_a,
                                  self.particle_b,
                                  self.particle_c])

    def test_simple_with_bad_time_stamp(self):
        """
        Read test data. Should detect that the data has a bad time stamp (only 7 bytes).
        Data stream should be rejected.
        """
        log.debug('CAG TEST: BAD TIME STAMP')
        with self.assertRaises(SampleException):
            stream_handle = StringIO(TEST_DATA_bts)
            self.parser =  CtdpfCklWfpSioMuleParser(self.config, None, stream_handle,
                                                    self.state_callback, self.pub_callback, self.exception_callback)
            # next get records
            result = self.parser.get_records(4)

    def test_simple_with_no_time_stamp(self):
        """
        Read test data. Should detect that the data is missing the time stamp.
        Data stream should be rejected.
        """
        log.debug('CAG TEST: NO TIME STAMP')
        with self.assertRaises(SampleException):
            stream_handle = StringIO(TEST_DATA_nts)
            self.parser =  CtdpfCklWfpSioMuleParser(self.config, None, stream_handle,
                                                    self.state_callback, self.pub_callback, self.exception_callback)
            # next get records
            result = self.parser.get_records(4)

    def test_simple_with_bad_eop(self):
        """
        Read test data. Should detect that the End of Profile (eop) is not all "F"s.
        Data stream should be rejected.
        """
        log.debug('CAG TEST: BAD END OF PROFILE')
        with self.assertRaises(SampleException):
            stream_handle = StringIO(TEST_DATA_beop)
            self.parser =  CtdpfCklWfpSioMuleParser(self.config, None, stream_handle,
                                                    self.state_callback, self.pub_callback, self.exception_callback)
            # next get records
            result = self.parser.get_records(4)

    def test_simple_with_no_eop(self):
        """
        Read test data. Should detect that the End of Profile (eop) is missing.
        Data stream should be rejected.
        """
        log.debug('CAG TEST: NO END OF PROFILE')
        with self.assertRaises(SampleException):
            stream_handle = StringIO(TEST_DATA_neop)
            self.parser =  CtdpfCklWfpSioMuleParser(self.config, None, stream_handle,
                                                    self.state_callback, self.pub_callback, self.exception_callback)
            # next get records
            result = self.parser.get_records(4)

    def test_simple_with_no_data_recs(self):
        """
        Read test data. Should detect that there is no data between the header and footer.
        Data out should be a metadata particle only
        """
        log.debug('CAG TEST: NO DATA RECORDS')
        with self.assertRaises(SampleException):
            stream_handle = StringIO(TEST_DATA_ndr)
            self.parser =  CtdpfCklWfpSioMuleParser(self.config, None, stream_handle,
                                                    self.state_callback, self.pub_callback, self.exception_callback)
            # next get records
            result = self.parser.get_records(1)

    def test_simple_with_input_too_short(self):
        """
        Read test data. Should detect that the input stream ?????
        Data stream should be rejected.
        """
        log.debug('CAG TEST: WAY TOO SHORT')
        with self.assertRaises(SampleException):
            stream_handle = StringIO(TEST_DATA_wts)
            self.parser =  CtdpfCklWfpSioMuleParser(self.config, None, stream_handle,
                                                    self.state_callback, self.pub_callback, self.exception_callback)
            # next get records
            result = self.parser.get_records(1)
