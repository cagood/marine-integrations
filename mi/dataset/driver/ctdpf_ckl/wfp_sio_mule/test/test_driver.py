"""
@package mi.dataset.driver.ctdpf_ckl.wfp_sio_mule.test.test_driver
@file marine-integrations/mi/dataset/driver/ctdpf_ckl/wfp_sio_mule/driver.py
@author cgoodrich
@brief Test cases for ctdpf_ckl_wfp_sio_mule driver

USAGE:
 Make tests verbose and provide stdout
   * From the IDK
       $ bin/dsa/test_driver
       $ bin/dsa/test_driver -i [-t testname]
       $ bin/dsa/test_driver -q [-t testname]
"""

__author__ = 'cgoodrich'
__license__ = 'Apache 2.0'

from nose.plugins.attrib import attr
from pyon.agent.agent import ResourceAgentState
from interface.objects import ResourceAgentErrorEvent

from mi.core.log import get_logger
log = get_logger()
from mi.idk.exceptions import SampleTimeout
from mi.idk.dataset.unit_test import DataSetTestCase
from mi.idk.dataset.unit_test import DataSetIntegrationTestCase
from mi.idk.dataset.unit_test import DataSetQualificationTestCase

from mi.dataset.dataset_driver import DriverParameter
from mi.dataset.dataset_driver import DataSourceConfigKey, DataSetDriverConfigKeys
from mi.dataset.driver.ctdpf_ckl.wfp_sio_mule.driver import CtdpfCklWfpDataSetDriver, DataTypeKey
from mi.dataset.parser.ctdpf_ckl_wfp_sio_mule import CtdpfCklWfpSioMuleDataParticle
from mi.dataset.parser.ctdpf_ckl_wfp_sio_mule import CtdpfCklWfpSioMuleMetadataParticle, DataParticleType
from mi.dataset.parser.ctdpf_ckl_wfp import CtdpfCklWfpDataParticle, CtdpfCklWfpMetadataParticle
from mi.dataset.parser.sio_mule_common import StateKey
from mi.idk.util import remove_all_files
import os

SIO_PARTICLES = (CtdpfCklWfpSioMuleDataParticle, CtdpfCklWfpSioMuleMetadataParticle)
WFP_PARTICLES = (CtdpfCklWfpDataParticle, CtdpfCklWfpMetadataParticle)


DIR_WFP = '/tmp/dsatest1'
DIR_WFP_SIO_MULE = '/tmp/dsatest'


# Fill in driver details
DataSetTestCase.initialize(
    driver_module='mi.dataset.driver.ctdpf_ckl.wfp_sio_mule.driver',
    driver_class='CtdpfCklWfpDataSetDriver',
    agent_resource_id='123xyz',
    agent_name='Agent007',
    agent_packet_config=CtdpfCklWfpDataSetDriver.stream_config(),
    startup_config = {
        DataSourceConfigKey.RESOURCE_ID: 'ctdpf_ckl_wfp_sio_mule',
        DataSourceConfigKey.HARVESTER:
        {
            DataTypeKey.CTDPF_CKL_WFP:
            {
                DataSetDriverConfigKeys.DIRECTORY: DIR_WFP,
                DataSetDriverConfigKeys.PATTERN: 'C*.DAT',
                DataSetDriverConfigKeys.FREQUENCY: 1,
            },
            DataTypeKey.CTDPF_CKL_WFP_SIO_MULE:
            {
                DataSetDriverConfigKeys.DIRECTORY: DIR_WFP_SIO_MULE,
                DataSetDriverConfigKeys.PATTERN: 'node58p1.dat',
                DataSetDriverConfigKeys.FREQUENCY: 1,
            }
        },
        DataSourceConfigKey.PARSER: {}
    }
)


# The integration and qualification tests generated here are suggested tests,
# but may not be enough to fully test your driver. Additional tests should be
# written as needed.

###############################################################################
#                            INTEGRATION TESTS                                #
# Device specific integration tests are for                                   #
# testing device specific capabilities                                        #
###############################################################################
@attr('INT', group='mi')
class IntegrationTest(DataSetIntegrationTestCase):

    def clear_sample_data(self):

        if os.path.exists(DIR_WFP_SIO_MULE):
            log.debug("Clean all data from %s", DIR_WFP_SIO_MULE)
            remove_all_files(DIR_WFP_SIO_MULE)
        else:
            log.debug("Create directory %s", DIR_WFP_SIO_MULE)
            os.makedirs(DIR_WFP_SIO_MULE)

    def test_get_simple(self):
        """
        Test that we can get data from a small file.
        """
        self.clear_sample_data()

        self.driver.start_sampling()

        self.clear_async_data()
        self.create_sample_data_set_dir('WC_ONE.DAT', DIR_WFP_SIO_MULE, 'node58p1.dat')
        self.assert_data(SIO_PARTICLES, 'WC_ONE.yml', count=96, timeout=10)

        self.driver.stop_sampling()

    def test_harvester_new_file_exception(self):
        """
        Test an exception raised after the driver is started during
        the file read.  Should call the exception callback.
        """
        self.clear_sample_data()

        # create the file so that it is unreadable
        self.create_sample_data_set_dir('WC_ONE.DAT', DIR_WFP_SIO_MULE, 'node58p1.dat', mode=000)

        # Start sampling and watch for an exception
        self.driver.start_sampling()

        self.assert_exception(ValueError)

        # At this point the harvester thread is dead.  The agent
        # exception handle should handle this case.

    def test_get_large(self):
        """
        Test that we can get data from a large file.
        """
        self.clear_sample_data()

        self.driver.start_sampling()

        self.clear_async_data()
        self.create_sample_data_set_dir('BIG_GIANT_HEAD.dat', DIR_WFP_SIO_MULE, 'node58p1.dat')
        self.assert_data(SIO_PARTICLES, 'BIG_GIANT_HEAD.yml', count=42062, timeout=1200)

        self.driver.stop_sampling()

###############################################################################
#                            QUALIFICATION TESTS                              #
# Device specific qualification tests are for                                 #
# testing device specific capabilities                                        #
###############################################################################
@attr('QUAL', group='mi')
class QualificationTest(DataSetQualificationTestCase):

    def assert_all_queue_empty(self):
        """
        Assert the sample queue for all 3 data streams is empty
        """
        self.assert_sample_queue_size(DataParticleType.METADATA, 0)
        self.assert_sample_queue_size(DataParticleType.DATA, 0)

    def test_publish_path(self):
        """
        Setup an agent/driver/harvester/parser and verify that data is
        published out the agent
        """
        self.create_sample_data('first.DAT', 'C0000001.DAT')
        self.assert_initialize(final_state=ResourceAgentState.COMMAND)

        # NOTE: If the processing is not slowed down here, the engineering samples are
        # returned in the wrong order
        self.dataset_agent_client.set_resource({DriverParameter.RECORDS_PER_SECOND: 1})
        self.assert_start_sampling()

        # Verify we get one sample
        try:
            result = self.data_subscribers.get_samples(DataParticleType.METADATA, 1)
            log.debug("First RESULT: %s", result)

            result_2 = self.data_subscribers.get_samples(DataParticleType.DATA, 3)
            log.debug("Second RESULT: %s", result_2)

            result.extend(result_2)
            log.debug("Extended RESULT: %s", result)

            # Verify values
            self.assert_data_values(result, 'first.result.yml')
        except Exception as e:
            log.error("Exception trapped: %s", e)
            self.fail("Sample timeout.")

    def test_large_import(self):
        """
        Test importing a large number of samples from the file at once
        """
        self.create_sample_data('C0000038.DAT')
        self.assert_initialize()

        # get results for each of the data particle streams
        result1 = self.get_samples(DataParticleType.METADATA,1,10)
        result2 = self.get_samples(DataParticleType.DATA,270,40)

    def test_stop_start(self):
        """
        Test the agents ability to start data flowing, stop, then restart
        at the correct spot.
        """
        log.info("CONFIG: %s", self._agent_config())
        self.create_sample_data('first.DAT', "C0000001.DAT")

        self.assert_initialize(final_state=ResourceAgentState.COMMAND)

        # Slow down processing to 1 per second to give us time to stop
        self.dataset_agent_client.set_resource({DriverParameter.RECORDS_PER_SECOND: 1})
        self.assert_start_sampling()

        try:
            # Read the first file and verify the data
            result = self.get_samples(DataParticleType.METADATA)
            result2 = self.get_samples(DataParticleType.DATA, 3)
            result.extend(result2)
            log.debug("RESULT: %s", result)

            # Verify values
            self.assert_data_values(result, 'first.result.yml')
            self.assert_all_queue_empty()

            self.create_sample_data('second.DAT', "C0000002.DAT")
            # Now read the first three records (1 metadata, 2 data) of the second file then stop
            result = self.get_samples(DataParticleType.METADATA)
            result2 = self.get_samples(DataParticleType.DATA, 2)
            result.extend(result2)
            log.debug("got result 1 %s", result)
            self.assert_stop_sampling()
            self.assert_all_queue_empty()

            # Restart sampling and ensure we get the last 4 records of the file
            self.assert_start_sampling()
            result3 = self.get_samples(DataParticleType.DATA, 4)
            log.debug("got result 2 %s", result3)
            result.extend(result3)
            self.assert_data_values(result, 'second.result.yml')

            self.assert_all_queue_empty()
        except SampleTimeout as e:
            log.error("Exception trapped: %s", e, exc_info=True)
            self.fail("Sample timeout.")

    def test_shutdown_restart(self):
        """
        Test a full stop of the dataset agent, then restart the agent
        and confirm it restarts at the correct spot.
        """
        log.info("CONFIG: %s", self._agent_config())
        self.create_sample_data('first.DAT', "C0000001.DAT")

        self.assert_initialize(final_state=ResourceAgentState.COMMAND)

        # Slow down processing to 1 per second to give us time to stop
        self.dataset_agent_client.set_resource({DriverParameter.RECORDS_PER_SECOND: 1})
        self.assert_start_sampling()

        try:
            # Read the first file and verify the data
            result = self.get_samples(DataParticleType.METADATA)
            result2 = self.get_samples(DataParticleType.DATA, 3)
            result.extend(result2)
            log.debug("RESULT: %s", result)

            # Verify values
            self.assert_data_values(result, 'first.result.yml')
            self.assert_all_queue_empty()

            self.create_sample_data('second.DAT', "C0000002.DAT")
            # Now read the first three records (1 metadata, 2 data) of the second file then stop
            result = self.get_samples(DataParticleType.METADATA)
            result2 = self.get_samples(DataParticleType.DATA, 2)
            result.extend(result2)
            log.debug("got result 1 %s", result)
            self.assert_stop_sampling()
            self.assert_all_queue_empty()

            # stop the agent
            self.stop_dataset_agent_client()
            # re-start the agent
            self.init_dataset_agent_client()
            #re-initialize
            self.assert_initialize(final_state=ResourceAgentState.COMMAND)

            # Restart sampling and ensure we get the last 4 records of the file
            self.assert_start_sampling()
            result3 = self.get_samples(DataParticleType.DATA, 4)
            log.debug("got result 2 %s", result3)
            result.extend(result3)
            self.assert_data_values(result, 'second.result.yml')

            self.assert_all_queue_empty()
        except SampleTimeout as e:
            log.error("Exception trapped: %s", e, exc_info=True)
            self.fail("Sample timeout.")

    def test_parser_exception(self):
        """
        Test an exception is raised after the driver is started during
        record parsing.
        """
        self.clear_sample_data()
        self.create_sample_data('bad_num_samples.DAT', 'C0000001.DAT')
        self.create_sample_data('first.DAT', 'C0000002.DAT')

        self.assert_initialize()

        self.event_subscribers.clear_events()
        result = self.get_samples(DataParticleType.METADATA)
        result1 = self.get_samples(DataParticleType.DATA, 3)
        result.extend(result1)
        self.assert_data_values(result, 'first.result.yml')
        self.assert_all_queue_empty();

        # Verify an event was raised and we are in our retry state
        self.assert_event_received(ResourceAgentErrorEvent, 10)
        self.assert_state_change(ResourceAgentState.STREAMING, 10)