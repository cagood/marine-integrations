"""
@package mi.instrument.noaa.iris.ooicore.test.test_driver
@file marine-integrations/mi/instrument/noaa/iris/ooicore/driver.py
@author David Everett
@brief Test cases for ooicore driver

USAGE:
 Make tests verbose and provide stdout
   * From the IDK
       $ bin/test_driver
       $ bin/test_driver -u [-t testname]
       $ bin/test_driver -i [-t testname]
       $ bin/test_driver -q [-t testname]
"""

__author__ = 'David Everett'
__license__ = 'Apache 2.0'

import time

import ntplib
from nose.plugins.attrib import attr
from mock import Mock

from mi.core.log import get_logger

log = get_logger()

# MI imports.
from mi.idk.unit_test import InstrumentDriverTestCase
from mi.idk.unit_test import InstrumentDriverUnitTestCase
from mi.idk.unit_test import InstrumentDriverIntegrationTestCase
from mi.idk.unit_test import InstrumentDriverQualificationTestCase
from mi.idk.unit_test import DriverTestMixin
from mi.idk.unit_test import ParameterTestConfigKey
from mi.idk.unit_test import AgentCapabilityType
from mi.core.instrument.port_agent_client import PortAgentPacket
from mi.core.instrument.chunker import StringChunker
from mi.core.common import BaseEnum
from mi.core.instrument.instrument_driver import DriverParameter
from mi.instrument.noaa.botpt.driver import BotptStatus01ParticleKey
from mi.instrument.noaa.botpt.iris.driver import InstrumentDriver
from mi.instrument.noaa.botpt.iris.driver import IRISStatus02ParticleKey
from mi.instrument.noaa.botpt.iris.driver import DataParticleType
from mi.instrument.noaa.botpt.iris.driver import IRISDataParticleKey
from mi.instrument.noaa.botpt.iris.driver import IRISDataParticle
from mi.instrument.noaa.botpt.iris.driver import InstrumentCommand
from mi.instrument.noaa.botpt.iris.driver import ProtocolState
from mi.instrument.noaa.botpt.iris.driver import ProtocolEvent
from mi.instrument.noaa.botpt.iris.driver import Capability
from mi.instrument.noaa.botpt.iris.driver import Protocol
from mi.instrument.noaa.botpt.iris.driver import NEWLINE
from mi.instrument.noaa.botpt.iris.driver import IRIS_COMMAND_STRING
from mi.instrument.noaa.botpt.iris.driver import IRIS_STRING
from mi.instrument.noaa.botpt.iris.driver import IRIS_DATA_ON
from mi.instrument.noaa.botpt.iris.driver import IRIS_DATA_OFF
from mi.instrument.noaa.botpt.iris.driver import IRIS_DUMP_01
from mi.instrument.noaa.botpt.iris.driver import IRIS_DUMP_02

from mi.core.exceptions import SampleException
from pyon.agent.agent import ResourceAgentState

###
#   Driver parameters for the tests
###
InstrumentDriverTestCase.initialize(
    driver_module='mi.instrument.noaa.botpt.iris.driver',
    driver_class="InstrumentDriver",

    instrument_agent_resource_id='1D644T',
    instrument_agent_name='noaa_iris_ooicore',
    instrument_agent_packet_config=DataParticleType(),

    driver_startup_config={}
)

GO_ACTIVE_TIMEOUT = 180

#################################### RULES ####################################
#                                                                             #
# Common capabilities in the base class                                       #
#                                                                             #
# Instrument specific stuff in the derived class                              #
#                                                                             #
# Generator spits out either stubs or comments describing test this here,     #
# test that there.                                                            #
#                                                                             #
# Qualification tests are driven through the instrument_agent                 #
#                                                                             #
###############################################################################

###
#   Driver constant definitions
###

INVALID_SAMPLE = "This is an invalid sample; it had better cause an exception." + NEWLINE
VALID_SAMPLE_01 = "IRIS,2013/05/29 00:25:34, -0.0882, -0.7524,28.45,N8642" + NEWLINE
VALID_SAMPLE_02 = "IRIS,2013/05/29 00:25:36, -0.0885, -0.7517,28.49,N8642" + NEWLINE

DATA_ON_COMMAND_RESPONSE = "IRIS,2013/05/29 00:23:34," + IRIS_COMMAND_STRING + IRIS_DATA_ON + NEWLINE
DATA_OFF_COMMAND_RESPONSE = "IRIS,2013/05/29 00:23:34," + IRIS_COMMAND_STRING + IRIS_DATA_OFF + NEWLINE
DUMP_01_COMMAND_RESPONSE = "IRIS,2013/05/29 00:22:57," + IRIS_COMMAND_STRING + IRIS_DUMP_01 + NEWLINE
DUMP_02_COMMAND_RESPONSE = "IRIS,2013/05/29 00:23:34," + IRIS_COMMAND_STRING + IRIS_DUMP_02 + NEWLINE

BOTPT_FIREHOSE_01 = "NANO,P,2013/05/16 17:03:22.000,14.858126,25.243003840" + NEWLINE
BOTPT_FIREHOSE_01 += "LILY,2013/05/16 17:03:22,-202.490,-330.000,149.88, 25.72,11.88,N9656" + NEWLINE
BOTPT_FIREHOSE_01 += "HEAT,2013/04/19 22:54:11,-001,0001,0025" + NEWLINE
BOTPT_FIREHOSE_01 += "IRIS,2013/05/29 00:25:34, -0.0882, -0.7524,28.45,N8642" + NEWLINE
BOTPT_FIREHOSE_01 += "NANO,P,2013/05/16 17:03:22.000,14.858126,25.243003840" + NEWLINE
BOTPT_FIREHOSE_01 += "LILY,2013/05/16 17:03:22,-202.490,-330.000,149.88, 25.72,11.88,N9656" + NEWLINE
BOTPT_FIREHOSE_01 += "HEAT,2013/04/19 22:54:11,-001,0001,0025" + NEWLINE

SIGNON_STATUS = \
    "IRIS,2013/06/12 18:03:44,*APPLIED GEOMECHANICS Model MD900-T Firmware V5.2 SN-N8642 ID01" + NEWLINE

DUMP_01_STATUS = \
    "IRIS,2013/06/19 21:13:00,*APPLIED GEOMECHANICS Model MD900-T Firmware V5.2 SN-N3616 ID01" + NEWLINE + \
    "IRIS,2013/06/12 18:03:44,*01: Vbias= 0.0000 0.0000 0.0000 0.0000" + NEWLINE + \
    "IRIS,2013/06/12 18:03:44,*01: Vgain= 0.0000 0.0000 0.0000 0.0000" + NEWLINE + \
    "IRIS,2013/06/12 18:03:44,*01: Vmin:  -2.50  -2.50   2.50   2.50" + NEWLINE + \
    "IRIS,2013/06/12 18:03:44,*01: Vmax:   2.50   2.50   2.50   2.50" + NEWLINE + \
    "IRIS,2013/06/12 18:03:44,*01: a0=    0.00000    0.00000    0.00000    0.00000    0.00000    0.00000" + NEWLINE + \
    "IRIS,2013/06/12 18:03:44,*01: a1=    0.00000    0.00000    0.00000    0.00000    0.00000    0.00000" + NEWLINE + \
    "IRIS,2013/06/12 18:03:44,*01: a2=    0.00000    0.00000    0.00000    0.00000    0.00000    0.00000" + NEWLINE + \
    "IRIS,2013/06/12 18:03:44,*01: a3=    0.00000    0.00000    0.00000    0.00000    0.00000    0.00000" + NEWLINE + \
    "IRIS,2013/06/12 18:03:44,*01: Tcoef 0: Ks=           0 Kz=           0 Tcal=           0" + NEWLINE + \
    "IRIS,2013/06/12 18:03:44,*01: Tcoef 1: Ks=           0 Kz=           0 Tcal=           0" + NEWLINE + \
    "IRIS,2013/06/12 18:03:44,*01: N_SAMP= 460 Xzero=  0.00 Yzero=  0.00" + NEWLINE + \
    "IRIS,2013/06/12 18:03:44,*01: TR-PASH-OFF E99-ON  SO-NMEA-SIM XY-EP  9600 baud FV-"

DUMP_02_STATUS = \
    "IRIS,2013/06/12 23:55:09,*01: TBias: 8.85" + NEWLINE + \
    "IRIS,2013/06/12 23:55:09,*Above 0.00(KZMinTemp): kz[0]=           0, kz[1]=           0" + NEWLINE + \
    "IRIS,2013/06/12 23:55:09,*Below 0.00(KZMinTemp): kz[2]=           0, kz[3]=           0" + NEWLINE + \
    "IRIS,2013/06/12 18:04:01,*01: ADCDelay:  310" + NEWLINE + \
    "IRIS,2013/06/12 18:04:01,*01: PCA Model: 90009-01" + NEWLINE + \
    "IRIS,2013/06/12 18:04:01,*01: Firmware Version: 5.2 Rev N" + NEWLINE + \
    "LILY,2013/06/12 18:04:01,-330.000,-247.647,290.73, 24.50,11.88,N9656" + NEWLINE + \
    "IRIS,2013/06/12 18:04:01,*01: X Ch Gain= 1.0000, Y Ch Gain= 1.0000, Temperature Gain= 1.0000" + NEWLINE + \
    "IRIS,2013/06/12 18:04:01,*01: Output Mode: Degrees" + NEWLINE + \
    "IRIS,2013/06/12 18:04:01,*01: Calibration performed in Degrees" + NEWLINE + \
    "IRIS,2013/06/12 18:04:01,*01: Control: Off" + NEWLINE + \
    "IRIS,2013/06/12 18:04:01,*01: Using RS232" + NEWLINE + \
    "IRIS,2013/06/12 18:04:01,*01: Real Time Clock: Not Installed" + NEWLINE + \
    "IRIS,2013/06/12 18:04:01,*01: Use RTC for Timing: No" + NEWLINE + \
    "IRIS,2013/06/12 18:04:01,*01: External Flash Capacity: 0 Bytes(Not Installed)" + NEWLINE + \
    "IRIS,2013/06/12 18:04:01,*01: Relay Thresholds:" + NEWLINE + \
    "IRIS,2013/06/12 18:04:01,*01:   Xpositive= 1.0000   Xnegative=-1.0000" + NEWLINE + \
    "IRIS,2013/06/12 18:04:01,*01:   Ypositive= 1.0000   Ynegative=-1.0000" + NEWLINE + \
    "IRIS,2013/06/12 18:04:01,*01: Relay Hysteresis:" + NEWLINE + \
    "IRIS,2013/06/12 18:04:01,*01:   Hysteresis= 0.0000" + NEWLINE + \
    "IRIS,2013/06/12 18:04:01,*01: Calibration method: Dynamic" + NEWLINE + \
    "IRIS,2013/06/12 18:04:01,*01: Positive Limit=26.25   Negative Limit=-26.25" + NEWLINE + \
    "IRIS,2013/06/12 18:04:02,*01: Calibration Points:025  X: Disabled  Y: Disabled" + NEWLINE + \
    "IRIS,2013/06/12 18:04:02,*01: Biaxial Sensor Type (0)" + NEWLINE + \
    "IRIS,2013/06/12 18:04:02,*01: ADC: 12-bit (internal)" + NEWLINE + \
    "IRIS,2013/06/12 18:04:02,*01: DAC Output Scale Factor: 0.10 Volts/Degree" + NEWLINE + \
    "HEAT,2013/06/12 18:04:02,-001,0001,0024" + NEWLINE + \
    "IRIS,2013/06/12 18:04:02,*01: Total Sample Storage Capacity: 372" + NEWLINE + \
    "IRIS,2013/06/12 18:04:02,*01: BAE Scale Factor:  2.88388 (arcseconds/bit)"

DUMP_01_STATUS_RESP = NEWLINE.join([line for line in DUMP_01_STATUS.split(NEWLINE) if line.startswith(IRIS_STRING)])
DUMP_02_STATUS_RESP = NEWLINE.join([line for line in DUMP_02_STATUS.split(NEWLINE) if line.startswith(IRIS_STRING)])


###############################################################################
#                           DRIVER TEST MIXIN                                 #
#     Defines a set of constants and assert methods used for data particle    #
#     verification                                                            #
#                                                                             #
#  In python mixin classes are classes designed such that they wouldn't be    #
#  able to stand on their own, but are inherited by other classes generally   #
#  using multiple inheritance.                                                #
#                                                                             #
# This class defines a configuration structure for testing and common assert  #
# methods for validating data particles.                                      #
###############################################################################
class IRISTestMixinSub(DriverTestMixin):
    TYPE = ParameterTestConfigKey.TYPE
    READONLY = ParameterTestConfigKey.READONLY
    STARTUP = ParameterTestConfigKey.STARTUP
    DA = ParameterTestConfigKey.DIRECT_ACCESS
    VALUE = ParameterTestConfigKey.VALUE
    REQUIRED = ParameterTestConfigKey.REQUIRED
    DEFAULT = ParameterTestConfigKey.DEFAULT
    STATES = ParameterTestConfigKey.STATES

    _driver_parameters = {
        # Parameters defined in the IOS
    }

    _sample_parameters_01 = {
        IRISDataParticleKey.TIME: {TYPE: float, VALUE: 3578801134.0, REQUIRED: True},
        IRISDataParticleKey.X_TILT: {TYPE: float, VALUE: -0.0882, REQUIRED: True},
        IRISDataParticleKey.Y_TILT: {TYPE: float, VALUE: -0.7524, REQUIRED: True},
        IRISDataParticleKey.TEMP: {TYPE: float, VALUE: 28.45, REQUIRED: True},
        IRISDataParticleKey.SN: {TYPE: unicode, VALUE: 'N8642', REQUIRED: True}
    }

    _sample_parameters_02 = {
        IRISDataParticleKey.TIME: {TYPE: float, VALUE: 3578801136.0, REQUIRED: True},
        IRISDataParticleKey.X_TILT: {TYPE: float, VALUE: -0.0885, REQUIRED: True},
        IRISDataParticleKey.Y_TILT: {TYPE: float, VALUE: -0.7517, REQUIRED: True},
        IRISDataParticleKey.TEMP: {TYPE: float, VALUE: 28.49, REQUIRED: True},
        IRISDataParticleKey.SN: {TYPE: unicode, VALUE: 'N8642', REQUIRED: True}
    }

    _status_parameters_01 = {
        BotptStatus01ParticleKey.TIME: {TYPE: float, VALUE: 3580690380.0, REQUIRED: True},
        BotptStatus01ParticleKey.MODEL: {TYPE: unicode, VALUE: 'Model MD900-T', REQUIRED: True},
        BotptStatus01ParticleKey.FIRMWARE_VERSION: {TYPE: unicode, VALUE: 'V5.2', REQUIRED: True},
        BotptStatus01ParticleKey.SERIAL_NUMBER: {TYPE: unicode, VALUE: 'SN-N3616', REQUIRED: True},
        BotptStatus01ParticleKey.ID_NUMBER: {TYPE: unicode, VALUE: 'ID01', REQUIRED: True},
        BotptStatus01ParticleKey.VBIAS: {TYPE: list, VALUE: [0.0] * 4, REQUIRED: True},
        BotptStatus01ParticleKey.VGAIN: {TYPE: list, VALUE: [0.0] * 4, REQUIRED: True},
        BotptStatus01ParticleKey.VMIN: {TYPE: list, VALUE: [-2.5] * 2 + [2.5] * 2, REQUIRED: True},
        BotptStatus01ParticleKey.VMAX: {TYPE: list, VALUE: [2.5] * 4, REQUIRED: True},
        BotptStatus01ParticleKey.AVALS_0: {TYPE: list, VALUE: [0.0] * 6, REQUIRED: True},
        BotptStatus01ParticleKey.AVALS_1: {TYPE: list, VALUE: [0.0] * 6, REQUIRED: True},
        BotptStatus01ParticleKey.AVALS_2: {TYPE: list, VALUE: [0.0] * 6, REQUIRED: True},
        BotptStatus01ParticleKey.AVALS_3: {TYPE: list, VALUE: [0.0] * 6, REQUIRED: True},
        BotptStatus01ParticleKey.TCOEF0_KS: {TYPE: int, VALUE: 0, REQUIRED: True},
        BotptStatus01ParticleKey.TCOEF0_KZ: {TYPE: int, VALUE: 0, REQUIRED: True},
        BotptStatus01ParticleKey.TCOEF0_TCAL: {TYPE: int, VALUE: 0, REQUIRED: True},
        BotptStatus01ParticleKey.TCOEF1_KS: {TYPE: int, VALUE: 0, REQUIRED: True},
        BotptStatus01ParticleKey.TCOEF1_KZ: {TYPE: int, VALUE: 0, REQUIRED: True},
        BotptStatus01ParticleKey.TCOEF1_TCAL: {TYPE: int, VALUE: 0, REQUIRED: True},
        BotptStatus01ParticleKey.N_SAMP: {TYPE: int, VALUE: 460, REQUIRED: True},
        BotptStatus01ParticleKey.XZERO: {TYPE: float, VALUE: 0.0, REQUIRED: True},
        BotptStatus01ParticleKey.YZERO: {TYPE: float, VALUE: 0.0, REQUIRED: True},
        BotptStatus01ParticleKey.BAUD: {TYPE: int, VALUE: 9600, REQUIRED: True},
    }

    _status_parameters_02 = {
        IRISStatus02ParticleKey.TIME: {TYPE: float, VALUE: 3580095309.0, REQUIRED: True},
        IRISStatus02ParticleKey.TBIAS: {TYPE: float, VALUE: 8.85, REQUIRED: True},
        IRISStatus02ParticleKey.ABOVE: {TYPE: float, VALUE: 0.0, REQUIRED: True},
        IRISStatus02ParticleKey.BELOW: {TYPE: float, VALUE: 0.0, REQUIRED: True},
        IRISStatus02ParticleKey.KZVALS: {TYPE: list, VALUE: [0] * 4, REQUIRED: True},
        IRISStatus02ParticleKey.ADC_DELAY: {TYPE: int, VALUE: 310},
        IRISStatus02ParticleKey.PCA_MODEL: {TYPE: unicode, VALUE: '90009-01'},
        IRISStatus02ParticleKey.FIRMWARE_REV: {TYPE: unicode, VALUE: '5.2 Rev N'},
        IRISStatus02ParticleKey.XCHAN_GAIN: {TYPE: float, VALUE: 1.0},
        IRISStatus02ParticleKey.YCHAN_GAIN: {TYPE: float, VALUE: 1.0},
        IRISStatus02ParticleKey.TEMP_GAIN: {TYPE: float, VALUE: 1.0},
        IRISStatus02ParticleKey.OUTPUT_MODE: {TYPE: unicode, VALUE: 'Degrees'},
        IRISStatus02ParticleKey.CAL_MODE: {TYPE: unicode, VALUE: 'Degrees'},
        IRISStatus02ParticleKey.CONTROL: {TYPE: unicode, VALUE: 'Off'},
        IRISStatus02ParticleKey.RS232: {TYPE: unicode, VALUE: 'RS232'},
        IRISStatus02ParticleKey.RTC_INSTALLED: {TYPE: unicode, VALUE: 'Not Installed'},
        IRISStatus02ParticleKey.RTC_TIMING: {TYPE: unicode, VALUE: 'No'},
        IRISStatus02ParticleKey.EXT_FLASH_CAPACITY: {TYPE: int, VALUE: 0, REQUIRED: True},
        IRISStatus02ParticleKey.XPOS_RELAY_THRESHOLD: {TYPE: float, VALUE: 1.0},
        IRISStatus02ParticleKey.XNEG_RELAY_THRESHOLD: {TYPE: float, VALUE: -1.0},
        IRISStatus02ParticleKey.YPOS_RELAY_THRESHOLD: {TYPE: float, VALUE: 1.0},
        IRISStatus02ParticleKey.YNEG_RELAY_THRESHOLD: {TYPE: float, VALUE: -1.0},
        IRISStatus02ParticleKey.RELAY_HYSTERESIS: {TYPE: float, VALUE: 0.0},
        IRISStatus02ParticleKey.CAL_METHOD: {TYPE: unicode, VALUE: 'Dynamic'},
        IRISStatus02ParticleKey.POS_LIMIT: {TYPE: float, VALUE: 26.25},
        IRISStatus02ParticleKey.NEG_LIMIT: {TYPE: float, VALUE: -26.25},
        IRISStatus02ParticleKey.NUM_CAL_POINTS: {TYPE: int, VALUE: 25},
        IRISStatus02ParticleKey.CAL_POINTS_X: {TYPE: unicode, VALUE: 'Disabled'},
        IRISStatus02ParticleKey.CAL_POINTS_Y: {TYPE: unicode, VALUE: 'Disabled'},
        IRISStatus02ParticleKey.SENSOR_TYPE: {TYPE: unicode, VALUE: 'Biaxial Sensor Type (0)'},
        IRISStatus02ParticleKey.ADC_TYPE: {TYPE: unicode, VALUE: '12-bit (internal)'},
        IRISStatus02ParticleKey.DAC_SCALE_FACTOR: {TYPE: float, VALUE: 0.10},
        IRISStatus02ParticleKey.DAC_SCALE_UNITS: {TYPE: unicode, VALUE: 'Volts/Degree'},
        IRISStatus02ParticleKey.SAMPLE_STORAGE_CAPACITY: {TYPE: int, VALUE: 372},
        IRISStatus02ParticleKey.BAE_SCALE_FACTOR: {TYPE: float, VALUE: 2.88388},
        IRISStatus02ParticleKey.BAE_SCALE_FACTOR_UNITS: {TYPE: unicode, VALUE: 'arcseconds/bit'},
    }

    def assert_particle(self, particle, particle_type=None, particle_key=None, params=None, verify_values=False):
        self.assert_data_particle_keys(particle_key, params)
        self.assert_data_particle_header(particle, particle_type, verify_values)
        self.assert_data_particle_parameters(particle, params, verify_values)

    def assert_particle_sample_01(self, data_particle, verify_values=False):
        self.assert_particle(data_particle, DataParticleType.IRIS_PARSED, IRISDataParticleKey,
                             self._sample_parameters_01, verify_values)

    def assert_particle_sample_02(self, data_particle, verify_values=False):
        self.assert_particle(data_particle, DataParticleType.IRIS_PARSED, IRISDataParticleKey,
                             self._sample_parameters_02, verify_values)

    def assert_particle_status_01(self, data_particle, verify_values=False):
        self.assert_particle(data_particle, DataParticleType.IRIS_STATUS1, BotptStatus01ParticleKey,
                             self._status_parameters_01, verify_values)

    def assert_particle_status_02(self, data_particle, verify_values=False):
        self.assert_particle(data_particle, DataParticleType.IRIS_STATUS2, IRISStatus02ParticleKey,
                             self._status_parameters_02, verify_values)


###############################################################################
#                                UNIT TESTS                                   #
#         Unit tests test the method calls and parameters using Mock.         #
#                                                                             #
#   These tests are especially useful for testing parsers and other data      #
#   handling.  The tests generally focus on small segments of code, like a    #
#   single function call, but more complex code using Mock objects.  However  #
#   if you find yourself mocking too much maybe it is better as an            #
#   integration test.                                                         #
#                                                                             #
#   Unit tests do not start up external processes like the port agent or      #
#   driver process.                                                           #
###############################################################################
# noinspection PyProtectedMember
@attr('UNIT', group='mi')
class DriverUnitTest(InstrumentDriverUnitTestCase, IRISTestMixinSub):
    def setUp(self):
        InstrumentDriverUnitTestCase.setUp(self)

    def _send_port_agent_packet(self, driver, data_item):
        ts = ntplib.system_to_ntp_time(time.time())
        port_agent_packet = PortAgentPacket()
        port_agent_packet.attach_data(data_item)
        port_agent_packet.attach_timestamp(ts)
        port_agent_packet.pack_header()
        driver._protocol.got_data(port_agent_packet)

    @staticmethod
    def my_send(driver):
        def inner(data):
            if data.startswith(InstrumentCommand.DATA_ON):
                my_response = DATA_ON_COMMAND_RESPONSE
            elif data.startswith(InstrumentCommand.DATA_OFF):
                my_response = DATA_OFF_COMMAND_RESPONSE
            elif data.startswith(InstrumentCommand.DUMP_SETTINGS_01):
                my_response = DUMP_01_COMMAND_RESPONSE
            elif data.startswith(InstrumentCommand.DUMP_SETTINGS_02):
                my_response = DUMP_02_COMMAND_RESPONSE
            else:
                my_response = None
            if my_response is not None:
                log.debug("my_send: data: %s, my_response: %s", data, my_response)
                driver._protocol._promptbuf += my_response
                return len(my_response)

        return inner

    def test_driver_enums(self):
        """
        Verify that all driver enumeration has no duplicate values that might cause confusion.  Also
        do a little extra validation for the Capabilities
        """
        self.assert_enum_has_no_duplicates(DataParticleType())
        self.assert_enum_has_no_duplicates(ProtocolState())
        self.assert_enum_has_no_duplicates(ProtocolEvent())
        self.assert_enum_has_no_duplicates(DriverParameter())
        self.assert_enum_has_no_duplicates(InstrumentCommand())

        # Test capabilities for duplicates, them verify that capabilities is a subset of proto events
        self.assert_enum_has_no_duplicates(Capability())
        self.assert_enum_complete(Capability(), ProtocolEvent())

    def test_chunker(self):
        """
        Test the chunker and verify the particles created.
        """
        chunker = StringChunker(Protocol.sieve_function)

        self.assert_chunker_sample(chunker, VALID_SAMPLE_01)
        self.assert_chunker_sample(chunker, DUMP_01_STATUS)
        self.assert_chunker_sample(chunker, DUMP_02_STATUS)

    def test_connect(self, initial_protocol_state=ProtocolState.COMMAND):
        """
        Verify driver transitions correctly and connects
        """
        driver = InstrumentDriver(self._got_data_event_callback)
        self.assert_initialize_driver(driver, initial_protocol_state)
        return driver

    def test_data_build_parsed_values(self):
        """
        Verify that the BOTPT IRIS driver build_parsed_values method
        raises SampleException when an invalid sample is encountered
        and that it returns a result when a valid sample is encountered
        """
        items = [
            (INVALID_SAMPLE, False),
            (VALID_SAMPLE_01, True),
            (VALID_SAMPLE_02, True),
        ]

        for raw_data, is_valid in items:
            sample_exception = False
            result = None
            try:
                result = IRISDataParticle(raw_data)._build_parsed_values()
            except SampleException as e:
                log.debug('SampleException caught: %s.', e)
                sample_exception = True
            if is_valid:
                self.assertFalse(sample_exception)
                self.assertIsInstance(result, list)

    def test_got_data(self):
        """
        Verify sample data passed through the got data method produces the correct data particles
        """
        driver = self.test_connect()

        self.assert_particle_published(driver, VALID_SAMPLE_01, self.assert_particle_sample_01, True)
        self.assert_particle_published(driver, VALID_SAMPLE_02, self.assert_particle_sample_02, True)
        self.assert_particle_published(driver, DUMP_01_STATUS, self.assert_particle_status_01, True)
        self.assert_particle_published(driver, DUMP_02_STATUS, self.assert_particle_status_02, True)
        self.assert_particle_published(driver, BOTPT_FIREHOSE_01, self.assert_particle_sample_01, True)

    def test_command_responses(self):
        """
        Verify that the driver correctly handles the various responses
        """
        driver = self.test_connect()

        items = [
            (DATA_ON_COMMAND_RESPONSE, IRIS_DATA_ON),
            (DATA_OFF_COMMAND_RESPONSE, IRIS_DATA_OFF),
            (DUMP_01_COMMAND_RESPONSE, IRIS_DUMP_01),
            (DUMP_02_COMMAND_RESPONSE, IRIS_DUMP_02),
        ]

        for response, expected_prompt in items:
            log.debug('test_command_response: response: %r expected_prompt: %r', response, expected_prompt)
            self._send_port_agent_packet(driver, response)
            self.assertTrue(driver._protocol._get_response(expected_prompt=expected_prompt))

    def test_handlers(self):
        items = [
            ('_handler_command_start_autosample', ProtocolState.COMMAND, ProtocolState.AUTOSAMPLE, IRIS_DATA_ON),
            ('_handler_autosample_stop_autosample', ProtocolState.AUTOSAMPLE, ProtocolState.COMMAND, IRIS_DATA_OFF),
            ('_handler_command_autosample_acquire_status', ProtocolState.COMMAND, None, IRIS_DUMP_02),
        ]

        for handler, initial_state, expected_state, prompt in items:
            driver = self.test_connect(initial_protocol_state=initial_state)
            driver._connection.send.side_effect = self.my_send(driver)
            result = getattr(driver._protocol, handler)()
            log.debug('handler: %r - result: %r expected: %r', handler, result, prompt)
            next_state = result[0]
            return_value = result[1][1]
            self.assertEqual(next_state, expected_state)
            self.assertTrue(return_value.endswith(prompt))

    def test_direct_access(self):
        driver = self.test_connect()
        driver._protocol._handler_direct_access_execute_direct(InstrumentCommand.DATA_ON)
        driver._protocol._handler_direct_access_execute_direct('LILY,BAD_COMMAND_HERE')
        self.assertEqual(driver._protocol._sent_cmds, [InstrumentCommand.DATA_ON])

    def test_protocol_filter_capabilities(self):
        """
        This tests driver filter_capabilities.
        Iterate through available capabilities, and verify that they can pass successfully through the filter.
        Test silly made up capabilities to verify they are blocked by filter.
        """
        mock_callback = Mock()
        protocol = Protocol(BaseEnum, NEWLINE, mock_callback)
        driver_capabilities = Capability().list()
        test_capabilities = Capability().list()

        # Add a bogus capability that will be filtered out.
        test_capabilities.append("BOGUS_CAPABILITY")

        # Verify "BOGUS_CAPABILITY was filtered out
        self.assertEquals(sorted(driver_capabilities), sorted(protocol._filter_capabilities(test_capabilities)))


###############################################################################
#                            INTEGRATION TESTS                                #
#     Integration test test the direct driver / instrument interaction        #
#     but making direct calls via zeromq.                                     #
#     - Common Integration tests test the driver through the instrument agent #
#     and common for all drivers (minimum requirement for ION ingestion)      #
###############################################################################
@attr('INT', group='mi')
class DriverIntegrationTest(InstrumentDriverIntegrationTestCase, IRISTestMixinSub):
    def setUp(self):
        InstrumentDriverIntegrationTestCase.setUp(self)

    def test_connect(self):
        self.assert_initialize_driver()

    def test_data_on(self):
        """
        @brief Test for turning data on
        """
        self.assert_initialize_driver()
        self.assert_particle_generation(ProtocolEvent.START_AUTOSAMPLE,
                                        DataParticleType.IRIS_PARSED,
                                        self.assert_particle_sample_01,
                                        delay=2)
        self.assert_async_particle_generation(DataParticleType.IRIS_PARSED,
                                              self.assert_particle_sample_01,
                                              particle_count=10,
                                              timeout=12)
        response = self.driver_client.cmd_dvr('execute_resource', ProtocolEvent.STOP_AUTOSAMPLE)
        self.assertTrue(response[1].endswith(IRIS_DATA_OFF))

    def test_acquire_status(self):
        """
        @brief Test for acquiring status dump 01
        """
        self.assert_initialize_driver()

        # Issue acquire status command
        self.driver_client.cmd_dvr('execute_resource', ProtocolEvent.ACQUIRE_STATUS)
        self.assert_async_particle_generation(DataParticleType.IRIS_STATUS1, self.assert_particle_status_01)
        self.assert_async_particle_generation(DataParticleType.IRIS_STATUS2, self.assert_particle_status_02)

    def test_direct_access(self):
        """
        Verify we can enter the direct access state
        """
        self.assert_initialize_driver(ProtocolState.COMMAND)
        self.assert_state_change(ProtocolState.COMMAND, 5)
        self.driver_client.cmd_dvr('execute_resource', ProtocolEvent.START_DIRECT)
        self.assert_state_change(ProtocolState.DIRECT_ACCESS, 5)

    def test_commands(self):
        self.assert_initialize_driver()
        self.assert_driver_command(Capability.START_AUTOSAMPLE)
        self.assert_driver_command(Capability.STOP_AUTOSAMPLE)
        self.assert_driver_command(Capability.ACQUIRE_STATUS)


###############################################################################
#                            QUALIFICATION TESTS                              #
# Device specific qualification tests are for doing final testing of ion      #
# integration.  The generally aren't used for instrument debugging and should #
# be tackled after all unit and integration tests are complete                #
###############################################################################
@attr('QUAL', group='mi')
class DriverQualificationTest(InstrumentDriverQualificationTestCase, IRISTestMixinSub):
    def setUp(self):
        InstrumentDriverQualificationTestCase.setUp(self)

    def test_direct_access_telnet_mode(self):
        """
        @brief This test manually tests that the Instrument Driver properly supports
        direct access to the physical instrument. (telnet mode)
        """
        self.assert_direct_access_start_telnet()
        self.assertTrue(self.tcp_client)
        self.tcp_client.send_data(InstrumentCommand.DATA_ON + NEWLINE)
        result = self.tcp_client.expect(IRIS_DATA_ON)
        self.assertTrue(result, msg='Failed to receive expected response in direct access mode.')
        self.assert_direct_access_stop_telnet()
        self.assert_state_change(ResourceAgentState.COMMAND, ProtocolState.COMMAND, 10)

    def test_sample_particles(self):
        self.assert_sample_autosample(self.assert_particle_sample_01, DataParticleType.IRIS_PARSED)

    def test_status_particles(self):
        self.assert_enter_command_mode()
        self.assert_particle_polled(Capability.ACQUIRE_STATUS, self.assert_particle_status_01,
                                    DataParticleType.IRIS_STATUS1)
        self.assert_particle_polled(Capability.ACQUIRE_STATUS, self.assert_particle_status_02,
                                    DataParticleType.IRIS_STATUS2)

    def test_get_capabilities(self):
        """
        @brief Verify that the correct capabilities are returned from get_capabilities
        at various driver/agent states.
        """
        self.assert_enter_command_mode()

        ##################
        #  Command Mode
        ##################
        capabilities = {
            AgentCapabilityType.AGENT_COMMAND: self._common_agent_commands(ResourceAgentState.COMMAND),
            AgentCapabilityType.AGENT_PARAMETER: self._common_agent_parameters(),
            AgentCapabilityType.RESOURCE_COMMAND: [
                ProtocolEvent.GET,
                ProtocolEvent.SET,
                ProtocolEvent.START_AUTOSAMPLE,
                ProtocolEvent.ACQUIRE_STATUS,
            ],
            AgentCapabilityType.RESOURCE_INTERFACE: None,
            AgentCapabilityType.RESOURCE_PARAMETER: self._driver_parameters.keys()
        }

        self.assert_capabilities(capabilities)

        ##################
        #  Streaming Mode
        ##################

        capabilities[AgentCapabilityType.AGENT_COMMAND] = self._common_agent_commands(ResourceAgentState.STREAMING)
        capabilities[AgentCapabilityType.RESOURCE_COMMAND] = [
            ProtocolEvent.STOP_AUTOSAMPLE,
            ProtocolEvent.ACQUIRE_STATUS,
        ]

        self.assert_start_autosample()
        self.assert_capabilities(capabilities)
        self.assert_stop_autosample()

    def test_direct_access_exit_from_autosample(self):
        """
        Verify that direct access mode can be exited while the instrument is
        sampling. This should be done for all instrument states. Override
        this function on a per-instrument basis.
        """
        self.assert_enter_command_mode()

        # go into direct access, and start sampling so ION doesnt know about it
        self.assert_direct_access_start_telnet(timeout=600)
        self.assertTrue(self.tcp_client)
        self.tcp_client.send_data(InstrumentCommand.DATA_ON + NEWLINE)

        self.assert_direct_access_stop_telnet()