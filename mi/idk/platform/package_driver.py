"""
@file coi-services/mi.idk.platform/package_driver.py
@author Emily Hahn
@brief Main script class for running the package_driver process
"""
import os
import sys
import subprocess

from mi.core.log import get_logger ; log = get_logger()

import mi.idk.package_driver
from mi.idk.exceptions import InvalidParameters
from mi.idk import prompt
from mi.idk.platform.metadata import Metadata
from mi.idk.platform.nose_test import NoseTest
from mi.idk.platform.driver_generator import DriverGenerator
from mi.idk.platform.egg_generator import EggGenerator

REPODIR = '/tmp/repoclone'

class PackageDriver(mi.idk.package_driver.PackageDriver):
    def _driver_prefix(self):
        return "platform"

    def archive_file(self):
        return "%s-%s-driver.zip" % (self.metadata.driver_name,
                                     self.metadata.version)

    def build_name(self):
        return "platform_%s" % self.metadata.driver_name

    def get_metadata(self):
        # get which dataset agent is selected from the current metadata, use
        # this to get metadata from the cloned repo
        tmp_metadata = Metadata()

        # read metadata from the cloned repo
        self.metadata = Metadata(tmp_metadata.driver_name,
                                 REPODIR + '/marine-integrations')

        return self.metadata


    def get_nose_test(self):
        return NoseTest(self.metadata, log_file=self.log_path())

    def get_driver_generator(self):
        return DriverGenerator(self.metadata)

    def get_egg_generator(self):
        return EggGenerator(self.metadata)
