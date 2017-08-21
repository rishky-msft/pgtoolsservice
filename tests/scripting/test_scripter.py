# --------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for license information.
# --------------------------------------------------------------------------------------------

"""Tests the scripter module"""
import unittest
from unittest import mock
from typing import List, Dict  # noqa

from pgsmo import Database, Server, Table, DataType, Schema
from pgsqltoolsservice.connection import ConnectionService
from pgsqltoolsservice.connection.contracts.common import ConnectionType
from pgsqltoolsservice.utils import constants
import tests.utils as utils
from pgsqltoolsservice.metadata.contracts.object_metadata import ObjectMetadata
from pgsqltoolsservice.scripting.scripter import Scripter
from pgsqltoolsservice.scripting.contracts.scriptas_request import (
    ScriptOperation, ScriptAsParameters
)

# OBJECT IMPORTS
from pgsmo.objects.table.table import Table


class TestScripter(unittest.TestCase):
    """Methods for testing the scripter module"""

    def setUp(self):
        """Set up mock objects for testing the scripting service.
        Ran before each unit test.
        """
        self.cursor = utils.MockCursor(None)
        self.connection = utils.MockConnection({"port": "8080", "host": "test", "dbname": "test"}, cursor=self.cursor)
        self.scripter = Scripter(self.connection)
        self.server = self.scripter.server
        self.schema = Schema(self.server, self.server.maintenance_db, 'myschema')
        self.scripter.server.maintenance_db._schemas = [self.schema]

    def test_table_create_script(self):
        """ Tests create script for tables"""
        # Set up the mocks
        mock_table = Table(None, None, 'test')

        def table_mock_fn(connection):
            mock_table._template_root = mock.MagicMock(return_value=Table.TEMPLATE_ROOT)
            mock_table._create_query_data = mock.MagicMock(return_value={"data": {"name": "test"}})
            result = mock_table.create_script(connection)
            return result

        self.schema.tables = [mock_table]

        # If I try to get select script for any object
        result = self.scripter.get_create_script(ObjectMetadata.from_data(0, 'Table', 'test', 'myschema'))

        # The result shouldn't be none or an empty string
        self.assertNotNoneOrEmpty(result)


    def assertNotNoneOrEmpty(self, result: str) -> bool:
        """Assertion to confirm a string to be not none or empty"""
        self.assertIsNotNone(result) and self.assertTrue(len(result))
