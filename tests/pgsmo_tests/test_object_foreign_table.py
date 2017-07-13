# --------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for license information.
# --------------------------------------------------------------------------------------------

import unittest

from pgsmo.objects.foreign_table import ForeignTable
from pgsmo.utils.querying import ServerConnection
import tests.pgsmo_tests.utils as utils

NODE_ROW = {
    'name': 'funcname(arg1 int)',
    'oid': 123,
    'description': 'func description',
    'lanname': 'sql',
    'funcowner': 'postgres'
}


class TestForeignTable(unittest.TestCase):
    # CONSTRUCTION TESTS ###################################################
    def test_init(self):
        props = [
            '_description', 'description',
            '_lanname', 'language',
            '_owner', 'owner'
        ]
        colls = []
        utils.init_base(ForeignTable, props, colls)

    def test_from_node_query(self):
        utils.from_node_query_base(ForeignTable, NODE_ROW, self._validate)

    def test_from_nodes_for_parent(self):
        utils.get_nodes_for_parent_base(
            ForeignTable,
            NODE_ROW,
            lambda conn: ForeignTable.get_nodes_for_parent(conn, 0),
            self._validate
        )

    # IMPLEMENTATION DETAILS ###############################################
    def _validate(self, obj: ForeignTable, mock_conn: ServerConnection):
        utils.validate_node_object_props(obj, mock_conn, NODE_ROW['name'], NODE_ROW['oid'])

        # Function basic properties
        self.assertEqual(obj._description, NODE_ROW['description'])
        self.assertEqual(obj.description, NODE_ROW['description'])
        self.assertEqual(obj._lanname, NODE_ROW['lanname'])
        self.assertEqual(obj.language, NODE_ROW['lanname'])
        self.assertEqual(obj._owner, NODE_ROW['funcowner'])
        self.assertEqual(obj.owner, NODE_ROW['funcowner'])