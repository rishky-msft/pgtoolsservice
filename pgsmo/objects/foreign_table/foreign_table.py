# --------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for license information.
# --------------------------------------------------------------------------------------------

from typing import List, Optional

from pgsmo.objects.node_object import NodeObject, get_nodes
import pgsmo.utils.querying as querying
import pgsmo.utils.templating as templating


TEMPLATE_ROOT = templating.get_template_root(__file__, 'templates')


class ForeignTable(NodeObject):
    @classmethod
    def get_nodes_for_parent(cls, conn: querying.ServerConnection, schema_id: int) -> List['ForeignTable']:
        """
        Generates a list of foreign tables for a given schema.
        :param conn: Connection to use to lookup the foreign tables for a schema
        :param schema_id: Object ID of the schema to retrieve foreign tables for
        :return: List of ForeignTable objects
        """
        return get_nodes(conn, TEMPLATE_ROOT, cls._from_node_query, scid=schema_id)

    @classmethod
    def _from_node_query(cls, conn: querying.ServerConnection, **kwargs) -> 'ForeignTable':
        """
        Creates a ForeignTable object from the result of a foreign table node query
        :param conn: Connection that executed the node query
        :param kwargs: Row from a foreign table node query
        Kwargs:
            oid int: Object ID of the foreign table
            name str: Name of the foreign table
            basensp str: Base namespace for the foreign table
            description str: Description of the foreign table
            options str: Options for the foreign table
            owner str: Name of the user that owns the foreign table
        :return: A ForeignTable instance
        """
        ft = cls(conn, kwargs['name'])
        ft._oid = kwargs['oid']

        # ForeignTable basic properties
        ft._base_namespace = kwargs['basensp']
        ft._description = kwargs['description']
        ft._options = kwargs['options']
        ft._owner = kwargs['owner']

        return ft

    def __init__(self, conn: querying.ServerConnection, name: str):
        super(ForeignTable, self).__init__(conn, name)

        # Declare basic properties
        self._base_namespace: Optional[str] = None      # TODO: Is this the same as the schema?
        self._description: Optional[str] = None
        self._options: Optional[str] = None             # TODO: How is this returned?
        self._owner: Optional[str] = None

    # PROPERTIES ###########################################################
    @property
    def base_namespace(self) -> Optional[str]:
        return self._base_namespace

    @property
    def description(self) -> Optional[str]:
        return self._description

    @property
    def options(self) -> Optional[str]:
        return self._options

    @property
    def owner(self) -> Optional[str]:
        return self._owner
