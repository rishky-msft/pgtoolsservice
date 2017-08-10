# --------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for license information.
# --------------------------------------------------------------------------------------------

from pgsmo import Schema, Server
from pgsqltoolsservice.metadata.contracts.object_metadata import ObjectMetadata     # noqa


class Scripter(object):
    """Service for retrieving operation scripts"""

    def __init__(self, conn):
        # get server from psycopg2 connection
        self.server: Server = Server(conn)

    # SCRIPTING METHODS ############################

    # SELECT ##################################################################
    @staticmethod
    def script_as_select(metadata: ObjectMetadata) -> str:
        """ Function to get script for select operations """
        schema = metadata["schema"]
        name = metadata["name"]
        # wrap quotes only around objects with all small letters
        name = f'"{name}"' if name.islower() else name
        script = f"SELECT *\nFROM {schema}.{name}\nLIMIT 1000\n"
        return script

    # CREATE ##################################################################

    def get_database_create_script(self, metadata: ObjectMetadata) -> str:
        """ Get create script for databases """
        return self.server.databases[metadata.name].create_script()

    def get_view_create_script(self, metadata: ObjectMetadata) -> str:
        """ Get create script for views """
        return self._find_schema(metadata).views[metadata.name].create_script()

    def get_table_create_script(self, metadata: ObjectMetadata) -> str:
        """ Get create script for tables """
        return self._find_schema(metadata).tables[metadata.name].create_script()

    def get_schema_create_script(self, metadata: ObjectMetadata) -> str:
        """ Get create script for schema """
        return self._find_schema(metadata).create_script()

    def get_role_create_script(self, metadata: ObjectMetadata) -> str:
        """ Get create script for role """
        return self.server.roles[metadata.name].create_script()

    # DELETE ##################################################################
    def get_table_delete_script(self, metadata: ObjectMetadata) -> str:
        """ Get delete script for table """
        return self._find_schema(metadata).tables[metadata.name].delete_script()

    def get_view_delete_script(self, metadata: ObjectMetadata) -> str:
        """ Get delete script for view """
        return self._find_schema(metadata).views[metadata.name].delete_script()

    def get_database_delete_script(self, metadata: ObjectMetadata) -> str:
        """ Get delete script for databases """
        return self.server.databases[metadata.name].delete_script()

    def get_schema_delete_script(self, metadata: ObjectMetadata) -> str:
        """ Get delete script for schemas """
        return self._find_schema(metadata).delete_script()

    # UPDATE ##################################################################

    def get_table_update_script(self, metadata: ObjectMetadata) -> str:
        """ Get update script for tables """
        return self._find_schema(metadata).tables[metadata.name]

    def get_view_update_script(self, metadata: ObjectMetadata) -> str:
        """ Get update date script for view """
        return self._find_schema(metadata).views[metadata.name]

    def get_schema_update_script(self, metadata: ObjectMetadata) -> str:
        """ Get update script for schemas """
        return self._find_schema(metadata).update_script()

    def get_role_update_script(self, metadata: ObjectMetadata) -> str:
        """ Get update script for roles """
        return self.server.roles[metadata.name].update_script()

    # HELPER METHODS ##########################################################

    def _find_schema(self, metadata: ObjectMetadata) -> Schema:
        """ Find the schema in the server to script as """
        return self.server.maintenance_db.schemas[metadata.schema]
