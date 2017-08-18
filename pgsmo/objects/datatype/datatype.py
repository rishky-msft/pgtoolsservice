# --------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for license information.
# --------------------------------------------------------------------------------------------

from typing import Optional, List, Any

from pgsmo.objects.node_object import NodeObject, NodeLazyPropertyCollection
from pgsmo.objects.server import server as s        # noqa
import pgsmo.utils.templating as templating
import pgsmo.utils.querying as querying


class DataType(NodeObject):
    """Represents a data type"""
    TEMPLATE_ROOT = templating.get_template_root(__file__, 'templates')

    @classmethod
    def _from_node_query(cls, server: 's.Server', parent: None, **kwargs) -> 'Type':
        """
        Creates a Type object from the result of a DataType node query
        :param server: Server that owns the DataType
        :param parent: Parent object of the DataType
        :param kwargs: Row from a DataType node query
        Kwargs:
            name str: Name of the DataType
            oid int: Object ID of the DataType
            rolcanlogin bool: Whether or not the DataType can login
            rolsuper bool: Whether or not the DataType is a super user
        :return: A DataType7 instance
        """
        datatype = cls(server, kwargs['name'])

        # Define values from node query
        datatype._oid = kwargs['oid']
        return datatype

    def __init__(self, server: 's.Server', name: str):
        """
        Initializes internal state of a DataType object
        :param server: Server that owns the role
        :param name: Name of the role
        """
        super(DataType, self).__init__(server, None, name)
        self._additional_properties: NodeLazyPropertyCollection = self._register_property_collection(self._additional_property_generator)


    # PROPERTIES ###########################################################
    @property
    def is_collatable(self) -> Optional[bool]:
        """Whether or not the DataType is collatable"""
        return self._full_properties.get("is_collatable", "")

    # TODO acl seems to be handled by separate query so skip for now
    # @property
    # def typeacl(self):
    #     return self._full_properties.get("type_acl", "")

    @property
    def alias(self):
        return self._full_properties.get("alias", "")

    @property
    def typeowner(self):
        return self._full_properties.get("typeowner", "")

    @property
    def element(self):
        return self._full_properties.get("element", "")

    @property
    def description(self):
        return self._full_properties.get("description", "")

    @property
    def is_sys_type(self):
        return self._full_properties.get("is_sys_type", "")

    @property
    def seclabels(self):
        return self._full_properties.get("seclabels", "")

    @property
    def typtype(self) -> str:
        return self._full_properties.get("typtype", "")

    @property
    def schema(self):
        return self._full_properties.get("schema", "")

    @property
    def typname(self):
        return self._additional_properties.get("typname", "")

    @property
    def collname(self):
        return self._additional_properties.get("collname", "")

    @property
    def opcname(self):
        return self._additional_properties.get("opcname", "")

    @property
    def rngsubdiff(self):
        return self._additional_properties.get("rngsubdiff", "")

    @property
    def rngcanonical(self):
        return self._additional_properties.get("rngcanonical", "")

    @property
    def composite(self) -> List[Any]:
        if not self.typtype == 'c':
            return None
        composite = []
        # TODO support composite, which is a complex property
        return composite
        
    # IMPLEMENTATION DETAILS ###############################################
    @classmethod
    def _template_root(cls, server: 's.Server') -> str:
        return cls.TEMPLATE_ROOT

    # SCRIPTING METHODS ####################################################

    def create_script(self, connection: querying.ServerConnection) -> str:
        """ Function to retrieve create scripts for a DataType """
        data = self._create_query_data()
        query_file = "create.sql"
        return self._get_template(connection, query_file, data)

    def update_script(self, connection: querying.ServerConnection) -> str:
        """ Function to retrieve update scripts for a DataType """
        data = self._update_query_data()
        query_file = "update.sql"
        filters = {'hasAny': templating.has_any}
        return self._get_template(connection, query_file, data, filters_to_add=filters)

    # HELPER METHODS ##################################################################

    def _create_query_data(self):
        """ Gives the data object for create query """
        # TODO support composite data type properties
        # TODO support enum value
        return {"data": {
            "name": self.name,
            "schema": self.schema,
            "typtype": self.typtype,
            "collname": self.collname,
            "opcname": self.opcname,
            "rngcanonical": self.rngcanonical,
            "rngsubdiff": self.rngsubdiff,
            "description": self.description,
            "composite": self.composite
        }}

    def _update_query_data(self):
        """ Gives the data object for update query """
        return {
            "data": {
                "rolname": self.name,
                "typeowner": self.typeowner,
                "description": self.description,
                "schema": self.schema,
                "rolcreaterole": self.createrole,
                "rolinherit": self.inherit,
                "rolreplication": self.replication,
                "rolconnlimit": self.connlimit,
                "rolvaliduntil": self.validuntil,
                "rolpassword": self.password,
                "rolcatupdate": self.catupdate,
                "revoked_admins": self.revoked_admins,
                "revoked": self.revoked,
                "admins": self.admins,
                "members": self.members,
                "variables": self.variables,
                "description": self.description
            }, "rolCanLogin": self.can_login
        }
