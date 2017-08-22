# --------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for license information.
# --------------------------------------------------------------------------------------------

"""A blueprint module implementing the sql auto complete feature."""

import enum
import itertools
import operator
import re
from typing import List, Any, Dict
from collections import namedtuple
try:
    from collections import Counter
except ImportError:
    # python 2.6
    from .counter import Counter

import sqlparse
from sqlparse.sql import Comparison, Identifier, Where

import psycopg2
import psycopg2.extensions

from pgsmo import Server, Database, Schema
import pgsmo.utils.querying as querying
from pgsqltoolsservice.connection.contracts import ConnectionType
from pgsqltoolsservice.connection import ConnectionInfo
from pgsqltoolsservice.language.contracts.completion import CompletionItem, CompletionItemKind, TextEdit
from pgsqltoolsservice.language.keywords import DefaultCompletionHelper
from pgsqltoolsservice.language.sqlautocomplete.function_metadata import FunctionMetadata
from pgsqltoolsservice.language.sqlautocomplete.parseutils import (
    last_word, extract_tables, find_prev_keyword, parse_partial_identifier)
from pgsqltoolsservice.language.sqlautocomplete.prioritization import PrevalenceCounter
from pgsqltoolsservice.workspace import ScriptFile
from pgsqltoolsservice.workspace.contracts.common import Range


Table = namedtuple('Table', ['schema'])

Function = namedtuple('Function', ['schema', 'filter'])
# For convenience, don't require the `filter` argument in Function constructor
Function.__new__.__defaults__ = (None, None)

Column = namedtuple('Column', ['tables', 'drop_unique'])
Column.__new__.__defaults__ = (None, None)

View = namedtuple('View', ['schema'])
Keyword = namedtuple('Keyword', [])
Datatype = namedtuple('Datatype', ['schema'])
Alias = namedtuple('Alias', ['aliases'])
Match = namedtuple('Match', ['completion', 'priority'])

# Regex for finding "words" in documents.
_FIND_WORD_RE = re.compile(r'([a-zA-Z0-9_]+|[^a-zA-Z0-9_\s]+)')
_FIND_BIG_WORD_RE = re.compile(r'([^\s]+)')
_NAME_PATTERN = re.compile(r'^[_a-z][_a-z0-9\$]*$')


class MatchMode(enum.Enum):
    """ Class that defines the ways to match"""
    Strict = "Strict"
    Fuzzy = "Fuzzy"


class Matcher:
    """
    Handles matching suggestions, with handling of escaping, keyword lookup and more
    """
    def __init__(self, server_conn: querying.ServerConnection):
        self.server_conn = server_conn
        self.keywords = []
        self.reserved_words = set()
        self.init_keywords()
        self.prioritizer = PrevalenceCounter(self.keywords)

    def init_keywords(self) -> None:
        """
        Queries keywords from the server and uses to init known keyword list
        and the reserved words. Reserved words are used to ensure these are escaped
        if they are also used as object names
        """
        sql = "SELECT upper(word) as word FROM pg_get_keywords()"
        cols, rows = self.server_conn.execute_dict(sql)
        if cols:
            for record in rows:
                self.keywords.append(record['word'])
            for keyword in self.keywords:
                self.reserved_words.update(keyword.split())

    def escape_name(self, name: str) -> str:
        """Escapes names if they are known keywords or contain special characters"""
        if name and ((not _NAME_PATTERN.match(name)) or
                     (name.upper() in self.reserved_words)):
            name = '"%s"' % name
        return name

    def unescape_name(self, name: str) -> str:
        """Unescapes names if they start with an escape character"""
        if name and name[0] == '"' and name[-1] == '"':
            name = name[1:-1]

        return name

    def escaped_names(self, names: List[str]) -> List[str]:
        """Escapes a list of names"""
        return [self.escape_name(name) for name in names]

    def find_matches(self, script_file: ScriptFile, text_range: Range, collection: List[Any], mode=MatchMode.Fuzzy,
                     meta=None) -> List[Match]:
        """
        Find completion matches for the given text.

        Given the user's input text and a collection of available
        completions, find completions matching the last word of the
        text.

        `mode` can be either 'fuzzy', or 'strict'
            'fuzzy': fuzzy matching, ties broken by name prevalance
            `keyword`: start only matching, ties broken by keyword prevalance

        yields prompt_toolkit Completion instances for any matches found
        in the collection of available completions.

        Args:
            text:
            collection:
            mode:
            meta:
        """
        text = script_file.get_text_in_range(text_range).lower()

        if text and text[0] == '"':
            # text starts with double quote; user is manually escaping a name
            # Match on everything that follows the double-quote. Note that
            # text_len is calculated before removing the quote, so the
            # Completion.position value is correct
            text = text[1:]

        if mode == MatchMode.Fuzzy:
            fuzzy = True
            priority_func = self.prioritizer.name_count
        else:
            fuzzy = False
            priority_func = self.prioritizer.keyword_count

        # Construct a `_match` function for either fuzzy or non-fuzzy matching
        # The match function returns a 2-tuple used for sorting the matches,
        # or None if the item doesn't match
        # Note: higher priority values mean more important, so use negative
        # signs to flip the direction of the tuple
        if fuzzy:
            regex = '.*?'.join(map(re.escape, text))
            pat = re.compile('(%s)' % regex)

            def _match(item):
                result = pat.search(self.unescape_name(item.lower()))
                if result:
                    return -len(result.group()), -result.start()
        else:
            match_end_limit = len(text)

            def _match(item):
                match_point = item.lower().find(text, 0, match_end_limit)
                if match_point >= 0:
                    # Use negative infinity to force keywords to sort after all
                    # fuzzy matches
                    return -float('Infinity'), -match_point

            # All completions have an identical meta
            collection = zip(collection, itertools.repeat(meta))

        matches = []

        for item, meta in collection:
            sort_key = _match(item)
            if sort_key:
                if meta and len(meta) > 50:
                    # Truncate meta-text to 50 characters, if necessary
                    meta = meta[:47] + u'...'

                # Lexical order of items in the collection, used for
                # tiebreaking items with the same match group length and start
                # position. Since we use *higher* priority to mean "more
                # important," we use -ord(c) to prioritize "aa" > "ab" and end
                # with 1 to prioritize shorter strings (ie "user" > "users").
                # We also use the unescape_name to make sure quoted names have
                # the same priority as unquoted names.
                lexical_priority = tuple(-ord(c) for c in self.unescape_name(item)) + (1,)

                priority = sort_key, priority_func(item), lexical_priority

                matches.append(Match(
                    completion=self._to_completion_item(item, meta, text_range),
                    priority=priority))

        return matches

    def _to_completion_item(self, item: str, detail: str, text_range: Range) -> CompletionItem:
        """Creates a competion item from text"""
        completion = CompletionItem()
        completion.label = item
        completion.insert_text = item
        completion.detail = detail
        completion.text_edit = TextEdit.from_data(text_range, item)
        # TODO completion.kind = ?
        return completion


class SuggestionMatcher():
    """
    Handles matching based on a suggestion
    """
    def __init__(self, matcher: Matcher, server: Server):
        self.matcher = matcher
        self.server = server
        self.schemas: Dict[str, 'Schema'] = {}
        self.suggestion_matchers = {
            Column: self.get_column_matches,
            Function: self.get_function_matches,
            Schema: self.get_schema_matches,
            Table: self.get_table_matches,
            View: self.get_view_matches,
            Alias: self.get_alias_matches,
            Database: self.get_database_matches,
            Keyword: self.get_keyword_matches,
            Datatype: self.get_datatype_matches,
        }
        self._load_schemas()

    def _load_schemas(self):
        database: Database = self.server.maintenance_db
        for schema in database.schemas:
            self.schemas[schema.name] = schema

    def _schema_names(self):
        return list(self.schemas.keys())

    def _get_schema(self, schema_name: str) -> Schema:
        """Attempts to find a schema given its name, or returns None if not found"""
        if schema_name:
            schema = next(s for s in self.schemas if s.name == schema_name)
            if schema:
                return schema
        return None

    def get_suggestion_matches(self, suggestion: namedtuple, word_before_cursor: str) -> List[Any]:
        """Gets matching object definitions based on a """
        suggestion_type = type(suggestion)
        matcher = self.suggestion_matchers[suggestion_type]
        return matcher(self, suggestion, word_before_cursor)

    def get_column_matches(self, suggestion, word_before_cursor):
        tables = suggestion.tables
        scoped_cols = self.populate_scoped_cols(tables)

        if suggestion.drop_unique:
            # drop_unique is used for 'tb11 JOIN tbl2 USING (...' which should
            # suggest only columns that appear in more than one table
            scoped_cols = [col for (col, count)
                           in Counter(scoped_cols).items()
                           if count > 1 and col != '*']

        return self.matcher.find_matches(word_before_cursor, scoped_cols, mode='strict', meta='column')

    def get_function_matches(self, suggestion, word_before_cursor):
        if suggestion.filter == 'is_set_returning':
            # Only suggest set-returning functions
            funcs = self.populate_functions(suggestion.schema)
        else:
            funcs = self.populate_schema_objects(suggestion.schema, 'functions')

        # Function overloading means we way have multiple functions of the same
        # name at this point, so keep unique names only
        funcs = set(funcs)

        funcs = self.matcher.find_matches(word_before_cursor, funcs, mode='strict', meta='function')

        return funcs

    def get_schema_matches(self, _, word_before_cursor):
        schema_names = self._schema_names

        # Unless we're sure the user really wants them, hide schema names
        # starting with pg_, which are mostly temporary schemas
        if not word_before_cursor.startswith('pg_'):
            schema_names = [s for s in schema_names if not s.startswith('pg_')]

        return self.matcher.find_matches(word_before_cursor, schema_names, mode='strict', meta='schema')

    def get_table_matches(self, suggestion, word_before_cursor):
        tables = self.populate_schema_objects(suggestion.schema, 'tables')

        # Unless we're sure the user really wants them, don't suggest the
        # pg_catalog tables that are implicitly on the search path
        if not suggestion.schema and (
                not word_before_cursor.startswith('pg_')):
            tables = [t for t in tables if not t.startswith('pg_')]

        return self.matcher.find_matches(word_before_cursor, tables, mode='strict', meta='table')

    def get_view_matches(self, suggestion, word_before_cursor):
        views = self.populate_schema_objects(suggestion.schema, 'views')

        if not suggestion.schema and (
                not word_before_cursor.startswith('pg_')):
            views = [v for v in views if not v.startswith('pg_')]

        return self.matcher.find_matches(word_before_cursor, views, mode='strict', meta='view')

    def get_alias_matches(self, suggestion, word_before_cursor):
        aliases = suggestion.aliases
        return self.matcher.find_matches(word_before_cursor, aliases, mode='strict', meta='table alias')

    def get_database_matches(self, _, word_before_cursor):
        databases = []
        database: Database
        for database in self.server.databases:
            databases.append(database.name)

        return self.matcher.find_matches(word_before_cursor, databases, mode='strict', meta='database')

    def get_keyword_matches(self, _, word_before_cursor):
        return self.find_matches(word_before_cursor, self.keywords,
                                 mode='strict', meta='keyword')

    def get_datatype_matches(self, suggestion, word_before_cursor):
        # suggest custom datatypes
        types = self.populate_schema_objects(suggestion.schema, 'datatypes')
        matches = self.matcher.find_matches(word_before_cursor, types, mode='strict', meta='datatype')

        return matches

    def populate_schema_objects(self, schema_name: str, obj_type):
        """
        Returns list of tables or functions for a (optional) schema

        Args:
            schema:
            obj_type:
        """
        objects = []
        # Get the schemas to iterate over
        schema = self._get_schema(schema_name)
        schemas: List[Schema] = [schema] if schema else self.schemas     

        for schema in schemas:
            object_matches = getattr(schema, obj_type)
        
        if (schema):
            in_clause = ''
            query = ''
            objects = []

        if schema:
            in_clause = '\'' + schema + '\''
        else:
            for r in self.search_path:
                in_clause += '\'' + r + '\','

            # Remove extra comma
            if len(in_clause) > 0:
                in_clause = in_clause[:-1]

        if obj_type == 'tables':
            query = render_template("/".join([self.sql_path, 'tableview.sql']),
                                    schema_names=in_clause,
                                    object_name='tables')
        elif obj_type == 'views':
            query = render_template("/".join([self.sql_path, 'tableview.sql']),
                                    schema_names=in_clause,
                                    object_name='views')
        elif obj_type == 'functions':
            query = render_template("/".join([self.sql_path, 'functions.sql']),
                                    schema_names=in_clause)
        elif obj_type == 'datatypes':
            query = render_template("/".join([self.sql_path, 'datatypes.sql']),
                                    schema_names=in_clause)

        if self.conn.connected():
            status, res = self.conn.execute_dict(query)
            if status:
                for record in res['rows']:
                    objects.append(record['object_name'])

        return objects


class SQLAutoComplete(object):
    """
    class SQLAutoComplete

        This class is used to provide the postgresql's autocomplete feature.
        This class used sqlparse to parse the given sql and psycopg2 to make
        the connection and get the tables, schemas, functions etc. based on
        the query.
    """

    def __init__(self, conn_info: ConnectionInfo):
        """
        This method is used to initialize the class.
        """
        self.conn_info = conn_info
        conn: psycopg2.extensions.connection = conn_info.get_connection(ConnectionType.INTELLISENSE)
        if not conn:
            raise RuntimeError('Intellisense Connection required')
        self.server = Server(conn)
        self.database = self.server.maintenance_db
        self.keyword_handler = Matcher(self.server.connection)
        self.search_path = []
