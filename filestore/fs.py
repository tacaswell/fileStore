from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import six
from pkg_resources import resource_filename
from contextlib import contextmanager
import json
import logging
import os.path
import shutil

import pymongo
from pymongo import MongoClient

import boltons.cacheutils

from .handlers_base import DuplicateHandler
from .utils import _make_sure_path_exists
import os

from . import core
from . import core_v0
from .fs_base import _ChainMap, FileStoreROBase

_API_MAP = {0: core_v0,
            1: core}


logger = logging.getLogger(__name__)


class FileStoreRO(FileStoreROBase):
    '''Base FileStore object that knows how to read the database.

    Parameters
    ----------
    config : dict
       Much have keys {'database', 'collection', 'host'} and may have a 'port'

    handler_reg : dict, optional
       Mapping between spec names and handler classes

    version : int, optional
        schema version of the database.
        Defaults to 1

    root_map : dict, optional
        str -> str mapping to account for temporarily moved/copied/remounted
        files.  Any resources which have a ``root`` in ``root_map``
        will have the resource path updated before being handed to the
        Handler in ``get_spec_handler``

    '''
    @property
    def version(self):
        return self._version

    @version.setter
    def version(self, val):
        if self._api is not None:
            raise RuntimeError("Can not change api version at runtime")
        self._api = _API_MAP[val]
        self._version = val

    def __init__(self, config, handler_reg=None, version=1, root_map=None):
        super().__init__(handler_reg, root_map)
        self.config = config
        self._api = None
        self.version = version

        self._datum_cache = boltons.cacheutils.LRU(max_size=1000000)
        self.__db = None
        self.__conn = None
        self.__datum_col = None
        self.__res_col = None
        self.__res_update_col = None

    def disconnect(self):
        self.__db = None
        self.__conn = None
        self.__datum_col = None
        self.__res_col = None

    def reconfigure(self, config):
        self.disconnect()
        self.config = config

    def _r_on_miss(self, k):
        col = self._resource_col
        return self._api.resource_given_uid(col, k)

    def resource_given_uid(self, uid):
        col = self._resource_col
        return self._api.resource_given_uid(col, uid)

    def retrieve(self, eid):
        return self._api.retrieve(self._datum_col, eid,
                                  self._datum_cache, self.get_spec_handler,
                                  logger)

    # back compat
    get_datum = retrieve

    @property
    def _db(self):
        if self.__db is None:
            conn = self._connection
            self.__db = conn.get_database(self.config['database'])
            if self.version > 0:
                sentinel = self.__db.get_collection('sentinel')
                versioned_collection = ['resource', 'datum']
                for col_name in versioned_collection:
                    val = sentinel.find_one({'collection': col_name})
                    if val is None:
                        raise RuntimeError('there is no version sentinel for '
                                           'the {} collection'.format(col_name)
                                           )
                    if val['version'] != self.version:
                        raise RuntimeError('DB version {!r} does not match'
                                           'API version of FS {} for the '
                                           '{} collection'.format(
                                               val, self.version, col_name))
        return self.__db

    @property
    def _resource_col(self):
        if self.__res_col is None:
            self.__res_col = self._db.get_collection('resource')
            self.__res_col.create_index('resource_id')

        return self.__res_col

    @property
    def _resource_update_col(self):
        if self.__res_update_col is None:
            self.__res_update_col = self._db.get_collection('resource_update')
            self.__res_update_col.create_index([
                ('resource', pymongo.DESCENDING),
                ('time', pymongo.DESCENDING)
            ])

        return self.__res_update_col

    @property
    def _datum_col(self):
        if self.__datum_col is None:
            self.__datum_col = self._db.get_collection('datum')
            self.__datum_col.create_index('datum_id', unique=True)
            self.__datum_col.create_index('resource')

        return self.__datum_col

    @property
    def _connection(self):
        if self.__conn is None:
            self.__conn = MongoClient(self.config['host'],
                                      self.config.get('port', None))
        return self.__conn

    def get_history(self, resource_uid):
        '''Generator of all updates to the given resource

        Parameters
        ----------
        resource_uid : Document or str
            The resource to get the history of

        Yields
        ------
        update : Document
        '''
        if self.version == 0:
            raise NotImplementedError('No history in v0 schema')

        for doc in self._api.get_resource_history(
                self._resource_update_col, resource_uid):
            yield doc


class FileStore(FileStoreRO):
    '''FileStore object that knows how to create new documents.'''
    def insert_resource(self, spec, resource_path, resource_kwargs, root=None):
        '''
         Parameters
         ----------

         spec : str
             spec used to determine what handler to use to open this
             resource.

         resource_path : str or None
             Url to the physical location of this resource

         resource_kwargs : dict, optional
             resource_kwargs name/value pairs of additional kwargs to be
             passed to the handler to open this resource.

         root : str, optional
             The 'root' part of the resource path.


        '''
        if root is None:
            root = ''

        col = self._resource_col

        return self._api.insert_resource(col, spec, resource_path,
                                         resource_kwargs,
                                         self.known_spec,
                                         root=root)

    def insert_datum(self, resource, datum_id, datum_kwargs):
        '''insert a datum for the given resource

        Parameters
        ----------

        resource : Resource or Resource.id
            Resource object

        datum_id : str
            Unique identifier for this datum.  This is the value stored in
            metadatastore and is the value passed to `retrieve` to get
            the data back out.

        datum_kwargs : dict
            dict with any kwargs needed to retrieve this specific
            datum from the resource.

        '''
        col = self._datum_col

        return self._api.insert_datum(col, resource, datum_id, datum_kwargs,
                                      self.known_spec, self._resource_col)

    def bulk_insert_datum(self, resource, datum_ids, datum_kwarg_list):
        col = self._datum_col

        return self._api.bulk_insert_datum(col, resource, datum_ids,
                                           datum_kwarg_list)

    def datum_gen_given_resource(self, resource_or_uid):
        """Given resource or resource uid return associated datum documents.
        """
        actual_resource = self.resource_given_uid(resource_or_uid)
        datum_gen = self._api.get_datum_by_res_gen(self._datum_col,
                                                   actual_resource['uid'])
        return datum_gen

    def get_file_list(self, resource_or_uid, datum_kwarg_gen):
        """Given a resource or resource uid and an iterable of datum kwargs,
        return filepaths.

        """
        actual_resource = self.resource_given_uid(resource_or_uid)
        return self._api.get_file_list(actual_resource, datum_kwarg_gen,
                                       self.get_spec_handler)

    def resource_given_eid(self, eid):
        '''Given a datum eid return its Resource document
        '''
        if self.version == 0:
            raise NotImplementedError('V0 has no notion of root so can not '
                                      'change it so no need for this method')

        res = self._api.resource_given_eid(self._datum_col, eid,
                                           self._datum_cache, logger)
        return self._resource_cache[res]


class FileStoreMoving(FileStore):
    '''FileStore object that knows how to move files.'''
    def change_root(self, resource_or_uid, new_root, remove_origin=True,
                    verify=False, file_rename_hook=None):
        '''Change the root directory of a given resource

        The registered handler must have a `get_file_list` method and the
        process running this method must have read/write access to both the
        source and destination file systems.

        Internally the resource level directory information is stored
        as two parts: the root and the resource_path.  The 'root' is
        the non-semantic component (typically a mount point) and the
        'resource_path' is the 'semantic' part of the file path.  For
        example, it is common to collect data into paths that look like
        ``/mnt/DATA/2016/04/28``.  In this case we could split this as
        ``/mnt/DATA`` as the 'root' and ``2016/04/28`` as the resource_path.



        Parameters
        ----------
        resource_or_uid : Document or str
            The resource to move the files of

        new_root : str
            The new 'root' to copy the files into

        remove_origin : bool, optional (True)
            If the source files should be removed

        verify : bool, optional (False)
            Verify that the move happened correctly.  This currently
            is not implemented and will raise if ``verify == True``.

        file_rename_hook : callable, optional
            If provided, must be a callable with signature ::

               def hook(file_counter, total_number, old_name, new_name):
                   pass

            This will be run in the inner loop of the file copy step and is
            run inside of an unconditional try/except block.

        See Also
        --------
        `FileStore.shift_root`


        .. Warning

           This will change documents in your data base, move files
           and possibly delete files.  Be sure you know what you are
           doing.

        '''
        resource = dict(self.resource_given_uid(resource_or_uid))

        file_lists = self.copy_files(resource, new_root, verify,
                                     file_rename_hook)
        old_file_list, new_file_list = zip(*file_lists)

        # update the database
        new_resource = dict(resource)
        new_resource['root'] = new_root

        update_col = self._resource_update_col
        resource_col = self._resource_col
        ret = self._api.update_resource(update_col, resource_col,
                                        old=resource,
                                        new=new_resource,
                                        cmd_kwargs=dict(
                                            remove_origin=remove_origin,
                                            verify=verify,
                                            new_root=new_root),
                                        cmd='change_root')

        # remove original files
        if remove_origin:
            for f in old_file_list:
                os.unlink(f)

        # nuke caches
        uid = resource['uid']
        self._resource_cache.pop(uid, None)
        for k in list(self._handler_cache):
            if k[0] == uid:
                del self._handler_cache[k]

        return ret
