# -*- coding: utf-8 -*-

import os
from sqlite3 import dbapi2 as sqlite
import logging
from ankisyncd.db import DB
from webob.exc import HTTPBadRequest
from ankisyncd.collection import CollectionWrapper

logger = logging.getLogger("ankisyncd.full_sync")

class FullSyncManager:
    def upload(self, col, data, session):
        # Verify integrity of the received database file before replacing our
        # existing db.
        print("FullSyncManager.upload() 完全上传")
        temp_db_path = session.get_collection_path() + ".tmp"
        with open(temp_db_path, 'wb') as f:
            f.write(data)

        try:
            with DB(temp_db_path) as test_db:
                if test_db.scalar("pragma integrity_check") != "ok":
                    raise HTTPBadRequest("Integrity check failed for uploaded "
                                         "collection database file.")
        except sqlite.Error as e:
            raise HTTPBadRequest("Uploaded collection database file is "
                                 "corrupt.")

        # Overwrite existing db.
        col.close()
        # try:
        #     os.replace(temp_db_path, session.get_collection_path())
        # finally:
        #     col.reopen()
        #     col.load()
        os.replace(temp_db_path, session.get_collection_path())
        return "OK"

    def download(self, col, session):
        # col.close()
        # try:
        #     data = open(session.get_collection_path(), 'rb').read()
        # finally:
        #     col.open()
        #     col.load()
        data = open(session.get_collection_path(), 'rb').read()
        return data


def get_full_sync_manager(config):
    print("full_sync.py.get_full_sync_manager() 获取完全同步管理器")
    if "full_sync_manager" in config and config["full_sync_manager"]:  # load from config
        import importlib
        import inspect
        module_name, class_name = config['full_sync_manager'].rsplit('.', 1)
        module = importlib.import_module(module_name.strip())
        class_ = getattr(module, class_name.strip())

        if not FullSyncManager in inspect.getmro(class_):
            raise TypeError('''"full_sync_manager" found in the conf file but it doesn''t
                            inherit from FullSyncManager''')
        return class_(config)
    else:
        logger.info("Not found full_sync_manager in config, using FullSyncManager for full sync")
        return FullSyncManager()
