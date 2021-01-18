# ankisyncd - A personal Anki sync server
# Copyright (C) 2013 David Snopek
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import sys
import json
import re
import gzip
import io
import logging
import os
import random
import time
import unicodedata
import zipfile
import ssl

from webob import Response

# import anki.db
import anki.sync
# import anki.utils

# from anki.consts import REM_CARD, REM_NOTE
# from anki.consts import SYNC_VER
from webob.dec import wsgify
from webob.exc import *

from ankisyncd.full_sync import get_full_sync_manager
from ankisyncd.sessions import get_session_manager
from ankisyncd.users import get_user_manager
from ankisyncd.utils import *
from ankisyncd.consts import *

from wsgiref.simple_server import make_server, WSGIRequestHandler

logger = logging.getLogger("ankisyncd")


class SyncCollectionHandler(anki.sync.Syncer):
    operations = ['meta', 'applyChanges', 'start', 'applyGraves', 'chunk', 'applyChunk', 'sanityCheck2', 'finish']

    def __init__(self, col):
        # So that 'server' (the 3rd argument) can't get set
        anki.sync.Syncer.__init__(self, col)
        self.name = "[Server Syncer]"

    @staticmethod
    def _old_client(cv):
        if not cv:
            return False
        note = {"alpha": 0, "beta": 0, "rc": 0}
        client, version, platform = cv.split(',')
        for t in note.keys():
            # 如果版本是2.13.1alpha10， version=2.13.1，note["alpha"]=10
            if t in version:
                vs = version.split(t)
                version = vs[0]
                note[t] = int(vs[-1])
        # convert the version string, ignoring non-numeric suffixes like in beta versions of Anki
        # 类似2.13.1alpha10，会被去掉非数字的后缀，变成2.13.1
        version_no_suffix = re.sub(r'[^0-9.].*$', '', version)
        version_int = [int(x) for x in version_no_suffix.split('.')]
        if client == 'ankidesktop':
            # 2.0.27版本以下属于旧版本
            return version_int < [2, 0, 27]
        elif client == 'ankidroid':
            if version_int == [2, 3]:
                if note["alpha"]:
                    return note["alpha"] < 4
            else:
                # 2.2.3版本以下属于旧版本
                return version_int < [2, 2, 3]
        else:  # unknown client, assume current version
            return False

    def meta(self, v=None, cv=None):
        print("SyncCollectionHandler.meta() 获取元信息，同步协议版本：{}， 客户端版本：{}".format(v, cv))
        if self._old_client(cv):
            return Response(status=501)  # client needs upgrade
        # if v > SYNC_VER:
        #     return {"cont": False, "msg": "Your client is using unsupported sync protocol ({}, supported version: {})".format(v, SYNC_VER)}
        if v < 9 and self.col.schedVer() >= 2:
            return {"cont": False, "msg": "Your client doesn't support the v{} scheduler.".format(self.col.schedVer())}
        # Make sure the media database is open!
        if self.col.media.db is None:
            self.col.media.connect()
        return {
            'scm': self.col.scm,
            'ts': intTime(),
            'mod': self.col.mod,
            'usn': self.col._usn,
            'musn': self.col.media.lastUsn(),
            'msg': '',
            'cont': True,
            'hostNum': 0,
        }

    def usnLim(self):
        return "usn >= %d" % self.minUsn

    # ankidesktop >=2.1rc2 sends graves in applyGraves, but still expects
    # server-side deletions to be returned by start
    # 1. 获取被其他客户端删除的对象
    # 2. 删除当前客户端删除的对象s
    def start(self, minUsn, lnewer, graves={"cards": [], "notes": [], "decks": []}, offset=None):
        # if offset is not None:
        #     raise NotImplementedError('You are using the experimental V2 scheduler, which is not supported by the server.')
        # minUsn - 客户端usn
        # maxUsn - 服务端usn
        # minUsn <= maxUsn
        # 获取服务端chunk的时候使用到此属性
        self.maxUsn = self.col._usn
        print(
            "▶ 开始执行start方法，maxUsn: {}, minUsn: {}, lnewer: {}, graves: {}".format(self.maxUsn, minUsn, lnewer, graves))
        # 客户端入参minUsn，来自最近一次调用服务端/sync/meta接口返回的meta[usn]
        # 这个值不可能大于服务端usn，因为有可能其他客户端已经同步过数据，即增加过maxUsn
        self.minUsn = minUsn
        # 哪边更加新，就使用哪边的model deck tag对象信息 conf配置信息
        self.lnewer = not lnewer
        # 找到被超前与当前客户端的其他客户端删除的对象
        lgraves = self.removed()
        self.remove(graves)
        return lgraves

    def applyGraves(self, chunk):
        self.remove(chunk)

    def applyChanges(self, changes):
        # 客户端model deck tag元信息 全局conf配置
        # 如果model非空，表示客户端的model云信息发生了变化
        # 其他对象同理
        self.rchg = changes
        # 服务端配置
        lchg = self.changes()
        # merge our side before returning
        # 将客户端发生变化的元信息或配置合并到服务端
        self.mergeChanges(lchg, self.rchg)
        return lchg

    def sanityCheck2(self, client):
        server = self.sanityCheck()
        print("client: {}, server: {}".format(client, server))
        # 客户端和服务端的验证结果如果不同则不允许通过
        if client != server:
            return dict(status="bad", c=client, s=server)
        return dict(status="ok")

    def finish(self, mod=None):
        return anki.sync.Syncer.finish(self, intTime(1000))

    # This function had to be put here in its entirety because Syncer.removed()
    # doesn't use self.usnLim() (which we override in this class) in queries.
    # "usn=-1" has been replaced with "usn >= ?", self.minUsn by hand.
    def removed(self):
        cards = []
        notes = []
        decks = []
        # 找到超前于当前客户端的其他客户端
        # 删除的对象
        curs = self.col.db.execute(
            "select oid, type from graves where usn >= ?", self.minUsn)

        for oid, type in curs:
            if type == REM_CARD:
                cards.append(oid)
            elif type == REM_NOTE:
                notes.append(oid)
            else:
                decks.append(oid)

        return dict(cards=cards, notes=notes, decks=decks)

    def getModels(self):
        return [m for m in self.col.models.all() if m['usn'] >= self.minUsn]

    def getDecks(self):
        return [
            [g for g in self.col.decks.all() if g['usn'] >= self.minUsn],
            [g for g in self.col.decks.allConf() if g['usn'] >= self.minUsn]
        ]

    def getTags(self):
        return [t for t, usn in self.col.tags.allItems()
                if usn >= self.minUsn]


class SyncMediaHandler:
    operations = ['begin', 'mediaChanges', 'mediaSanity', 'uploadChanges', 'downloadFiles']

    def __init__(self, col):
        self.col = col

    def begin(self, skey):
        return {
            'data': {
                'sk': skey,
                'usn': self.col.media.lastUsn(),
            },
            'err': '',
        }

    def uploadChanges(self, data):
        """
        The zip file contains files the client hasn't synced with the server
        yet ('dirty'), and info on files it has deleted from its own media dir.
        """

        with zipfile.ZipFile(io.BytesIO(data), "r") as z:
            self._check_zip_data(z)
            processed_count = self._adopt_media_changes_from_zip(z)

        return {
            'data': [processed_count, self.col.media.lastUsn()],
            'err': '',
        }

    @staticmethod
    def _check_zip_data(zip_file):
        max_zip_size = 100 * 1024 * 1024
        max_meta_file_size = 100000

        meta_file_size = zip_file.getinfo("_meta").file_size
        sum_file_sizes = sum(info.file_size for info in zip_file.infolist())

        if meta_file_size > max_meta_file_size:
            raise ValueError("Zip file's metadata file is larger than %s "
                             "Bytes." % max_meta_file_size)
        elif sum_file_sizes > max_zip_size:
            raise ValueError("Zip file contents are larger than %s Bytes." %
                             max_zip_size)

    def _adopt_media_changes_from_zip(self, zip_file):
        """
        Adds and removes files to/from the database and media directory
        according to the data in zip file zipData.
        """
        oldUsn = self.col.media.lastUsn()
        # Get meta info first.
        meta = json.loads(zip_file.read("_meta").decode())
        # Remove media files that were removed on the client.
        media_to_remove = []
        for normname, ordinal in meta:
            if ordinal is None or ordinal == "":
                fname = self._normalize_filename(normname)
                media_to_remove.append(fname)
        # 删除文件的DB数据会导致usn增加
        if media_to_remove:
            self._remove_media_files(media_to_remove)
        # Add media files that were added on the client.
        media_to_add = []
        usn = self.col.media.lastUsn()
        for i in zip_file.infolist():
            if i.filename == "_meta":  # Ignore previously retrieved metadata.
                continue
            file_data = zip_file.read(i)
            csum = checksum(file_data)
            filename = self._normalize_filename(meta[int(i.filename)][0])
            file_path = os.path.join(self.col.media.dir(), filename)

            # Save file to media directory.
            with open(file_path, 'wb') as f:
                f.write(file_data)
            usn += 1
            media_to_add.append((filename, usn, csum))
        # We count all files we are to remove, even if we don't have them in
        # our media directory and our db doesn't know about them.
        processed_count = len(media_to_remove) + len(media_to_add)
        assert len(meta) == processed_count  # sanity check
        if media_to_add:
            self.col.media.db.executemany(
                "INSERT OR REPLACE INTO media VALUES (?,?,?)", media_to_add)
            self.col.media.db.commit()
        assert self.col.media.lastUsn() == oldUsn + processed_count  # TODO: move to some unit test
        return processed_count

    @staticmethod
    def _normalize_filename(filename):
        """
        Performs unicode normalization for file names. Logic taken from Anki's
        MediaManager.addFilesFromZip().
        """

        # Normalize name for platform.
        if isMac:  # global
            filename = unicodedata.normalize("NFD", filename)
        else:
            filename = unicodedata.normalize("NFC", filename)

        return filename

    def _remove_media_files(self, filenames):
        """
        Marks all files in list filenames as deleted and removes them from the
        media directory.
        """
        logger.debug('Removing %d files from media dir.' % len(filenames))
        for filename in filenames:
            try:
                self.col.media.syncDelete(filename)
                self.col.media.db.commit()
            except OSError as err:
                logger.error("Error when removing file '%s' from media dir: "
                             "%s" % (filename, str(err)))

    def downloadFiles(self, files):
        flist = {}
        cnt = 0
        sz = 0
        f = io.BytesIO()

        with zipfile.ZipFile(f, "w", compression=zipfile.ZIP_DEFLATED) as z:
            for fname in files:
                fpath = os.path.join(self.col.media.dir(), fname)
                z.write(fpath, str(cnt))
                flist[str(cnt)] = fname
                sz += os.path.getsize(os.path.join(self.col.media.dir(), fname))
                if sz > SYNC_MAX_BYTES or cnt > SYNC_MAX_FILES:
                    break
                cnt += 1
            z.writestr("_meta", json.dumps(flist))
        return f.getvalue()

    def mediaChanges(self, lastUsn):
        result = []
        server_lastUsn = self.col.media.lastUsn()
        fname = csum = None
        if lastUsn < server_lastUsn or lastUsn == 0:
            sql = "select fname,usn,csum from media order by usn desc limit ?"
            for fname, usn, csum, in self.col.media.db.execute(sql, server_lastUsn - lastUsn):
                result.append([fname, usn, csum])

        # anki assumes server_lastUsn == result[-1][1]
        # ref: anki/sync.py:720 (commit cca3fcb2418880d0430a5c5c2e6b81ba260065b7)
        result.reverse()

        return {'data': result, 'err': ''}

    def mediaSanity(self, local=None):
        print("SyncMediaHandler.mediaSanity() 媒体文件合法性检查")
        print("服务端媒体usn：{}, 客户端媒体usn：{}".format(self.col.media.mediaCount(), local))
        if self.col.media.mediaCount() == local:
            result = "OK"
        else:
            result = "FAILED"
        return {'data': result, 'err': ''}


class SyncUserSession:
    def __init__(self, name, path, collection_manager, setup_new_collection=None):
        self.skey = self._generate_session_key()
        self.name = name
        self.path = path
        self.collection_manager = collection_manager
        self.setup_new_collection = setup_new_collection
        self.version = None
        self.client_version = None
        self.created = time.time()
        self.collection_handler = None
        self.media_handler = None

        # make sure the user path exists
        if not os.path.exists(path):
            os.mkdir(path)

    def _generate_session_key(self):
        print("SyncUserSession._generate_session_key() 生成随机的session key")
        return checksum(str(random.random()))[:8]

    def get_collection_path(self):
        return os.path.realpath(os.path.join(self.path, 'collection.anki2'))

    def get_thread(self):
        print("SyncUserSession.get_thread() 获取线程")
        return self.collection_manager.get_collection(self.get_collection_path(), self.setup_new_collection)

    def get_handler_for_operation(self, operation, col):
        if operation in SyncCollectionHandler.operations:
            attr, handler_class = 'collection_handler', SyncCollectionHandler
        elif operation in SyncMediaHandler.operations:
            attr, handler_class = 'media_handler', SyncMediaHandler
        else:
            raise Exception("no handler for {}".format(operation))

        if getattr(self, attr) is None:
            setattr(self, attr, handler_class(col))
        handler = getattr(self, attr)
        # The col object may actually be new now! This happens when we close a collection
        # for inactivity and then later re-open it (creating a new Collection object).
        handler.col = col
        return handler


class SyncApp:
    valid_urls = SyncCollectionHandler.operations + SyncMediaHandler.operations + ['hostKey', 'upload', 'download']

    def __init__(self, config):
        from ankisyncd.thread import get_collection_manager

        self.data_root = os.path.abspath(config['data_root'])
        self.base_url = config['base_url']
        self.base_media_url = config['base_media_url']
        self.setup_new_collection = None

        self.prehooks = {}
        self.posthooks = {}

        self.user_manager = get_user_manager(config)
        self.session_manager = get_session_manager(config)
        self.full_sync_manager = get_full_sync_manager(config)
        self.collection_manager = get_collection_manager(config)

        # make sure the base_url has a trailing slash
        if not self.base_url.endswith('/'):
            self.base_url += '/'
        if not self.base_media_url.endswith('/'):
            self.base_media_url += '/'

    # backwards compat
    @property
    def hook_pre_sync(self):
        return self.prehooks.get("start")

    @hook_pre_sync.setter
    def hook_pre_sync(self, value):
        self.prehooks['start'] = value

    @property
    def hook_post_sync(self):
        return self.posthooks.get("finish")

    @hook_post_sync.setter
    def hook_post_sync(self, value):
        self.posthooks['finish'] = value

    @property
    def hook_upload(self):
        return self.prehooks.get("upload")

    @hook_upload.setter
    def hook_upload(self, value):
        self.prehooks['upload'] = value

    @property
    def hook_download(self):
        return self.posthooks.get("download")

    @hook_download.setter
    def hook_download(self, value):
        self.posthooks['download'] = value

    def generateHostKey(self, username):
        """Generates a new host key to be used by the given username to identify their session.
        This values is random."""
        print("SyncApp.generateHostKey() 根据用户名生成唯一标识session的随机数")
        import hashlib
        import time
        import random
        import string
        chars = string.ascii_letters + string.digits
        val = ':'.join([username, str(int(time.time())), ''.join(random.choice(chars) for x in range(8))]).encode()
        return hashlib.md5(val).hexdigest()

    def create_session(self, username, user_path):
        print("SyncApp.create_session() 创建session")
        return SyncUserSession(username, user_path, self.collection_manager, self.setup_new_collection)

    def _decode_data(self, data, compression=0):
        print("SyncApp._decode_data() 解码数据")
        if compression:
            with gzip.GzipFile(mode="rb", fileobj=io.BytesIO(data)) as gz:
                data = gz.read()

        try:
            data = json.loads(data.decode())
        except (ValueError, UnicodeDecodeError):
            data = {'data': data}

        return data

    def operation_hostKey(self, username, password):
        print("SyncApp.operation_hostKey() 用户身份验证")
        if not self.user_manager.authenticate(username, password):
            return
        dirname = self.user_manager.userdir(username)
        if dirname is None:
            return
        hkey = self.generateHostKey(username)
        user_path = os.path.join(self.data_root, dirname)
        session = self.create_session(username, user_path)
        self.session_manager.save(hkey, session)

        return {'key': hkey}

    def operation_upload(self, col, data, session):
        # Verify integrity of the received database file before replacing our
        # existing db.

        return self.full_sync_manager.upload(col, data, session)

    def operation_download(self, col, session):
        # returns user data (not media) as a sqlite3 database for replacing their
        # local copy in Anki
        return self.full_sync_manager.download(col, session)

    @wsgify
    def __call__(self, req):
        print("SyncApp.__call__() 处理HTTP请求 url: {}， POST数据：{}".format(req.path, req.POST))
        if req.path.startswith(self.base_url) is False and \
                req.path.startswith(self.base_media_url) is False:
            # 重定向到网站首页
            return "Anki学霸"
        # 解码数据
        try:
            compression = int(req.POST['c'])
        except KeyError:
            compression = 0
        try:
            data = req.POST['data'].file.read()
            data = self._decode_data(data, compression)
        except KeyError:
            data = {}
        # 登陆验证
        if req.path == self.base_url + "hostKey":
            result = self.operation_hostKey(data.get("u"), data.get("p"))
            if result:
                return json.dumps(result)
            else:
                raise HTTPForbidden('null')
        print("登陆验证通过")
        # 已经登陆，验证session
        hkey = None
        if 'k' in req.POST:
            hkey = req.POST['k']
        if hkey is None and 'k' in req.GET:
            hkey = req.GET['k']
        session = self.session_manager.load(hkey, self.create_session)
        if session is None and 'sk' in req.POST:
            skey = req.POST['sk']
            session = self.session_manager.load_from_skey(skey, self.create_session)
        if session is None:
            raise HTTPForbidden()

        def validURL(u):
            if u not in self.valid_urls:
                raise HTTPNotFound()

        # 处理非媒体数据同步请求
        if req.path.startswith(self.base_url):
            url = req.path[len(self.base_url):]
            validURL(url)
            # 上传
            if url == "upload":
                thread = session.get_thread()
                if url in self.prehooks:
                    thread.execute(self.prehooks[url], [session])
                result = thread.execute(self.operation_upload, [data['data'], session])
                if url in self.posthooks:
                    thread.execute(self.posthooks[url], [session])
                return result
            # 下载
            if url == "download":
                # CollectionWrapper对象
                thread = session.get_thread()
                if url in self.prehooks:
                    thread.execute(self.prehooks[url], [session])
                result = thread.execute(self.operation_download, [session])
                if url in self.posthooks:
                    thread.execute(self.posthooks[url], [session])
                return result
            # 'meta' passes the SYNC_VER but it isn't used in the handler
            if url == 'meta':
                if session.skey is None and 's' in req.POST:
                    session.skey = req.POST['s']
                if 'v' in data:
                    session.version = data['v']
                if 'cv' in data:
                    session.client_version = data['cv']
                # 保存可能已被更新的session
                self.session_manager.save(hkey, session)
                session = self.session_manager.load(hkey, self.create_session)
            thread = session.get_thread()
            if url in self.prehooks:
                thread.execute(self.prehooks[url], [session])
            # 调用/sync/xxx接口方法
            result = self._execute_handler_method_in_thread(url, data, session)
            # If it's a complex data type, we convert it to JSON
            if type(result) not in (str, bytes, Response):
                result = json.dumps(result)
            if url in self.posthooks:
                thread.execute(self.posthooks[url], [session])
            return result
        # 处理媒体数据同步请求
        elif req.path.startswith(self.base_media_url):
            url = req.path[len(self.base_media_url):]
            validURL(url)
            if url == "begin":
                data['skey'] = session.skey
            result = self._execute_handler_method_in_thread(url, data, session)
            # If it's a complex data type, we convert it to JSON
            if type(result) not in (str, bytes):
                result = json.dumps(result)
            return result
        return ""

    @staticmethod
    def _execute_handler_method_in_thread(method_name, keyword_args, session):
        """
        Gets and runs the handler method specified by method_name inside the
        thread for session. The handler method will access the collection as
        self.col.
        """
        # print("SyncApp._execute_handler_method_in_thread() 准备执行线程中的某个方法, method_mame: {}. keyword_args: {}, "
        #       "session: {}".format(method_name, keyword_args, session))
        print("SyncApp._execute_handler_method_in_thread() 准备执行线程中的某个方法, method_mame: {}, "
              "session: {}".format(method_name, session))

        def run_func(col, **keyword_args):
            # Retrieve the correct handler method.
            handler = session.get_handler_for_operation(method_name, col)
            handler_method = getattr(handler, method_name)

            res = handler_method(**keyword_args)

            col.save()
            return res

        run_func.__name__ = method_name  # More useful debugging messages.

        # Send the closure to the thread for execution.
        thread = session.get_thread()
        result = thread.execute(run_func, kw=keyword_args)
        # print("响应数据：{}".format(result))
        print("-" * 100)
        return result


def make_app(global_conf, **local_conf):
    return SyncApp(**local_conf)


class RequestHandler(WSGIRequestHandler):
    logger = logging.getLogger("ankisyncd.http")

    def log_error(self, format, *args):
        self.logger.error("%s %s", self.address_string(), format % args)

    def log_message(self, format, *args):
        self.logger.info("%s %s", self.address_string(), format % args)


def main():
    import ankisyncd
    logging.basicConfig(level=logging.ERROR, format="[%(asctime)s]:%(levelname)s:%(name)s:%(message)s")
    import ankisyncd.config
    config = ankisyncd.config.load()
    host, port, app = config['host'], int(config['port']),SyncApp(config)
    httpd = make_server(host, port, app, handler_class=RequestHandler)
    try:
        logger.info("Serving HTTP on {} port {}...".format(*httpd.server_address))
        httpd.serve_forever()
    except KeyboardInterrupt:
        logger.info("Exiting...")
    finally:
        from ankisyncd.thread import shutdown
        shutdown()
