import logging
import optparse
import time
import datetime
import os
import glob
import shutil
import copy
import signal
import errno
import sqlite3
from threading import Thread, Semaphore
from queue import Queue, Empty
from functools import wraps
from contextlib import contextmanager
from sqlalchemy import create_engine, func, asc
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool
from trapum_db import (
    Pointing, Beam, FileAction, FileType,
    FileActionRequest, Beam, BeamformerConfiguration,
    Target, DataProduct, StorageDevice, DataProductStorageMap)

log = logging.getLogger('trapum_db.archive_service')

DISK_LIMIT = 0.995  # Disks will not be filled beyond this fraction
DISK_PATH = "/media/local/"
ACTIVE_DISKS = 20
ACTION_TYPE = "mtrans::archive"
TERMINATOR = "EOQ"
EXPECTED_PERFORMANCE = 200e6  # 200 MB/s
MIN_PEFORMANCE = 1e6  # 1 MB/s
DEFAULT_SQLITE_DB = "archive.db"

class NoSpaceAvailable(Exception):
    pass

class DataBaseConn(object):
    def __init__(self, database):
        self._session_engine = create_engine(database,
            echo=False, poolclass=NullPool)
        self._session_factory = sessionmaker(
            bind=self._session_engine)

    @contextmanager
    def session(self):
        session = self._session_factory()
        try:
            yield session
            session.commit()
        except Exception as error:
            session.rollback()
            raise error
        finally:
            session.close()


class Disk(object):
    def __init__(self, path):
        self._path = path
        self._reserved = 0

    def __repr__(self):
        return "<Disk: {}>".format(self.name)

    @property
    def name(self):
        return self._path.split("/")[-1]

    @property
    def path(self):
        return self._path

    @property
    def free(self):
        return shutil.disk_usage(self._path).free

    @property
    def total(self):
        return shutil.disk_usage(self._path).total

    @property
    def used(self):
        return shutil.disk_usage(self._path).used

    @property
    def full(self):
        return self.used >= self.total * DISK_LIMIT

    def has_space_for(self, size):
        return size < self.free * DISK_LIMIT

    def reserve(self, size):
        if self._reserved + size > self.free * DISK_LIMIT:
            raise NoSpaceAvailable
        else:
            self._reserved += size


class GenricCopyRequest(object):
    def __init__(self, source, dest):
        self.source = source
        self.dest = dest
        self.disk = None
        self.success = False
        self.error = None

    def __repr__(self):
        return "<{}: {} --> {}>".format(
            self.__class__.__name__,
            os.path.basename(self.source),
            os.path.basename(self.dest))


    def get_disk_path(self, base):
        path = os.path.dirname(self.dest)
        disk_path = os.path.join(base, path)
        return disk_path


class TrapumCopyRequest(GenricCopyRequest):
    def __init__(self, source, dest, request_id, dp_id):
        super(TrapumCopyRequest, self).__init__(source, dest)
        self.request_id = request_id
        self.dp_id = dp_id


class CopyThread(Thread):
    def __init__(self, queue, output_queue, disk, disk_sem):
        Thread.__init__(self)
        self._queue = queue
        self._output_queue = output_queue
        self._disk = disk
        self._disk_sem = disk_sem
        self._stopper = False
        self._has_sem = False
        self._performance = EXPECTED_PERFORMANCE

    def __repr__(self):
        return "<CopyThread: {}>".format(repr(self._disk))

    def stop(self):
        self._stopper = True

    def _acquire_sem(self):
        #log.debug("{}: Acquiring semaphore".format(self.__repr__()))
        self._disk_sem.acquire()
        self._has_sem = True
        #log.debug("{}: Semaphore succesfully acquired".format(self.__repr__()))

    def _release_sem(self):
        #log.debug("{}: Releasing semaphore".format(self.__repr__()))
        self._disk_sem.release()
        self._has_sem = False
        #log.debug("{}: Semaphore succesfully released".format(self.__repr__()))

    def handle_messsage(self, request):
        log.debug("{}: Handling request: {}".format(self.__repr__(), request))
        if request == TERMINATOR:
            log.info("{}: Encountered terminator, exiting thread".format(self.__repr__()))
            self.stop()
            return
        log.info("{}: Request source: {},  destination: {}".format(
            self.__repr__(), request.source, request.dest))
        size = os.stat(request.source).st_size
        log.info("{}: Source size: {}".format(self.__repr__(), size))
        if not self._disk.has_space_for(size):
            log.error("Disk no longer has space for file")
            request.success = False
            request.error = NoSpaceAvailable("Disk {} no longer has space for file".format(
                request.disk.name))
            self._output_queue.put(request)
            self._queue.task_done()
        disk_path = request.get_disk_path(self._disk.path)
        log.debug("{}: Disk path: {}".format(self.__repr__(), disk_path))
        try:
            log.debug("{}: Making directories".format(self.__repr__()))
            try:
                os.makedirs(disk_path)
            except OSError as error:
                if error.errno != errno.EEXIST:
                    raise
            expected_copy_time = size / self._performance
            log.debug("{}: Exepected copy time {} seconds".format(
                self.__repr__(), expected_copy_time))
            start = time.time()
            #shutil.copy2(request.source, disk_path)
            os.system("rsync -a -W --no-compress {} {}".format(request.source, disk_path))
            os.sync()
            end = time.time()
            log.debug("{}: Actual copy time {} seconds".format(
                self.__repr__(), (end - start)))
            t = max(1, end-start)
            self._performance = max(size / t, MIN_PEFORMANCE)
            log.info("{}: Transfer rate {:.1f} MB/s".format(self.__repr__(), self._performance/1e6))
        except Exception as error:
            log.exception("{}: Error during copy process: {}".format(self.__repr__(), str(error)))
            request.success = False
            request.error = error
        else:
            log.debug("{}: Copy successful".format(self.__repr__()))
            request.success = True
        log.debug("{}: Passing request into queue".format(self.__repr__()))
        self._output_queue.put(request)
        self._queue.task_done()

    def run(self):
        log.debug("{}: Thread starting".format(self.__repr__()))
        try:
            while not self._stopper:
                self._acquire_sem()
                try:
                    request = self._queue.get_nowait()
                    log.info("{}: Remaining request for this disk: {}".format(
                        self.__repr__(), self._queue.unfinished_tasks))
                except Empty:
                    self._release_sem()
                    time.sleep(1)
                    continue
                except Exception as error:
                    log.exception(str(error))
                    raise
                else:
                    try:
                        self.handle_messsage(request)
                    except Exception as error:
                        log.exception("Error during copy message handling: {}".format(str(error)))
                    finally:
                        if self._has_sem:
                            self._release_sem()
        except Exception as error:
            log.exception(str(error))
        finally:
            if self._has_sem:
                self._release_sem()
        log.debug("{}: Thread exiting".format(self.__repr__()))


class CopyManager(object):
    def __init__(self, disks, active_disks=ACTIVE_DISKS):
        self._disks = disks
        self._disk_sem = Semaphore(active_disks)
        self._threads = []
        self._queues = {}
        for disk in disks:
            self._queues[disk] = Queue()
        self._output_queue = Queue()
        self._queues[None] = Queue()

    def __repr__(self):
        return "<CopyManager: {} disks>".format(len(self._disks))

    def terminate(self):
        log.warning("CopyManager received terminate request")
        for q in self._queues.values():
            while True:
                try:
                    q.get_nowait()
                except:
                    break
        self.wait()

    def start(self):
        log.debug("Starting CopyManager")
        for disk in self._disks:
            log.debug("Creating copy thread for {}".format(disk))
            thread = CopyThread(self._queues[disk], self._output_queue, disk, self._disk_sem)
            self._threads.append(thread)
            thread.start()

    def wait(self):
        log.info("Waiting on all copies to complete")
        for q in self._queues.values():
            q.put(TERMINATOR)
        for thread in self._threads:
            log.debug("Joined thread {}".format(thread))
            thread.join()
        log.debug("Injecting terminator into output queue")
        self._output_queue.put(TERMINATOR)

    def enqueue(self, message):
        log.debug("Enqueuing message: {}".format(message))
        self._queues[message.disk].put(message)


class DBUpdater(Thread):
    def __init__(self, db_conn, queue):
        Thread.__init__(self)
        self._db_conn = db_conn
        self._queue = queue
        self._stopper = False
        self._sd_ids = {}

    def stop(self):
        self._stopper = True

    def upload(self, copy_request):
        log.debug("Updating database with copy request: {}".format(copy_request))
        with self._db_conn.session() as s:
            far = s.query(FileActionRequest).get(copy_request.request_id)
            log.debug("Retrieved FileActionRequest with ID: {}".format(copy_request.request_id))
            far.success = int(copy_request.success)
            far.completed_at = datetime.datetime.utcnow()
            s.add(far)
            log.debug("FileActionRequest updated")
            if not copy_request.success:
                return
            disk_path = copy_request.disk.path
            disk_name = os.path.basename(disk_path)
            if disk_path not in self._sd_ids.keys():
                log.debug("Finding storage device ID")
                storage_device = s.query(StorageDevice).filter(
                        StorageDevice.name.ilike(disk_name)
                    ).first()
                if not storage_device:
                    og.debug("No storage device found, creating new entry for {}".format(disk_name))
                    storage_device = StorageDevice()
                    storage_device.name = disk_name
                    storage_device.type = "HDD"
                    storage_device.location = "SA::KDRA::mtrans"
                    s.add(storage_device)
                    s.flush()
                log.debug("Storage device ID: {}".format(storage_device.id))
                self._sd_ids[disk_path] = storage_device.id


            result = s.query(DataProductStorageMap).filter(
                DataProductStorageMap.data_product_id == copy_request.dp_id,
                DataProductStorageMap.storage_device_id == self._sd_ids[disk_path],
                DataProductStorageMap.path.like(copy_request.dest)
                ).first()
            if result:
                log.info("DataProductStorageMap entry already exists for request: {}".format(copy_request))
            else:
                log.debug("Adding entry to DataProductStorageMap")
                map_entry = DataProductStorageMap()
                map_entry.data_product_id = copy_request.dp_id
                map_entry.storage_device_id = self._sd_ids[disk_path]
                map_entry.path = copy_request.dest
                s.add(map_entry)
                s.flush()
                log.info("Database updated based on copy request: {}".format(copy_request))

    def run(self):
        log.debug("Starting DBUpdater thread")
        while not self._stopper:
            request = self._queue.get()
            if request == TERMINATOR:
                log.debug("Exiting DBUpdater thread")
                return
            self.upload(request)


def sanitise_string(string):
    valid_nonalnum = ["-", "+"]
    return "".join([i if any([i.isalnum(), i in valid_nonalnum]) else '_' for i in string])


def generate_requests_from_db(db_conn):
    log.info("Finding copy requests from database")
    with db_conn.session() as s:
        results = s.query(
                FileActionRequest.id.label("request_id"),
            ).join(
                FileAction
            ).join(
                DataProduct, FileActionRequest.data_product_id == DataProduct.id
            ).add_columns(
                DataProduct.id.label("dp_id"),
                DataProduct.filename,
                DataProduct.filepath
            ).join(
                FileType
            ).add_columns(
                FileType.name.label("file_type")
            ).join(
                Pointing, DataProduct.pointing_id == Pointing.id
            ).add_columns(
                Pointing.mkat_pid,
                Pointing.utc_start
            ).join(
                Beam, DataProduct.beam_id == Beam.id
            ).add_columns(
                Beam.name.label("beam_name")
            ).join(
                BeamformerConfiguration, Pointing.bf_config_id == BeamformerConfiguration.id
            ).add_columns(
                BeamformerConfiguration.centre_frequency
            ).join(
                Target
            ).add_columns(
                Target.source_name
            ).filter(
                FileAction.action == ACTION_TYPE
            ).filter(
                FileActionRequest.completed_at.is_(None),
                DataProduct.available == 1,
            ).order_by(
                asc(FileActionRequest.requested_at)
            ).all()
        copy_requests = []
        log.info("{} requests recovered".format(len(results)))
        for result in results:
            source_path = os.path.join(result.filepath, result.filename)
            extension = os.path.splitext(source_path)[1]
            pid = sanitise_string(result.mkat_pid)
            source_name = sanitise_string(result.source_name)
            utc = result.utc_start.strftime("%Y-%m-%d-%H:%M:%S")
            freq = str(int((result.centre_frequency / 1e6)))
            ft_name = sanitise_string(result.file_type)
            dest_path = os.path.join("TRAPUM", pid, source_name, utc, freq, result.beam_name,
                "{}_{}_{}_{}_{}{}".format(
                    source_name, utc, freq, result.beam_name, ft_name, extension))
            copy_request = TrapumCopyRequest(source_path, dest_path, result.request_id, result.dp_id)
            copy_requests.append(copy_request)
            log.debug("Created copy request: {}".format(copy_request))
        return copy_requests

def generate_requests_from_file(fname):
    copy_requests = []
    with open(fname, "r") as f:
        for line in f:
            source, dest = line.strip().split()
            dest = dest.lstrip("/")
            copy_requests.append(GenricCopyRequest(source, dest))
    return copy_requests

def get_disks():
    disk_paths = sorted(glob.glob("{}/TRAPUM_*".format(DISK_PATH)))
    disk_pool = [Disk(path) for path in disk_paths]
    return disk_pool

def create_mappings(disks, requests, archive_db):
    conn = sqlite3.connect(archive_db)
    c = conn.cursor()
    c.execute("CREATE TABLE IF NOT EXISTS mapping (source text, dest text, disk text, UNIQUE (source, dest));")
    conn.commit()
    valid_requests = []
    disk_map = {disk.path: disk for disk in disks}

    for request in requests:
        size = os.stat(request.source).st_size
        c.execute("SELECT disk FROM mapping WHERE source=? and dest=?", (request.source, request.dest))
        pre_disk = c.fetchone()
        if pre_disk:
            pre_disk = pre_disk[0]
            log.info("Found prior mapping with for request {} to disk {}".format(request, pre_disk))

            if pre_disk not in disk_map:
                log.warning("Mapped disk is not currently available, skipping request: {}".format(request))
            else:
                request.disk = disk_map[pre_disk]
                valid_requests.append(request)
                continue
        for disk in disks:
            try:
                disk.reserve(size)
            except NoSpaceAvailable:
                continue
            else:
                request.disk = disk
                valid_requests.append(request)
                c.execute("INSERT INTO mapping VALUES (?, ?, ?)", (request.source, request.dest, disk.path))
                break
        else:
            log.error("No disk space available for request: {}".format(request))
    conn.commit()
    return valid_requests


FORMAT = "[%(levelname)s - %(asctime)s - %(filename)s:%(lineno)s] %(message)s"
logging.basicConfig(format=FORMAT, level=logging.WARNING)
log.setLevel("DEBUG")
#db = "mysql+pymysql://root:trapumdb@10.98.76.190:30002/trapum_web"
#db_conn = DataBaseConn(db)
#res = generate_requests_from_db(db_conn)[0]
#requests = generate_requests_from_file("/beegfs/DATA/MeerTIME/J1748-2021B/J1748-2021B.to_transfer")
#disks = get_disks()
#cm = CopyManager(disks)
#cm.start()
#for request in requests:
#    cm.enqueue(request)

if __name__ == "__main__":
    FORMAT = "[%(levelname)s - %(asctime)s - %(filename)s:%(lineno)s] %(message)s"
    logging.basicConfig(format=FORMAT, level=logging.WARNING)
    parser = optparse.OptionParser()
    parser.add_option('--db', type=str, help="SQLA DB connection string", dest="db", default=None)
    parser.add_option('--file', type=str, help="Source/Dest file", dest="file", default=None)
    parser.add_option('--map', type=str, help="Pre-existing mapping database", dest="mapping_db", default=DEFAULT_SQLITE_DB)
    parser.add_option('--log_level', type=str, help="Logging level", dest="log", default="info")
    opts, args = parser.parse_args()
    log.setLevel(opts.log.upper())

    if not opts.db and not opts.file:
        raise Exception("Must specify one of either --db or --file")
    elif opts.db and opts.file:
        raise Exception("--db and --file are mutually exclusive arguments")

    disks = get_disks()
    copy_manager = CopyManager(disks)
    copy_manager.start()
    if opts.db:
        db_conn = DataBaseConn(opts.db)
        copy_requests = generate_requests_from_db(db_conn)
        copy_requests = create_mappings(disks, copy_requests, opts.mapping_db)
        for request in copy_requests:
            copy_manager.enqueue(request)
        db_updater = DBUpdater(db_conn, copy_manager._output_queue)
        db_updater.start()
        def terminate(*args, **kwargs):
            copy_manager.terminate()
            db_updater.join()
        signal.signal(signal.SIGINT, terminate)
        copy_manager.wait()
        db_updater.join()

    elif opts.file:
        copy_requests = generate_requests_from_file(opts.file)
        copy_requests = create_mappings(disks, copy_requests, opts.mapping_db)
        for request in copy_requests:
            copy_manager.enqueue(request)
        signal.signal(signal.SIGINT, lambda *args, **kwargs: copy_manager.terminate())
        copy_manager.wait()







