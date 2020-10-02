import logging
import optparse
import time
import datetime
from functools import wraps
from contextlib import contextmanager
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool
from trapum_db import FileAction, FileActionRequest

log = logging.getLogger('trapum_db.file_actions')


class TimeTracker(object):
    def __init__(self):
        self.count = 0
        self.elapsed = 0.0

    def add(self, duration):
        self.count += 1
        self.elapsed += duration


class Timer(object):
    trackers = {}

    @staticmethod
    def track(func):
        @wraps(func)
        def wrapped(*args, **kwargs):
            start = time.time()
            retval = func(*args, **kwargs)
            duration = time.time() - start
            name = func.__name__
            if name in Timer.trackers.keys():
                Timer.trackers[name].add(duration)
            else:
                t = TimeTracker()
                t.add(duration)
                Timer.trackers[name] = t
            return retval
        return wrapped

    @staticmethod
    def summary():
        for f, tracker in Timer.trackers.items():
            print("Func: {}, Called: {}, Total: {} s, Avg: {} s".format(
                f, tracker.count, tracker.elapsed,
                tracker.elapsed/tracker.count))


class TrapumFileActions(object):
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

    @Timer.track
    def handle_requests(self, valid_actions=None):
        with self.session() as session:
            query = session.query(
                    FileActionRequest
                ).join(
                    FileAction
                ).add_columns(
                    FileAction.action,
                    FileAction.is_destructive
                )
            if valid_actions:
                query = query.filter(
                    FileAction.action.in_(valid_actions)
                )
            requests = query.filter(
                    FileAction.success.is_(None)
                ).order_by(
                    func.asc(FileActionRequest.requested_at)
                ).all()
            log.info("Found {} pending requests".format(len(requests)))
            for request, action, data_product in requests:
                fullpath = "/".join([data_product.filepath,data_product.filename])
                log.debug("Handling request ID {} (action={}, destructive={}, dp={})".format(
                    request.id, action.action, bool(action.is_destructive), fullpath))
                handler = getattr(self, action.action, self.unknown_action)
                handler(request, action, data_product)

    @Timer.track
    def delete(self, request, action, data_product):
        pass

    @Timer.track
    def noop(self, request, action, data_product):
        log.debug("Executing NO-OP handling")
        request.completed_at = datetime.datetime.utcnow()
        request.success = 1

    @Timer.track
    def migrate(self, request, action, data_product):
        pass

    @Timer.track
    def compress(self, request, action, data_product):
        pass

    @Timer.track
    def decompress(self, request, action, data_product):
        pass

    @Timer.track
    def unknown_action(self, request, action, data_product):
        pass


if __name__ == "__main__":
    FORMAT = "[%(levelname)s - %(asctime)s - %(filename)s:%(lineno)s] %(message)s"
    logging.basicConfig(format=FORMAT, level=logging.DEBUG)
    parser = optparse.OptionParser()
    parser.add_option('--db', type=str, help="SQLA DB connection string", dest="db")
    parser.add_option('--log_level', type=str, help="Logging level", dest="log", default="info")
    opts, args = parser.parse_args()
    log.setLevel(opts.log.upper())
    uploader = TrapumFileActions(opts.db)











