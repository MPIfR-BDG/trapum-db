import logging
import argparse
import time
import tarfile
import os
import io
from functools import wraps
from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool
from trapum_db import DataProduct, Target, Pointing, Beam

log = logging.getLogger('trapum_db.file_actions')

PICS_FILE = "pics_original_descending_scores.txt"

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


class TrapumCandidateSelector(object):
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
    def extract_all(self, tarballs, pics_score=0.0, output_tar=None):
        candidates = []
        if output_tar:
            output = tarfile.open(output_tar, mode='w:gz')
        else:
            output = None
        n = len(tarballs)
        for ii, (tarball, source, pid, bid, name, utc) in enumerate(tarballs):
            for fname, pics_score in self.extract(tarball, pics_score, output):
                candidates.append([tarball, source, pid, bid, name, utc, fname, pics_score])
            log.info("Completed {} of {} tarballs".format(ii+1, n))
        if output:
            string = io.BytesIO()
            for row in candidates:
                string.write(",".join(row) + "\n")
            string.seek(0)
            info = tarfile.TarInfo(name="overview.csv")
            info.size = len(string.buf)
            output.addfile(tarinfo=info, fileobj=string)
            output.close()
        log.info("Extracted a total of {} candidates".format(
            len(candidates)))
        return candidates

    @Timer.track
    def extract(self, tarball, pics_score=0.0, output=None):
        log.info("Extracting: {}".format(tarball))
        extracted_candidates = []
        with tarfile.open(tarball) as f:
            basename = f.getmembers()[0].name.split("/")[0]
            pics_file_member = None
            for member in f.getmembers():
                if PICS_FILE in member.name:
                    pics_file_member = member
                    break
            else:
                log.warning("No PICs file detected in tarball: {}".format(
                    tarball))
                return extracted_candidates
            with f.extractfile(pics_file_member) as fo:
                for line in fo.readlines():
                    pfd, score = line.split()
                    if float(score) > pics_score:
                        pfd = pfd.decode()
                        png_name = "{}/{}.png".format(
                            basename, pfd.split("/")[-1])
                        if output:
                            m = f.getmember(png_name)
                            output.addfile(m, f.extractfile(m))
                        extracted_candidates.append((png_name, score))
        log.info("Found {} candidates above PICs score {}".format(
            len(extracted_candidates), pics_score))
        return extracted_candidates

    @Timer.track
    def find_tarballs(self, pointings):
        with self.session() as session:
            tarballs = session.query(
                    DataProduct.filepath,
                    DataProduct.filename,
                    Target.source_name,
                    Pointing.id.label("pointing_id"),
                    Beam.id.label("beam_id"),
                    Beam.name,
                    Pointing.utc_start
                ).join(
                    Pointing, Pointing.id == DataProduct.pointing_id
                ).join(
                    Beam, Beam.id == DataProduct.beam_id
                ).join(
                    Target
                ).filter(
                    DataProduct.pointing_id.in_(pointings),
                    DataProduct.file_type_id == 25
                ).all()
        log.info("Found {} tarballs for pointings: {}".format(len(tarballs), pointings))
        return [(os.path.join(i.filepath, i.filename), i.source_name, i.pointing_id, i.beam_id, i.name, i.utc_start) for i in tarballs]


if __name__ == "__main__":
    FORMAT = "[%(levelname)s - %(asctime)s - %(filename)s:%(lineno)s] %(message)s"
    logging.basicConfig(format=FORMAT, level=logging.DEBUG)
    parser = argparse.ArgumentParser(description="Script for extracting candidate PNGs from TRAPUM tarballs")
    parser.add_argument('--db', type=str, help="SQLA DB connection string", dest="db")
    parser.add_argument('--pics-score', type=float, help="Extract candidate above this score", dest="pics_score", default=0.0)
    parser.add_argument('--output-tar', type=str, help="Extract to new tarfile", dest="output_tar", default=None)
    parser.add_argument('--pointings', type=int, nargs='+', help="Pointing ID to extract", dest="pointing_ids")
    parser.add_argument('--log-level', type=str, help="Logging level", dest="log", default="info")
    opts = parser.parse_args()
    if len(opts.pointing_ids) == 0:
        raise Exception("No --pointing value passed")
    log.setLevel(opts.log.upper())
    selector = TrapumCandidateSelector(
        opts.db)

    #TMP_POINTINGS = [149 150 151 152 153 154 157 158 160 161 162 163 164 165 166 167 168 169 170 213 214 215 216 217 218 219 220 221 222 223 224 225 226 227 228 229 230 231 232 126 131 132 133 134 135 136 137 138 139 144 140 141 142 143 145 146 147 148]

    tarballs = selector.find_tarballs(opts.pointing_ids)
    candidates = selector.extract_all(
        tarballs,
        pics_score=opts.pics_score,
        output_tar=opts.output_tar)











