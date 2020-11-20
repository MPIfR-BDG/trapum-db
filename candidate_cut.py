import logging
import argparse
import time
import tarfile
import os
import io
import subprocess
import shlex
import glob
import sys
import cPickle
from functools import wraps
from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool
from trapum_db import DataProduct, Target, Pointing, Beam

sys.path.append('/home/psr')
import ubc_AI
from ubc_AI.data import pfdreader
AI_PATH = '/'.join(ubc_AI.__file__.split('/')[:-1]) + '/trained_AI/'
MODELS = ["clfl2_trapum_Ter5.pkl", "clfl2_PALFA.pkl"]
MODELS = [os.path.join(AI_PATH, m) for m in MODELS]
#MODELS = glob.glob("/home/psr/ubc_AI/trained_AI/*.pkl")
LOADED_MODELS = []
for MODEL in MODELS:
    with open(MODEL, 'rb') as f:
        LOADED_MODELS.append(cPickle.load(f))

log = logging.getLogger('trapum_db.candidate_selector')

PICS_FILE = "pics_scores.txt"


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
            for fname, scores in self.extract(tarball, pics_score, output):
                candidates.append([tarball, source, pid, bid, name, utc, fname] + scores)
            log.info("Completed {} of {} tarballs".format(ii+1, n))
        if output:
            string = io.BytesIO()
            string.write("#tarball #source #pid #bid #bname #utc #fname {}\n".format(
		"".join(["#{} ".format(i.split("/")[-1]) for i in MODELS])))
            for row in candidates:
                string.write(",".join(map(str, row)) + "\n")
            nbytes = string.tell()
            string.seek(0)
            info = tarfile.TarInfo(name="overview.csv")
            info.size = nbytes
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
            fo = f.extractfile(pics_file_member)
            for line in fo.readlines():
                if line.startswith("#"):
                    continue
                split = line.split(",")
                pfd = split[0]
                scores = map(float, split[1:])

                if any([float(score) >= pics_score for score in scores]):
                    pfd = pfd.decode()
                    png_name = "{}/{}.png".format(
                        basename, pfd.split("/")[-1])
                    if output:
                        try:
                            m = f.getmember(png_name)
                        except:
                            continue
                        else:
                            output.addfile(m, f.extractfile(m))
                    extracted_candidates.append((png_name, scores))
        fo.close()
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
        log.info("Found {} tarballs for pointings: {}".format(
            len(tarballs), pointings))
        return [(os.path.join(i.filepath, i.filename), i.source_name, i.pointing_id, i.beam_id, i.name, i.utc_start) for i in tarballs]


def tar_filter(extensions):
    def filter_func(info):
        if any([info.name.endswith(ex) for ex in extensions]):
            return None
        else:
            return info
    return filter_func


def parse_pdmp_stdout(stream):
    for line in stream.splitlines():
        if line.startswith("Best DM"):
            dm = float(line.split()[3])
            break
    else:
        raise Exception("no best DM")
    for line in stream.splitlines():
        if line.startswith("Best TC Period"):
            tc = float(line.split()[5])
            break
    else:
        raise Exception("no best TC period")
    return tc, dm


def get_dm_info(ar):
    beam, utc, _, _, cand_id, _, orig_dm, _, orig_acc, orig_period, _ = ar.split("/")[-1].split("_")
    fname, bwidth, opt_dm = subprocess.check_output("psrstat -Q -c bwidth -c dm {}".format(ar), shell=True).strip().split()
    orig_dm = float(orig_dm)
    bwidth = float(bwidth)
    opt_dm = float(opt_dm)
    pdmp_dm_range = bwidth * 9532
    npdmp_ranges = abs(orig_dm - opt_dm) / pdmp_dm_range
    return orig_dm, opt_dm, npdmp_ranges


def reinstall_dms(tarball, out_tarball):
    log.info("reinstalling DMs on {}".format(tarball))
    with tarfile.open(tarball, "r:gz") as f:
        members = f.getmembers()
        ar_members = [member for member in members if member.name.endswith(b".ar")]
        f.extractall()
        basename = ar_members[0].name.split("/")[0]
        for member in ar_members:
            log.debug("Testing {}".format(member.name))
            ar = member.name
            orig_dm, opt_dm, npdmp_ranges = get_dm_info(ar)
            log.debug("Orig DM: {},   Opt DM {},   NPDMP {}".format(orig_dm, opt_dm, npdmp_ranges))
	    if orig_dm <= 2.0:
	        log.debug("Orig DM <= 2, deleting candidate")
                os.remove(ar)
                os.remove("{}.png".format(ar))
	        continue
            if npdmp_ranges > 0.5:
                log.debug("DM test exceeds tolerance")
                log.debug("Reinstalling original DM")
                os.system("pam -d {} -m {}".format(orig_dm, ar))
                log.debug("Rerunning pdmp") 
                os.remove("{}.png".format(ar))
                cwd = os.getcwd()
                os.chdir(basename)
                tmp_ar = ar.split("/")[-1]
                tc, dm = parse_pdmp_stdout(
                    subprocess.check_output(
                        shlex.split("pdmp -mc 32 -ms 32 -g {}.png/png {}".format(tmp_ar, tmp_ar))))
                os.chdir(cwd)
                log.debug("Optimal PDMP params: DM {},  period {} ms".format(dm, tc))
                os.system("pam --period {} -d {} -m {}".format(str(tc/1000.0), dm, ar))
            else:
                os.remove(ar)
                os.remove("{}.png".format(ar))
        extract_and_score(basename)

    with tarfile.open(out_tarball, "w:gz") as f:
        f.add(basename, filter=tar_filter([".pfd", ".bestprof", ".ps", ".pfd.png"]))
    os.system("rm -rf {}".format(basename))


def convert_and_score(tarball, out_tarball):
    with tarfile.open(tarball, "r:gz") as f:
        members = f.getmembers()
        pfd_members = [member for member in members if member.name.endswith(b".pfd")]
        f.extractall()
        basename = pfd_members[0].name.split("/")[0]
        os.system("psrconv -o psrfits -e ar {}/*.pfd".format(basename))
        time.sleep(2)
        os.system("clfd --no-report {}/*.ar".format(basename))
        for member in pfd_members:
            ar_name = member.name.replace(".pfd", ".ar")
            os.system("mv {} {}".format(member.name.replace(".pfd", ".ar.clfd"), ar_name))
        cwd = os.getcwd()
        os.chdir("{}/{}".format(cwd, basename))
        for ar in glob.glob("*.ar"):
            log.info("pdmp {}".format(ar))
            tc, dm = parse_pdmp_stdout(subprocess.check_output(shlex.split("pdmp -mc 32 -ms 32 -g {}.png/png {}".format(ar, ar))))
            os.system("pam --period {} -d {} -m {}".format(str(tc/1000.0), dm, ar))
        os.chdir(cwd)
        extract_and_score(basename)

    with tarfile.open(out_tarball, "w:gz") as f:
        f.add(basename, filter=tar_filter([".pfd", ".bestprof", ".ps", ".pfd.png"]))
    os.system("rm -rf {}".format(basename))


def extract_and_score(path):
    # Load model
    classifiers = LOADED_MODELS
    # Find all files
    arfiles = glob.glob("{}/*.ar".format(path))
    log.info("Retrieved {} archive files from {}".format(
        len(arfiles), path))
    if len(arfiles) == 0:
        return
    scores = []
    readers = [pfdreader(f) for f in arfiles]
    for classifier in classifiers:
        scores.append(classifier.report_score(readers))
    log.info("Scored with all models")
    combined = sorted(zip(arfiles, *scores), reverse=True, key=lambda x: x[1])
    log.info("Sorted scores...")
    names = "\t".join(["#{}".format(model.split("/")[-1]) for model in MODELS])
    with open("{}/pics_scores.txt".format(path), "w") as fout:
        fout.write("#arfile\t{}\n".format(names))
        for row in combined:
            scores = ",".join(map(str, row[1:]))
            fout.write("{},{}\n".format(row[0], scores))
    log.info("Written to file in {}".format(path))


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
    new_tarballs = []   
    for ii, tarball in enumerate(tarballs):
        log.info("Rescoring/reinstalling tarball {} of {}".format(ii+1, len(tarballs)))
        tarball = list(tarball) 
        tarname = tarball[0]
        new_tarname = tarname.replace(".tar.gz", "_rescore.tar.gz")
        if not os.path.isfile(new_tarname): 
            convert_and_score(tarname, new_tarname)
        else:
            log.info("Skipping generation of {} as it already exists".format(new_tarname))
        dm_reinstall_tarname = new_tarname.replace("_rescore.tar.gz", "_redm.tar.gz")
	if not os.path.isfile(dm_reinstall_tarname):
	    reinstall_dms(new_tarname, dm_reinstall_tarname)
        else:
            log.info("Skipping generation of {} as it already exists".format(dm_reinstall_tarname))
        new_row = [dm_reinstall_tarname]
        new_row.extend(tarball[1:])
        new_tarballs.append(new_row)

    print(tarballs)
  
    candidates = selector.extract_all(
        new_tarballs,
        pics_score=opts.pics_score,
        output_tar=opts.output_tar)











