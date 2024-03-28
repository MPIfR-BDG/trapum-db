import logging
import argparse
import tarfile
import os
import io
import time
import pandas
from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool
from trapum_db import DataProduct, Target, Pointing, Beam, Processing, ProcessingRequest

log = logging.getLogger("candidate_cut_pulsarx")

class FileNotFound(Exception):
    pass


def info_from_member(name, member):
    info = tarfile.TarInfo(name=name)
    info.size = member.size
    info.mtime = member.mtime
    info.chksum = member.chksum
    return info


class CandidateTarArchive(object):
    HEADER_LENGTH = 11

    def __init__(self, filepath, source, pointing_id,
        beam_id, beam_name, utc_start):
        self.filepath = filepath
        self.source = source
        self.pointing_id = pointing_id
        self.beam_id = beam_id
        self.beam_name = beam_name
        self.utc_start = utc_start.strftime("%Y-%m-%dT%H:%M:%S")
        self.candidates = None
        self.header = None
        self.metafile = None
        self.metaname = None
        self.csv_file = None
        self.basename = None
        self._file = None

    def __repr__(self):
        return "<CandidateTarArchive ({}, {}, {}, {})>".format(
            self.filepath, self.source, self.utc_start, self.beam_name)

    def open(self):
        self._file = tarfile.open(self.filepath, mode="r:gz")

    def close(self):
        if self._file:
            self._file.close()

    def _read_header(self, csv_file):
        header = {}
        for _ in range(self.HEADER_LENGTH):
            line = csv_file.readline()
            key, value = line.strip()[1:].split()
            header[key.lower()] = value
        return header

    def read_metadata(self):
        for member in self._file.getmembers():
            if member.name.endswith(".meta"):
                self.metafile = member
	        split_name = member.name.split("/")
                if len(split_name) > 1:
                    self.basename = split_name[0]
                else:
                    self.basename = ""
                log.debug("Found metafile member: {}".format(self.metafile))
                log.debug("Basename: {}".format(self.basename))
            elif member.name.endswith(".csv"):
                self.csv_file = member
                log.debug("Found CSV member: {}".format(self.csv_file))
        if not self.metafile:
            raise FileNotFound("Pointing metafile")
        elif not self.csv_file:
            raise FileNotFound("Candidate CSV file")
        csv_file = self._file.extractfile(self.csv_file)
        self.header = self._read_header(csv_file)
        self.candidates = pandas.read_csv(csv_file)

    def extract_pngs(self, pngs, dest):
        log.debug("Extracting PNGs")
        for png_path_new, png_path in pngs:
            member = self._file.getmember(png_path)
            info = info_from_member(png_path_new, member)
            info.size = member.size
            dest.addfile(info, self._file.extractfile(member))

    def extract_metafile(self, name, dest):
        log.debug("Extracting metafile")
        info = info_from_member(name, self.metafile)
        dest.addfile(info, self._file.extractfile(self.metafile))


class AggregatedCandfile(object):
    KEYS = ["pointing_id", "beam_id", "beam_name", "source_name",
            "ra", "dec", "gl", "gb", "mjd_start", "utc_start",
            "f0_user", "f0_opt", "f0_opt_err", "f1_user", "f1_opt",
            "f1_opt_err", "acc_user", "acc_opt", "acc_opt_err",
            "dm_user", "dm_opt", "dm_opt_err", "sn_fft", "sn_fold",
            "pepoch", "maxdm_ymw16", "dist_ymw16", "pics_trapum_ter5",
            "pics_palfa", "png_path", "metafile_path", "filterbank_path",
            "candidate_tarball_path"]

    def __init__(self, fname):
        self._metafiles = set()
        self._csv_file = io.BytesIO()
        self._csv_file.write("{}\n".format(",".join(self.KEYS)).encode())
        self._tar_file = tarfile.open(fname, mode="w:gz")
        self._counter = 0
        self._metafiles = set()

    def finalise(self):
        log.info("Finalising the archive and writing to disk")
        info = tarfile.TarInfo(name="candidates.csv")
        info.mtime = time.time()
        info.size = self._csv_file.tell()
        self._csv_file.seek(0)
        self._tar_file.addfile(info, self._csv_file)
        self._tar_file.close()

    def add_candidates(self, cand_archive, pics_cut, dm_cut, sn_cut):
        cand_archive.open()
        cand_archive.read_metadata()
        candidates = cand_archive.candidates
        subset = candidates[ (candidates["dm_new"] > dm_cut) &
                             ((candidates["pics_TRAPUM_Ter5"] > pics_cut) |
                              (candidates["pics_PALFA"] > pics_cut)) &
                              (candidates["S/N_new"] > sn_cut)]
        if len(subset) == 0:
            log.info("No candidates to extract")
            return
        pngs = []
        for _, candidate in subset.iterrows():
            png_path = os.path.join(
                cand_archive.basename,
                os.path.basename(candidate["png_file"]))
            new_png_path = os.path.join("plots", "{}_{}_{}_{:08d}.png".format(
                cand_archive.utc_start, cand_archive.source,
                cand_archive.beam_name, self._counter))
            metafile_path = os.path.join(
                cand_archive.basename,
                os.path.basename(cand_archive.metafile.name))
            new_metafile_path = os.path.join("metafiles", os.path.basename(
                cand_archive.metafile.name))
            self._counter += 1

            data = {}
            # -- Database info --
            data["pointing_id"] = cand_archive.pointing_id
            data["beam_id"] = cand_archive.beam_id
            data["beam_name"] = cand_archive.beam_name
            # -- Observation details --
            data["source_name"] = cand_archive.source
            data["ra"] = cand_archive.header[b"ra"].decode()
            data["dec"] = cand_archive.header[b"dec"].decode()
            data["gl"] = cand_archive.header[b"gl"].decode()
            data["gb"] = cand_archive.header[b"gb"].decode()
            data["mjd_start"] = cand_archive.header[b"date"].decode()
            data["utc_start"] = cand_archive.utc_start  ##TODO check formatting
            # -- Measured parameters --
            data["f0_user"] = candidate["f0_old"]
            data["f0_opt"] = candidate["f0_new"]
            data["f0_opt_err"] = candidate["f0_err"]
            data["f1_user"] = candidate["f1_old"]
            data["f1_opt"] = candidate["f1_new"]
            data["f1_opt_err"] = candidate["f1_err"]
            data["acc_user"] = candidate["acc_old"]
            data["acc_opt"] = candidate["acc_new"]
            data["acc_opt_err"] = candidate["acc_err"]
            data["dm_user"] = candidate["dm_old"]
            data["dm_opt"] = candidate["dm_new"]
            data["dm_opt_err"] = candidate["dm_err"]
            data["sn_fft"] = candidate["S/N"]
            data["sn_fold"] = candidate["S/N_new"]
            data["pepoch"] = cand_archive.header[b"pepoch"].decode()
            # -- YMW16 parameters --
            data["maxdm_ymw16"] = cand_archive.header[b"maxdm_ymw16"].decode()
            data["dist_ymw16"] = candidate["dist_ymw16"]
            # -- Classifier scores --
            data["pics_trapum_ter5"] = candidate["pics_TRAPUM_Ter5"]
            data["pics_palfa"] = candidate["pics_PALFA"]
            # -- Data products --
            data["png_path"] = new_png_path
            data["metafile_path"] = new_metafile_path
            data["filterbank_path"] = cand_archive.header[b"filename"].decode()
            data["candidate_tarball_path"] = cand_archive.filepath
            self._csv_file.write(",".join([str(data[key]) for key in self.KEYS]).encode())
            self._csv_file.write(b"\n")
            # add PNG
            pngs.append([new_png_path, png_path])
        log.info("Exracting {} candidates that meet the cut".format(len(pngs)))
        # Extract all PNGs
        cand_archive.extract_pngs(pngs, self._tar_file)
        # Extract metadata file
        if new_metafile_path not in self._metafiles:
            cand_archive.extract_metafile(new_metafile_path, self._tar_file)
        cand_archive.close()

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

    def find_tarballs(self, prequests):
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
                    Processing, Processing.id == DataProduct.processing_id
                ).join(
                    Beam, Beam.id == DataProduct.beam_id
                ).join(
                    Target
                ).filter(
                    Processing.id.in_(prequests), 
                    DataProduct.file_type_id.in_((35, 51))
                ).all()
        log.info("Found {} tarballs for prequests: {}".format(
            len(tarballs), prequests))
        return [CandidateTarArchive(os.path.join(i.filepath, i.filename),
            i.source_name, i.pointing_id, i.beam_id, i.name, i.utc_start)
            for i in tarballs]


def extract(tar_name, tarballs):
    log.info("Extracting to {}".format(tar_name))
    agg = AggregatedCandfile(tar_name)
    n = len(tarballs)
    for ii, tarball in enumerate(tarballs):
        log.info("Parsing cand file: {}".format(repr(tarball)))
        try:
            agg.add_candidates(tarball, opts.pics_cut, opts.dm_cut, opts.sn_cut)
            log.info("Completed {} of {} extractions".format(ii+1, n))
        except Exception as error:
            with open("errors.txt", "a") as f:
                f.write("{}, {}\n".format(ii, repr(tarball)))
    agg.finalise()

if __name__ == "__main__":
    FORMAT = "[%(levelname)s - %(asctime)s - %(filename)s:%(lineno)s] %(message)s"
    logging.basicConfig(format=FORMAT, level=logging.DEBUG)
    parser = argparse.ArgumentParser(description="Script for extracting candidate PNGs from TRAPUM tarballs")
    parser.add_argument('--db', type=str, help="SQLA DB connection string", dest="db")
    parser.add_argument('--output-tar', type=str, help="Extract to new tarfile (if using --split give basename)", dest="output_tar", default=None)
    parser.add_argument('--pids', type=int, nargs='+', help="Processing IDs to extract", dest="prequest_ids")
    parser.add_argument('--pics-cut', type=float,  help="Minimim valid PICs score", dest="pics_cut", default=0.1)
    parser.add_argument('--dm-cut', type=float,  help="Minimum valid DM", dest="dm_cut", default=3.0)
    parser.add_argument('--sn-cut', type=float,  help="Minimum valid S/N", dest="sn_cut", default=7.0)   
    parser.add_argument('--split', action="store_true",  help="Split output tarball by pointing", dest="split", default=False)
    parser.add_argument('--log-level', type=str, help="Logging level", dest="log", default="info")
    opts = parser.parse_args()
    if len(opts.prequest_ids) == 0:
        raise Exception("No --pointing value passed")
    log.setLevel(opts.log.upper())
    selector = TrapumCandidateSelector(opts.db)
    tarballs = selector.find_tarballs(opts.prequest_ids)
    if not tarballs:
        raise Exception("No candidate returned")

    if opts.split:
        pointings = {}
        for tarball in tarballs:
            if tarball.pointing_id not in pointings:
                pointings[tarball.pointing_id] = []
            pointings[tarball.pointing_id].append(tarball) 	
	for pointing_id, ptarballs in pointings.items():
            out_name = "{}_{}_{}.tar.gz".format(opts.output_tar.strip(".tar.gz"), pointing_id, ptarballs[0].source) 
            extract(out_name, ptarballs) 
    else:
        extract(opts.output_tar, tarballs)            
