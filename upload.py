import json
import glob
import os
import logging
import xxhash
import optparse
import time
import datetime
from functools import wraps
from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool
from trapum_db import (
    BeamformerConfiguration, Target, Pointing,
    Beam, DataProduct, FileType)
from header_util import parseSigprocHeader, updateHeader

log = logging.getLogger('trapum_db.upload')


def parse_katpoint_string(kptarget):
    names, _, ra, dec = kptarget.split(",")
    primary_name = names.split("|")[0]
    return primary_name.strip(), ra.strip(), dec.strip()


def read_apsuse_metafile(filename):
    """
    @ brief Retrieve info from meta file in path

    @parans File path of interest

    @return all APSUSE meta info as a dictionary
    """
    with open(filename, 'r', encoding='utf-8') as json_file:
        am_config_info = json.load(json_file)
    return am_config_info


class NoMetaFileException(Exception):
    pass


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
        for func, tracker in Timer.trackers.items():
            print("Func: {}, Called: {}, Total: {} s, Avg: {} s".format(
                func, tracker.count, tracker.elapsed,
                tracker.elapsed/tracker.count))


class TrapumUploader(object):
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
    def _get_bf_config_id(self, metadata):
        bf_params = dict(
                centre_frequency=metadata['centre_frequency'],
                bandwidth=metadata['bandwidth'],
                incoherent_nchans=metadata['incoherent_nchans'],
                incoherent_tsamp=metadata['incoherent_tsamp'],
                incoherent_antennas=metadata.get('incoherent_antennas', ''),
                coherent_nchans=metadata['coherent_nchans'],
                coherent_tsamp=metadata['coherent_tsamp'],
                coherent_antennas=metadata.get('coherent_antennas', ''),
                configuration_authority=metadata.get('configuration_authority', ''),
                receiver=metadata.get('receiver', ''),
                metainfo=metadata.get('metadata', '')
            )
        with self.session() as session:
            bf_config_id = session.query(
                    BeamformerConfiguration.id
                ).filter(
                    BeamformerConfiguration.centre_frequency == bf_params['centre_frequency'],
                    BeamformerConfiguration.bandwidth == bf_params['bandwidth'],
                    BeamformerConfiguration.incoherent_nchans == bf_params['incoherent_nchans'],
                    BeamformerConfiguration.incoherent_tsamp == bf_params['incoherent_tsamp'],
                    BeamformerConfiguration.incoherent_antennas.ilike(bf_params['incoherent_antennas']),
                    BeamformerConfiguration.coherent_nchans == bf_params['coherent_nchans'],
                    BeamformerConfiguration.coherent_tsamp == bf_params['coherent_tsamp'],
                    BeamformerConfiguration.coherent_antennas.ilike(bf_params['coherent_antennas']),
                    BeamformerConfiguration.configuration_authority.ilike(bf_params['configuration_authority']),
                    BeamformerConfiguration.receiver.ilike(bf_params['receiver']),
                    BeamformerConfiguration.metainfo.ilike(bf_params['metainfo'])
                ).scalar()

            if not bf_config_id:
                bf_config = BeamformerConfiguration(**bf_params)
                session.add(bf_config)
                session.flush()
                bf_config_id = bf_config.id
        return bf_config_id

    @Timer.track
    def _get_target_id(self, kptarget):
        name, ra, dec = parse_katpoint_string(kptarget)
        target_params = dict(
            source_name=name,
            ra=ra,
            dec=dec)
        with self.session() as session:
            target_id = session.query(
                    Target.id
                ).filter(
                    Target.source_name.ilike(target_params['source_name']),
                ).scalar()
            if not target_id:
                target = Target(**target_params)
                session.add(target)
                session.flush()
                target_id = target.id
        return target_id

    @Timer.track
    def _get_pointing_id(self, target_id, bf_config_id, metadata):
        utc_start = datetime.datetime.strptime(metadata['utc_start'], '%Y/%m/%d %H:%M:%S')
        pointing_params = dict(
            target_id=target_id,
            bf_config_id=bf_config_id,
            utc_start=utc_start,
            sb_id=metadata['sb_id'],
            mkat_pid=metadata['project_name']
            )
        with self.session() as session:
            pointing_id = session.query(
                    Pointing.id
                ).filter(
                    Pointing.target_id == target_id,
                    Pointing.bf_config_id == bf_config_id,
                    Pointing.utc_start == utc_start,
                    Pointing.sb_id.like(metadata['sb_id']),
                    Pointing.mkat_pid.like(metadata['project_name'])
                ).scalar()
            if not pointing_id:
                pointing = Pointing(**pointing_params)
                session.add(pointing)
                session.flush()
                pointing_id = pointing.id
        return pointing_id

    @Timer.track
    def _get_beam_id(self, kptarget, beamname, pointing_id):
        _, ra, dec = parse_katpoint_string(kptarget)
        with self.session() as session:
            beam_id = session.query(
                    Beam.id
                ).filter(
                    Beam.pointing_id == pointing_id,
                    Beam.name.like(beamname)
                ).scalar()
            if not beam_id:
                beam = Beam(
                    pointing_id=pointing_id,
                    on_target=True,
                    ra=ra,
                    dec=dec,
                    coherent="cfbf" in beamname,
                    name=beamname
                    )
                session.add(beam)
                session.flush()
                beam_id = beam.id
        return beam_id

    @Timer.track
    def _get_file_type_id(self, name):
        with self.session() as session:
            ft_id = session.query(
                    FileType.id
                ).filter_by(
                    name=name
                ).scalar()
            if not ft_id:
                ft = FileType(
                    name=name
                    )
                session.add(ft)
                session.flush()
                ft_id = ft.id
        return ft_id

    @Timer.track
    def _generate_filehash(self, filepath):
        xx = xxhash.xxh64()
        with open(filepath, 'rb') as f:
            xx.update(f.read(10000))
            xx.update(b'{}'.format(os.path.getsize(filepath)))
        return xx.hexdigest()

    @Timer.track
    def _get_dp_id(self, pointing_id, beam_id, file_type_id, filepath, filename):
        with self.session() as session:
            dp_id = session.query(
                    DataProduct.id
                ).filter(
                    DataProduct.pointing_id == pointing_id,
                    DataProduct.beam_id == beam_id,
                    DataProduct.file_type_id == file_type_id,
                    DataProduct.filename.like(filename),
                    DataProduct.filepath.like(filepath)
                ).scalar()
            if not dp_id:
                dp = DataProduct(
                    pointing_id=pointing_id,
                    beam_id=beam_id,
                    file_type_id=file_type_id,
                    processing_id=None,
                    filename=filename,
                    filepath=filepath,
                    filehash=self._generate_filehash(
                        os.path.join(filepath, filename)),
                    available=True,
                    locked=True
                    )
                session.add(dp)
                session.flush()
                dp_id = dp.id
        return dp_id

    @Timer.track
    def _validate_header(self, filterbank):
        try:
            header = parseSigprocHeader(filterbank)
            header = updateHeader(header)
        except Exception as error:
            log.exception(str(error))
            return False
        else:
            return True

    @Timer.track
    def scrape_directory(self, path):
        metafile = os.path.join(path, 'apsuse.meta')
        if not os.path.isfile(metafile):
            log.error('No meta file found in {}'.format(path))
            raise NoMetaFileException
        else:
            log.info("Found metafile: {}".format(metafile))
        metadata = read_apsuse_metafile(metafile)
        log.debug(metadata)

        # Note the beam list is a complete dump of all possible beams
        # not of the beams that are actually recorded
        beams = sorted(list(metadata['beams'].keys()))
        valid_beams = [beam for beam in beams if os.path.isdir(
            os.path.join(path, beam))]
        log.info("Found valid paths for {} beams".format(len(valid_beams)))
        bf_config_id = self._get_bf_config_id(metadata)

        target_ids = {}
        pointing_ids = {}

        cb_filetype = "filterbank-raw-{}-{}us-0dm".format(
            metadata['coherent_nchans'],
            int(metadata['coherent_tsamp']*1e6))
        cb_filetype_id = self._get_file_type_id(cb_filetype)
        log.info("CB file type: {}".format(cb_filetype))
        ib_filetype = "filterbank-raw-{}-{}us-0dm".format(
            metadata['incoherent_nchans'],
            int(metadata['incoherent_tsamp']*1e6))
        log.info("IB file type: {}".format(ib_filetype))
        ib_filetype_id = self._get_file_type_id(ib_filetype)

        for beamname in valid_beams:
            log.info("Handling beam: {}".format(beamname))
            filepath = os.path.join(path, beamname)
            target_str = metadata['beams'][beamname]
            if target_str in target_ids.keys():
                target_id = target_ids[target_str]
            else:
                target_id = self._get_target_id(target_str)
                target_ids[target_str] = target_id
            log.info("Target ID: {}".format(target_id))
            if target_str in pointing_ids.keys():
                pointing_id = pointing_ids[target_str]
            else:
                pointing_id = self._get_pointing_id(
                    target_id, bf_config_id, metadata)
                pointing_ids[target_str] = pointing_id
            log.info("Pointing ID: {}".format(pointing_id))
            beam_id = self._get_beam_id(target_str, beamname, pointing_id)
            log.info("Beam ID: {}".format(beam_id))
            coherent = "cfbf" in beamname
            if coherent:
                file_type_id = cb_filetype_id
            else:
                file_type_id = ib_filetype_id

            log.info("Finding associated filterbank files under ".format(filepath))
            filterbanks = glob.glob('{}/*.fil'.format(filepath))
            log.info("Found {} files".format(len(filterbanks)))
            for filterbank in filterbanks:
                log.info("Handing file: {}".format(filterbank))
                filename = os.path.basename(filterbank)
                if not self._validate_header(filterbank):
                    continue
                dp_id = self._get_dp_id(
                    pointing_id, beam_id, file_type_id,
                    filepath, filename)
                log.info("Uploaded data product ID: {}".format(dp_id))


if __name__ == "__main__":
    FORMAT = "[%(levelname)s - %(asctime)s - %(filename)s:%(lineno)s] %(message)s"
    logging.basicConfig(format=FORMAT, level=logging.DEBUG)
    parser = optparse.OptionParser()
    parser.add_option('--pd', type=str, help="parent directory path", dest="path")
    parser.add_option('--db', type=str, help="SQLA DB connection string", dest="db")
    parser.add_option('--log_level', type=str, help="Logging level", dest="log", default="info")
    opts, args = parser.parse_args()
    log.setLevel(opts.log.upper())
    uploader = TrapumUploader(opts.db)
    uploader.scrape_directory(opts.path)
