
from datetime import datetime, timedelta
from glob import glob
import os,sys
import shutil
from flo.computation import Computation
from flo.time import round_datetime
from flo.subprocess import check_call
from flo.util import augmented_env, symlink_inputs_to_working_dir
from flo.config import config
from flo.product import StoredProductCatalog
from flo.ingest import IngestCatalog

from flo.sw.hirs import HIRS
#from flo.sw.hirs.util import generate_cfsr_bin
from flo.sw.hirs_avhrr import HIRS_AVHRR
from flo.sw.hirs_csrb_monthly import HIRS_CSRB_MONTHLY
from flo.sw.hirs.delta import delta_catalog

# every module should have a LOG object
import logging, traceback
LOG = logging.getLogger(__file__)

ingest_catalog = IngestCatalog('PEATE')
SPC = StoredProductCatalog()


class HIRS_CTP_ORBITAL(Computation):

    parameters = ['granule', 'sat', 'hirs_version', 'collo_version', 'csrb_version', 'ctp_version']
    outputs = ['out']

    def build_task(self, context, task):
        LOG.debug("Running build_task()") # GPC
        LOG.debug("context:  {}".format(context)) # GPC
        LOG.debug("Initial task.inputs:  {}".format(task.inputs)) # GPC

        # HIRS L1B Input
        hirs_context = context.copy()
        [hirs_context.pop(k) for k in ['collo_version', 'csrb_version', 'ctp_version']]
        LOG.debug("hirs_context:  {}".format(hirs_context)) # GPC
        task.input('HIR1B', HIRS().dataset('out').product(hirs_context))


        # Collo Input
        collo_context = hirs_context
        collo_context['collo_version'] = context['collo_version']
        LOG.debug("collo_context:  {}".format(collo_context)) # GPC
        task.input('COLLO', HIRS_AVHRR().dataset('out').product(collo_context))

        # CSRB Monthly Input
        csrb_context = collo_context
        csrb_context['csrb_version'] = context['csrb_version']
        csrb_context['granule'] = datetime(context['granule'].year, context['granule'].month, 1)
        LOG.debug("csrb_context:  {}".format(csrb_context)) # GPC
        task.input('CSRB', HIRS_CSRB_MONTHLY().dataset('zonal_means').product(csrb_context))

        # PTMSX Input
        LOG.debug("Searching for PTMSX file for granule  {}".format(context['granule'])) # GPC
        ptmsx_files = delta_catalog.file('avhrr', context['sat'],'PTMSX', context['granule'])
        task.input('PTMSX',ptmsx_files)

        # CFSR Input
        cfsr_granule = round_datetime(context['granule'], timedelta(hours=6))
        LOG.debug("cfsr_granule:  {}".format(cfsr_granule)) # GPC
        cfsr_file = self.get_cfsr(cfsr_granule)
        task.input('CFSR', cfsr_file)

        LOG.debug("Final task.inputs...") # GPC
        for task_key in task.inputs.keys():
            LOG.debug("\t{}: {}".format(task_key,task.inputs[task_key])) # GPC

        for task_key in ['HIR1B','COLLO','CSRB']:
            if task_key in task.inputs.keys():
                LOG.debug("{} file: {}".format(task_key,SPC.file(task.inputs[task_key]).path))
        for task_key in ['CFSR']:
            if task_key in task.inputs.keys():
                LOG.debug("{} file: {}".format(task_key,task.inputs[task_key].path))

        LOG.debug("Exiting build_task()...") # GPC


    def run_task(self, inputs, context):
        LOG.debug("Running run_task()") # GPC

        debug = 0
        shifted_FM_opt = 2

        # Inputs
        inputs = symlink_inputs_to_working_dir(inputs)
        lib_dir = os.path.join(self.package_root, context['ctp_version'], 'lib')

        LOG.debug("inputs :  {}".format(inputs)) # GPC

        # Output Name
        output = 'ctp.orbital.{}.{}.nc'.format(context['sat'], inputs['HIR1B'][12:30])
        LOG.debug("output :  {}".format(output)) # GPC

        # Copy coeffs to working directory
        [shutil.copy(f, './')
         for f
         in glob(os.path.join(self.package_root, context['ctp_version'], 'coeffs/*'))]

        # Generating CFSR Binaries
        cfsr_bin_files = self.generate_cfsr_bin(os.path.join(self.package_root, context['ctp_version']))
        LOG.debug("cfsr_bin_files :  {}".format(cfsr_bin_files)) # GPC

        # Running CTP Orbital
        cmd = os.path.join(self.package_root, context['ctp_version'],
                           'bin/process_hirs_cfsr.exe')
        cmd += ' {} {}.bin {}'.format(inputs['HIR1B'], inputs['CFSR'], inputs['COLLO'])
        cmd += ' {} {}'.format(inputs['PTMSX'], inputs['CSRB'])
        cmd += ' {}'.format(os.path.join(self.package_root, context['ctp_version'],
                                         'CFSR_lst.bin'))
        cmd += ' {} {} {}'.format(debug, shifted_FM_opt, output)

        LOG.debug(cmd)
        check_call(cmd, shell=True, env=augmented_env({'LD_LIBRARY_PATH': lib_dir}))

        return {'out': output}


    def get_cfsr(self,cfsr_granule):

        num_cfsr_files = 0

        # Search for the old style pgbhnl.gdas.*.grb2 file from the PEATE
        if num_cfsr_files == 0:
            LOG.debug("Trying to retrieve pgbhnl.gdas.*.grb2 CFSR files from PEATE...") # GPC
            try:
                cfsr_file = ingest_catalog.file('CFSR_PGRBHANL',cfsr_granule)
                num_cfsr_files = len(cfsr_file)
                if num_cfsr_files != 0:
                    LOG.debug("\tpgbhnl.gdas.*.grb2 CFSR files from PEATE : {}".format(cfsr_file)) # GPC
            except Exception, err :
                #LOG.error("{}.".format(err))
                LOG.debug("\tRetrieval of pgbhnl.gdas.*.grb2 CFSR file from PEATE failed") # GPC

        # Search for the new style cdas1.*.t*z.pgrbhanl.grib2 file from PEATE
        #num_cfsr_files = 0
        if num_cfsr_files == 0:
            LOG.debug("Trying to retrieve cdas1.*.t*z.pgrbhanl.grib2 CFSR file from PEATE...") # GPC
            try:
                cfsr_file = ingest_catalog.file('CFSV2_PGRBHANL',cfsr_granule)
                num_cfsr_files = len(cfsr_file)
                if num_cfsr_files != 0:
                    LOG.debug("\tcdas1.*.t*z.pgrbhanl.grib2 CFSR file from PEATE : {}".format(cfsr_file)) # GPC
            except Exception, err :
                #LOG.error("{}.".format(err))
                LOG.debug("\tRetrieval of cdas1.*.t*z.pgrbhanl.grib2 CFSR file from PEATE failed") # GPC

        # Search for the old style pgbhnl.gdas.*.grb2 file from the file list
        #num_cfsr_files = 0
        #if num_cfsr_files == 0:
            #LOG.debug("Trying to retrieve pgbhnl.gdas.*.grb2 CFSR file from DELTA...") # GPC
            #try:
                #cfsr_file = delta_catalog.file('ancillary', 'NONE', 'CFSR', cfsr_granule)
                #num_cfsr_files = len(cfsr_file)
                #if num_cfsr_files != 0:
                    #LOG.debug("pgbhnl.gdas.*.grb2 CFSR file from DELTA : {}\n".format(cfsr_file)) # GPC
            #except Exception, err :
                #LOG.error("{}.".format(err))
                #LOG.warn("\tRetrieval of pgbhnl.gdas.*.grb2 CFSR file from DELTA failed\n") # GPC


        #LOG.info("We've found {} CFSR file for context {}".format(len(cfsr_file),cfsr_granule)) # GPC

        return cfsr_file


    def generate_cfsr_bin(self,package_root):

        shutil.copy(os.path.join(package_root, 'bin/wgrib2'), './')

        # Search for the old style pgbhnl.gdas.*.grb2 files
        LOG.debug("Searching for pgbhnl.gdas.*.grb2 ...")
        files = glob('pgbhnl.gdas.*.grb2')
        LOG.debug("... found {}".format(files))

        # Search for the new style cdas1.*.t*z.pgrbhanl.grib2
        if len(files)==0:
            LOG.debug("Searching for cdas1.*.pgrbhanl.grib2 ...")
            files = glob('cdas1.*.pgrbhanl.grib2')
            LOG.debug("... found {}".format(files))

        LOG.debug("CFSR files: {}".format(files)) # GPC

        new_cfsr_files = []
        for file in files:
            cmd = os.path.join(package_root, 'bin/extract_cfsr.csh')
            cmd += ' {} {}.bin ./'.format(file, file)

            LOG.debug(cmd)

            try:
                check_call(cmd, shell=True)
                new_cfsr_files.append('{}.bin'.format(file))
            except:
                pass

        return new_cfsr_files


    def find_contexts(self, sat, hirs_version, collo_version, csrb_version, ctp_version,
                      time_interval):

        LOG.debug("Running find_contexts()") # GPC
        files = delta_catalog.files('hirs', sat, 'HIR1B', time_interval)
        return [{'granule': file.data_interval.left,
                 'sat': sat,
                 'hirs_version': hirs_version,
                 'collo_version': collo_version,
                 'csrb_version': csrb_version,
                 'ctp_version': ctp_version}
                for file in files
                if file.data_interval.left >= time_interval.left]

    def context_path(self, context, output):

        LOG.debug("Running context_path()") # GPC
        return os.path.join('HIRS',
                            '{}/{}'.format(context['sat'], context['granule'].year),
                            'CTP_ORBITAL')
