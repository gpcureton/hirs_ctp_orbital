#!/usr/bin/env python
# encoding: utf-8
"""

Purpose: Run the hirs_ctp_orbital package

Copyright (c) 2015 University of Wisconsin Regents.
Licensed under GNU GPLv3.
"""

import os
from os.path import basename, dirname, curdir, abspath, isdir, isfile, exists, splitext, join as pjoin
import sys
from glob import glob
import shutil
import logging
import traceback

from flo.computation import Computation
from flo.builder import WorkflowNotReady
from timeutil import TimeInterval, datetime, timedelta, round_datetime
from flo.util import augmented_env, symlink_inputs_to_working_dir
from flo.product import StoredProductCatalog

import sipsprod
from glutil import (
    check_call,
    dawg_catalog,
    #delivered_software,
    #support_software,
    #runscript,
    #prepare_env,
    #nc_gen,
    nc_compress,
    reraise_as,
    #set_official_product_metadata,
    FileNotFound
)
import flo.sw.hirs as hirs
import flo.sw.hirs_avhrr as hirs_avhrr
import flo.sw.hirs_csrb_monthly as hirs_csrb_monthly
from flo.sw.hirs.delta import DeltaCatalog

# every module should have a LOG object
LOG = logging.getLogger(__name__)

SPC = StoredProductCatalog()

def set_input_sources(input_locations):
    global delta_catalog
    delta_catalog = DeltaCatalog(**input_locations)

class HIRS_CTP_ORBITAL(Computation):

    parameters = ['granule', 'sat', 'hirs_version', 'collo_version', 'csrb_version', 'ctp_version']
    outputs = ['out']

    @reraise_as(WorkflowNotReady, FileNotFound, prefix='HIRS_CTP_ORBITAL')
    def build_task(self, context, task):
        '''
        Build up a set of inputs for a single context
        '''
        global delta_catalog

        LOG.debug("Running build_task()")
        LOG.debug("context:  {}".format(context))

        # Initialize the hirs and hirs_avhrr modules with the data locations
        hirs.delta_catalog = delta_catalog
        hirs_avhrr.delta_catalog = delta_catalog

        # Instantiate the hirs, hirs_avhrr and hirs_csrb_monthly computations
        hirs_comp = hirs.HIRS()
        hirs_avhrr_comp = hirs_avhrr.HIRS_AVHRR()
        hirs_csrb_monthly_comp = hirs_csrb_monthly.HIRS_CSRB_MONTHLY()

        # HIRS L1B Input
        hirs_context = context.copy()
        [hirs_context.pop(k) for k in ['collo_version', 'csrb_version', 'ctp_version']]
        LOG.debug("hirs_context:  {}".format(hirs_context))
        hirs_prod = hirs_comp.dataset('out').product(hirs_context)

        if SPC.exists(hirs_prod):
            task.input('HIR1B', hirs_prod)
        else:
            raise WorkflowNotReady('No HIRS inputs available for {}'.format(hirs_context['granule']))

        # Collo Input
        hirs_avhrr_context = hirs_context
        hirs_avhrr_context['collo_version'] = context['collo_version']
        LOG.debug("hirs_avhrr_context:  {}".format(hirs_avhrr_context))
        hirs_avhrr_prod = hirs_avhrr_comp.dataset('out').product(hirs_avhrr_context)

        if SPC.exists(hirs_avhrr_prod):
            task.input('COLLO', hirs_avhrr_prod)
        else:
            raise WorkflowNotReady('No HIRS_AVHRR inputs available for {}'.format(hirs_avhrr_context['granule']))

        # CSRB Monthly Input
        csrb_context = hirs_avhrr_context
        csrb_context['csrb_version'] = context['csrb_version']
        csrb_context['granule'] = datetime(context['granule'].year, context['granule'].month, 1)
        LOG.debug("csrb_context:  {}".format(csrb_context))
        hirs_csrb_monthly_prod = hirs_csrb_monthly_comp.dataset('zonal_means').product(csrb_context)

        if SPC.exists(hirs_csrb_monthly_prod):
            task.input('CSRB', hirs_csrb_monthly_prod)
        else:
            raise WorkflowNotReady('No HIRS_CSRB_MONTHLY inputs available for {}'.format(csrb_context['granule']))

        # PTMSX Input
        LOG.debug('Getting PTMSX input...')
        sensor = 'avhrr'
        sat =  context['sat']
        file_type = 'PTMSX'
        granule = context['granule']

        try:
            ptmsx_file = delta_catalog.file(sensor, sat, file_type, granule)
            task.input('PTMSX',ptmsx_file)
        except IngestFileMissing:
            raise WorkflowNotReady('No PTMSX inputs available for {}'.format(granule))

        # CFSR Input
        LOG.debug('Getting CFSR input...')
        cfsr_file = self.get_cfsr(context['granule'])
        if cfsr_file is not None:
            task.input('CFSR', cfsr_file)
        else:
            raise WorkflowNotReady('No CFSR inputs available for {}'.format(granule))

        LOG.debug("Final task.inputs...")
        for task_key in task.inputs.keys():
            LOG.debug("\t{}: {}".format(task_key,task.inputs[task_key]))

    @reraise_as(WorkflowNotReady, FileNotFound, prefix='HIRS_CTP_ORBITAL')
    def run_task(self, inputs, context):
        LOG.debug("Running run_task()")

        debug = 0
        shifted_FM_opt = 2

        # Inputs
        inputs = symlink_inputs_to_working_dir(inputs)
        lib_dir = os.path.join(self.package_root, context['ctp_version'], 'lib')

        LOG.debug("inputs :  {}".format(inputs))

        # Output Name
        output = 'ctp.orbital.{}.{}.nc'.format(context['sat'], inputs['HIR1B'][12:30])
        LOG.debug("output :  {}".format(output))

        # Copy coeffs to working directory
        [shutil.copy(f, './')
         for f
         in glob(os.path.join(self.package_root, context['ctp_version'], 'coeffs/*'))]

        # Generating CFSR Binaries
        cfsr_bin_files = self.generate_cfsr_bin(os.path.join(self.package_root, context['ctp_version']))
        LOG.debug("cfsr_bin_files :  {}".format(cfsr_bin_files))

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

    def get_cfsr(self, granule):
        '''
        Retrieve the CFSR file which covers the desired granule.
        '''

        wedge = timedelta(seconds=1)
        day = timedelta(days=1)

        cfsr_granule = round_datetime(granule, timedelta(hours=6))
        cfsr_file = None

        have_cfsr_file = False

        # Search for the old style pgbhnl.gdas.*.grb2 file from DAWG
        if not have_cfsr_file:
            LOG.debug("Trying to retrieve CFSR_PGRBHANL product (pgbhnl.gdas.*.grb2) CFSR files from DAWG...")
            try:
                cfsr_file = dawg_catalog.file('', 'CFSR_PGRBHANL', cfsr_granule)
                have_cfsr_file = True
            except Exception, err :
                LOG.debug("{}.".format(err))

        # Search for the new style cdas1.*.t*z.pgrbhanl.grib2 file DAWG
        if not have_cfsr_file:
            LOG.debug("Trying to retrieve cdas1.*.t*z.pgrbhanl.grib2 CFSR file from DAWG...")
            try:
                cfsr_file = dawg_catalog.file('', 'CFSV2_PGRBHANL', cfsr_granule)
                have_cfsr_file = True
            except Exception, err :
                LOG.debug("{}.".format(err))

        return cfsr_file

    def generate_cfsr_bin(self,package_root):
        '''
        Convert the CFSR file into a flat binary file.
        '''

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

        LOG.debug("CFSR files: {}".format(files))

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


    def find_contexts(self, time_interval, sat, hirs_version, collo_version, csrb_version, ctp_version):

        LOG.debug("Running find_contexts()")
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

        LOG.debug("Running context_path()")
        return os.path.join('HIRS',
                            '{}/{}'.format(context['sat'], context['granule'].year),
                            'CTP_ORBITAL')
