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
from subprocess import CalledProcessError

from flo.computation import Computation
from flo.builder import WorkflowNotReady
from timeutil import TimeInterval, datetime, timedelta, round_datetime
from flo.util import augmented_env, symlink_inputs_to_working_dir
from flo.product import StoredProductCatalog

import sipsprod
from glutil import (
    check_call,
    dawg_catalog,
    delivered_software,
    #support_software,
    runscript,
    #prepare_env,
    #nc_gen,
    nc_compress,
    reraise_as,
    #set_official_product_metadata,
    FileNotFound
)
import flo.sw.hirs2nc as hirs2nc
import flo.sw.hirs_avhrr as hirs_avhrr
import flo.sw.hirs_csrb_monthly as hirs_csrb_monthly
from flo.sw.hirs2nc.delta import DeltaCatalog
from flo.sw.hirs2nc.utils import link_files

# every module should have a LOG object
LOG = logging.getLogger(__name__)

def set_input_sources(input_locations):
    global delta_catalog
    delta_catalog = DeltaCatalog(**input_locations)

class HIRS_CTP_ORBITAL(Computation):

    parameters = ['granule', 'satellite', 'hirs2nc_delivery_id', 'hirs_avhrr_delivery_id',
                  'hirs_csrb_daily_delivery_id', 'hirs_csrb_monthly_delivery_id',
                  'hirs_ctp_orbital_delivery_id']
    outputs = ['out']

    def find_contexts(self, time_interval, satellite, hirs2nc_delivery_id, hirs_avhrr_delivery_id,
                      hirs_csrb_daily_delivery_id, hirs_csrb_monthly_delivery_id,
                      hirs_ctp_orbital_delivery_id):

        LOG.debug("Running find_contexts()")
        files = delta_catalog.files('hirs', satellite, 'HIR1B', time_interval)
        return [{'granule': file.data_interval.left,
                 'satellite': satellite,
                 'hirs2nc_delivery_id': hirs2nc_delivery_id,
                 'hirs_avhrr_delivery_id': hirs_avhrr_delivery_id,
                 'hirs_csrb_daily_delivery_id': hirs_csrb_daily_delivery_id,
                 'hirs_csrb_monthly_delivery_id': hirs_csrb_monthly_delivery_id,
                 'hirs_ctp_orbital_delivery_id': hirs_ctp_orbital_delivery_id}
                for file in files
                if file.data_interval.left >= time_interval.left]

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

    @reraise_as(WorkflowNotReady, FileNotFound, prefix='HIRS_CTP_ORBITAL')
    def build_task(self, context, task):
        '''
        Build up a set of inputs for a single context
        '''
        global delta_catalog

        LOG.debug("Running build_task()")

        # Initialize the hirs2nc and hirs_avhrr modules with the data locations
        hirs2nc.delta_catalog = delta_catalog
        hirs_avhrr.delta_catalog = delta_catalog

        # Instantiate the hirs, hirs_avhrr and hirs_csrb_monthly computations
        hirs2nc_comp = hirs2nc.HIRS2NC()
        hirs_avhrr_comp = hirs_avhrr.HIRS_AVHRR()
        hirs_csrb_monthly_comp = hirs_csrb_monthly.HIRS_CSRB_MONTHLY()

        SPC = StoredProductCatalog()

        # HIRS L1B Input
        hirs2nc_context = context.copy()
        [hirs2nc_context.pop(k) for k in ['hirs_avhrr_delivery_id', 'hirs_csrb_daily_delivery_id',
                                          'hirs_csrb_monthly_delivery_id', 'hirs_ctp_orbital_delivery_id']]
        hirs2nc_prod = hirs2nc_comp.dataset('out').product(hirs2nc_context)

        if SPC.exists(hirs2nc_prod):
            task.input('HIR1B', hirs2nc_prod)
        else:
            raise WorkflowNotReady('No HIRS inputs available for {}'.format(hirs2nc_context['granule']))

        # PTMSX Input
        LOG.debug('Getting PTMSX input...')
        sensor = 'avhrr'
        satellite =  context['satellite']
        file_type = 'PTMSX'
        granule = context['granule']

        try:
            ptmsx_file = delta_catalog.file(sensor, satellite, file_type, granule)
            task.input('PTMSX',ptmsx_file)
        except WorkflowNotReady:
            raise WorkflowNotReady('No PTMSX inputs available for {}'.format(granule))

        # Collo Input
        hirs_avhrr_context = hirs2nc_context
        hirs_avhrr_context['hirs_avhrr_delivery_id'] = context['hirs_avhrr_delivery_id']
        hirs_avhrr_prod = hirs_avhrr_comp.dataset('out').product(hirs_avhrr_context)

        if SPC.exists(hirs_avhrr_prod):
            task.input('COLLO', hirs_avhrr_prod)
        else:
            raise WorkflowNotReady('No HIRS_AVHRR inputs available for {}'.format(hirs_avhrr_context['granule']))

        # CSRB Monthly Input
        hirs_csrb_monthly_context = context.copy()
        [hirs_csrb_monthly_context.pop(k) for k in ['hirs_ctp_orbital_delivery_id']]
        hirs_csrb_monthly_context['granule'] = datetime(context['granule'].year, context['granule'].month, 1)
        hirs_csrb_monthly_prod = hirs_csrb_monthly_comp.dataset('zonal_means').product(hirs_csrb_monthly_context)

        if SPC.exists(hirs_csrb_monthly_prod):
            task.input('CSRB', hirs_csrb_monthly_prod)
        else:
            raise WorkflowNotReady('No HIRS_CSRB_MONTHLY inputs available for {}'.format(hirs_csrb_monthly_context['granule']))
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

    def extract_bin_from_cfsr(self, inputs, context):
        '''
        Run wgrib2 on the  input CFSR grib files, to create flat binary files
        containing the desired data.
        '''

        # Where are we running the package
        work_dir = abspath(curdir)
        LOG.debug("working dir = {}".format(work_dir))

        # Get the required CFSR and wgrib2 script locations
        hirs_ctp_orbital_delivery_id = context['hirs_ctp_orbital_delivery_id']
        delivery = delivered_software.lookup('hirs_ctp_orbital', delivery_id=hirs_ctp_orbital_delivery_id)
        dist_root = pjoin(delivery.path, 'dist')
        extract_cfsr_bin = pjoin(dist_root, 'bin/extract_cfsr.csh')
        version = delivery.version

        # Get the CFSR input
        cfsr_file = inputs['CFSR']
        LOG.debug("CFSR file: {}".format(cfsr_file))

        # Extract the desired datasets for each CFSR file
        rc = 0
        new_cfsr_files = []

        output_cfsr_file = '{}.bin'.format(basename(cfsr_file))
        cmd = '{} {} {} {}'.format(extract_cfsr_bin, cfsr_file, output_cfsr_file, dirname(extract_cfsr_bin))
        #cmd = 'sleep 0; touch {}'.format(output_cfsr_file) # DEBUG

        try:
            LOG.debug("cmd = \\\n\t{}".format(cmd.replace(' ',' \\\n\t')))
            rc_extract_cfsr = 0
            runscript(cmd, [delivery])
        except CalledProcessError as err:
            rc_extract_cfsr = err.returncode
            LOG.error("extract_cfsr binary {} returned a value of {}".format(extract_cfsr_bin, rc_extract_cfsr))
            return rc_extract_cfsr, []

        # Verify output file
        output_cfsr_file = glob(output_cfsr_file)
        if len(output_cfsr_file) != 0:
            output_cfsr_file = output_cfsr_file[0]
            LOG.debug('Found flat CFSR file "{}"'.format(output_cfsr_file))
        else:
            LOG.error('Failed to generate "{}", aborting'.format(output_cfsr_file))
            rc = 1
            return rc, None

        return rc, output_cfsr_file

    def hirs_to_time_interval(self, filename):
        '''
        Takes the HIRS filename as input and returns the time interval
        covering that file.
        '''

        file_chunks = filename.split('.')
        begin_time = datetime.strptime('.'.join(file_chunks[3:5]), 'D%y%j.S%H%M')
        end_time = datetime.strptime('.'.join([file_chunks[3], file_chunks[5]]), 'D%y%j.E%H%M')

        if end_time < begin_time:
            end_time += timedelta(days=1)

        return TimeInterval(begin_time, end_time)

    def create_ctp_orbital(self, inputs, context):
        '''
        Create the the CTP Orbital for the current granule.
        '''

        rc = 0

        # Create the output directory
        current_dir = os.getcwd()

        # Get the required CFSR and wgrib2 script locations
        hirs_ctp_orbital_delivery_id = context['hirs_ctp_orbital_delivery_id']
        delivery = delivered_software.lookup('hirs_ctp_orbital', delivery_id=hirs_ctp_orbital_delivery_id)
        dist_root = pjoin(delivery.path, 'dist')
        lut_dir = pjoin(dist_root, 'luts')
        version = delivery.version

        # Compile a dictionary of the input orbital data files
        interval = self.hirs_to_time_interval(inputs['HIR1B'])
        LOG.debug("HIRS interval {} -> {}".format(interval.left,interval.right))

        # Determine the output filenames
        output_file = 'hirs_ctp_orbital_{}_{}{}.nc'.format(context['satellite'],
                                                          interval.left.strftime('D%y%j.S%H%M'),
                                                          interval.right.strftime('.E%H%M'))
        LOG.info("output_file: {}".format(output_file))

        # Link the coefficient files into the working directory
        shifted_coeffs = [abspath(x) for x in glob(pjoin(lut_dir,'shifted_hirs_FM_coeff/*'))]
        unshifted_coeffs = [abspath(x) for x in glob(pjoin(lut_dir,'unshifted_hirs_FM_coeff/*'))]
        linked_coeffs = link_files(current_dir, shifted_coeffs+
                                   unshifted_coeffs+
                                   [
                                       abspath(pjoin(lut_dir, 'CFSR_lst.bin')),
                                       abspath(pjoin(lut_dir, 'CO2_1979-2017_monthly_181_lat.dat'))
                                   ])

        LOG.debug("Linked coeffs: {}".format(linked_coeffs))

        ctp_orbital_bin = pjoin(dist_root, 'bin/process_hirs_cfsr.exe')
        debug = 0
        shifted_FM_opt = 2

        cmd = '{} {} {} {} {} {} {} {} {} {} {}'.format(
                ctp_orbital_bin,
                inputs['HIR1B'],
                inputs['CFSR'],
                inputs['COLLO'],
                inputs['PTMSX'],
                inputs['CSRB'],
                'CFSR_lst.bin',
                'CO2_1979-2017_monthly_181_lat.dat',
                debug,
                shifted_FM_opt,
                output_file
                )
        #cmd = 'sleep 1; touch {}'.format(output_file) # DEBUG

        try:
            LOG.debug("cmd = \\\n\t{}".format(cmd.replace(' ',' \\\n\t')))
            rc_ctp = 0
            runscript(cmd, [delivery])
        except CalledProcessError as err:
            rc_ctp = err.returncode
            LOG.error(" CTP orbital binary {} returned a value of {}".format(ctp_orbital_bin, rc_ctp))
            return rc_ctp, None

        # Verify output file
        output_file = glob(output_file)
        if len(output_file) != 0:
            output_file = output_file[0]
            LOG.debug('Found output  CTP orbital file "{}"'.format(output_file))
        else:
            LOG.error('Failed to generate "{}", aborting'.format(output_file))
            rc = 1
            return rc, None

        return rc, output_file

    @reraise_as(WorkflowNotReady, FileNotFound, prefix='HIRS_CTP_ORBITAL')
    def run_task(self, inputs, context):
        '''
        Run the CTP Orbital binary on a single context
        '''

        LOG.debug("Running run_task()...")

        for key in context.keys():
            LOG.debug("run_task() context['{}'] = {}".format(key, context[key]))

        rc = 0

        # Extract a binary array from a CFSR reanalysis GRIB2 file on a
        # global equal angle grid at 0.5 degree resolution. CFSR files
        rc, cfsr_file = self.extract_bin_from_cfsr(inputs, context)

        # Link the inputs into the working directory
        inputs.pop('CFSR')
        inputs = symlink_inputs_to_working_dir(inputs)
        inputs['CFSR'] = cfsr_file

        # Create the CTP Orbital for the current granule.
        rc, ctp_orbital_file = self.create_ctp_orbital(inputs, context)

        return {'out': nc_compress(ctp_orbital_file)}
