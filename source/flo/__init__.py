
from datetime import datetime, timedelta
from glob import glob
import os
import shutil
from flo.computation import Computation
from flo.time import round_datetime
from flo.subprocess import check_call
from flo.util import augmented_env, symlink_inputs_to_working_dir
from flo.sw.hirs import HIRS
from flo.sw.hirs.util import generate_cfsr_bin
from flo.sw.hirs_avhrr import HIRS_AVHRR
from flo.sw.hirs_csrb_monthly import HIRS_CSRB_MONTHLY
from flo.sw.hirs.delta import delta_catalog


class HIRS_CTP_ORBITAL(Computation):

    parameters = ['granule', 'sat', 'hirs_version', 'collo_version', 'csrb_version', 'ctp_version']
    outputs = ['out']

    def build_task(self, context, task):

        # HIRS L1B Input
        hirs_context = context.copy()
        [hirs_context.pop(k) for k in ['collo_version', 'csrb_version', 'ctp_version']]
        task.input('HIR1B', HIRS().dataset('out').product(hirs_context))

        # Collo Input
        collo_context = hirs_context
        collo_context['collo_version'] = context['collo_version']
        task.input('COLLO', HIRS_AVHRR().dataset('out').product(collo_context))

        # CSRB Monthly Input
        csrb_context = collo_context
        csrb_context['csrb_version'] = context['csrb_version']
        csrb_context['granule'] = datetime(context['granule'].year, context['granule'].month, 1)
        task.input('CSRB', HIRS_CSRB_MONTHLY().dataset('zonal_means').product(csrb_context))

        # PTMSX Input
        task.input('PTMSX', delta_catalog.file('avhrr', context['sat'], 'PTMSX',
                                               context['granule']))

        # CFSR Input
        cfsr_granule = round_datetime(context['granule'], timedelta(hours=6))
        task.input('CFSR', delta_catalog.file('ancillary', 'NONE', 'CFSR', cfsr_granule))

    def run_task(self, inputs, context):

        debug = 0
        shifted_FM_opt = 2

        # Inputs
        inputs = symlink_inputs_to_working_dir(inputs)
        lib_dir = os.path.join(self.package_root, context['ctp_version'], 'lib')

        # Output Name
        output = 'ctp.orbital.{}.{}.nc'.format(context['sat'], inputs['HIR1B'][12:30])

        # Copy coeffs to working directory
        [shutil.copy(f, './')
         for f
         in glob(os.path.join(self.package_root, context['ctp_version'], 'coeffs/*'))]

        # Generating CFSR Binaries
        generate_cfsr_bin(os.path.join(self.package_root, context['ctp_version']))

        # Running CTP Orbital
        cmd = os.path.join(self.package_root, context['ctp_version'],
                           'bin/process_hirs_cfsr.exe')
        cmd += ' {} {}.bin {}'.format(inputs['HIR1B'], inputs['CFSR'], inputs['COLLO'])
        cmd += ' {} {}'.format(inputs['PTMSX'], inputs['CSRB'])
        cmd += ' {}'.format(os.path.join(self.package_root, context['ctp_version'],
                                         'CFSR_lst.bin'))
        cmd += ' {} {} {}'.format(debug, shifted_FM_opt, output)

        print cmd
        check_call(cmd, shell=True, env=augmented_env({'LD_LIBRARY_PATH': lib_dir}))

        return {'out': output}

    def find_contexts(self, sat, hirs_version, collo_version, csrb_version, ctp_version,
                      time_interval):

        files = delta_catalog.files('hirs', sat, 'HIR1B', time_interval)
        return [{'granule': file.data_interval.left,
                 'sat': sat,
                 'hirs_version': hirs_version,
                 'collo_version': collo_version,
                 'csrb_version': csrb_version,
                 'ctp_version': ctp_version}
                for file in files]

    def context_path(self, context, output):

        return os.path.join('HIRS',
                            '{}/{}'.format(context['sat'], context['granule'].year),
                            'CTP_ORBITAL')
