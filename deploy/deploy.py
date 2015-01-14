
from flo_deploy.packagelib import *


class HIRS_CTP_Orbital_Package(Package):

    def deploy_package(self):
        for version in ['v20140204']:
            self.merge(Extracted('HIRS_CTP_Orbital_{}.tar.gz'.format(version)).path(), version)
            self.merge(NetcdfFortran().path(), version)
            self.merge(Netcdf().path(), version)
            self.merge(Hdf5().path(), version)
