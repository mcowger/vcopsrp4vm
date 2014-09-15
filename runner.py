__author__ = 'mcowger'


import logging
from RP4VM_vcops import RP4VM_Connections,Vcops_Connection
from docopt import docopt
import time
from pprint import pprint,pformat

__doc__ = """
Usage: runner.py [-h] RPA_IP RPA_USER RPA_PASS VCOPS_IP [--interval=<interval>] [--vcops_user=<user>] [--vcops_pass=<pass>] [--quiet] [--debug_level=<level>] [--protocol=<proto>]

Collect Performance Statistics from an XtremIO array and push them to vCenter Operations

Arguments:
    RPA_IP       IP of XMS (required)
    RPA_USER    Username for XMS
    RPA_PASS    PAssword for XMS
    VCOPS_IP    IP of VCOPS instance


Options:
    -h --help    show this
    --quiet      print less text
    --debug_level=<level>    Very verbose debugging [default: WARN]
    --protocol=<proto>   [http | https] [default: https]
    --vcops_user=<user>  VC Ops User [default: admin]
    --vcops_pass=<pass>  VC Ops Password [default: P@ssword1!]
    --interval=<interval>   Sleep interval between collections [default: 60]
"""




if __name__ == "__main__":
    options = docopt(__doc__, argv=None, help=True, version=None, options_first=False)
    numeric_level = getattr(logging, options['--debug_level'].upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % loglevel)
    logging.basicConfig(level=numeric_level,format='%(asctime)s|%(name)s|%(levelname)s|%(module)s:%(lineno)d:%(funcName)s|%(message)s')
    logger = logging.getLogger(__name__)
    logger.info("Printing received options:")
    logger.info(options)

    while True:
        rp4vm = RP4VM_Connections(options)
        rp4vm.collect_and_submit()
        logger.warn("Completed Collection Run, sleeping for {} seconds".format(options['--interval']))
        time.sleep(int(options['--interval']))