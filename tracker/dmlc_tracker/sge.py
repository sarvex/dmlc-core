"""Submit jobs to Sun Grid Engine."""
# pylint: disable=invalid-name
from __future__ import absolute_import

import os
import subprocess
from . import tracker

def submit(args):
    """Job submission script for SGE."""
    if args.jobname is None:
        args.jobname = ('dmlc%d.' % args.num_workers) + args.command[0].split('/')[-1]
    if args.sge_log_dir is None:
        args.sge_log_dir = f'{args.jobname}.log'

    if os.path.exists(args.sge_log_dir):
        if not os.path.isdir(args.sge_log_dir):
            raise RuntimeError(f'specified --sge-log-dir {args.sge_log_dir} is not a dir')
    else:
        os.mkdir(args.sge_log_dir)

    runscript = f'{args.logdir}/rundmlc.sh'
    with open(runscript, 'w') as fo:
        fo.write('source ~/.bashrc\n')
        fo.write('export DMLC_TASK_ID=${SGE_TASK_ID}\n')
        fo.write('export DMLC_JOB_CLUSTER=sge\n')
        fo.write('\"$@\"\n')
    def sge_submit(nworker, nserver, pass_envs):
        """Internal submission function."""
        env_arg = ','.join('%s=\"%s\"' % (k, str(v)) for k, v in pass_envs.items())
        cmd = 'qsub -cwd -t 1-%d -S /bin/bash' % (nworker + nserver)
        if args.queue != 'default':
            cmd += f'-q {args.queue}'
        cmd += f' -N {args.jobname} '
        cmd += f' -e {args.logdir} -o {args.logdir}'
        cmd += ' -pe orte %d' % (args.vcores)
        cmd += ' -v %s,PATH=${PATH}:.' % env_arg
        cmd += f" {runscript} {' '.join(args.command)}"
        print(cmd)
        subprocess.check_call(cmd, shell=True)
        print('Waiting for the jobs to get up...')

    # call submit, with nslave, the commands to run each job and submit function
    tracker.submit(args.num_workers, args.num_servers,
                   fun_submit=sge_submit,
                   pscmd=' '.join(args.command))
