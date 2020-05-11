using System;
using System.Collections.Generic;
using System.Text;

namespace svm_fs
{
    internal class pbs_params
    {
        internal const string env_jobid = @"${PBS_JOBID}${MOAB_JOBID}";
        internal const string env_jobname = @"${PBS_JOBNAME}${MOAB_JOBNAME}";
        internal const string env_arrayindex = @"${PBS_ARRAYID}${MOAB_JOBARRAYINDEX}";
        
        internal string program_runtime = System.Diagnostics.Process.GetCurrentProcess().MainModule.FileName;
      
        internal string pbs_execution_directory = "";
        internal string pbs_jobname = "";
        internal string pbs_mail_addr = "";
        internal string pbs_mail_opt = "n";
        internal string pbs_mem = null;
        internal string pbs_stdout_filename = $@"{nameof(svm_fs)}_%J_%I.pbs.stdout";
        internal string pbs_stderr_filename = $@"{nameof(svm_fs)}_%J_%I.pbs.stderr";
        internal TimeSpan pbs_walltime = TimeSpan.FromSeconds(0);
        internal int pbs_nodes = 0;
        internal int pbs_ppn = 0;
        internal string program_stdout_filename = $@"{nameof(svm_fs)}_{env_jobid}_{env_jobname}_{env_arrayindex}.program.stdout";
        internal string program_stderr_filename = $@"{nameof(svm_fs)}_{env_jobid}_{env_jobname}_{env_arrayindex}.program.stderr";

        public static pbs_params get_default_ctl_values()
        {
            return new pbs_params()
            {
                pbs_execution_directory = $@"{cmd_params.svm_fs_home}/pbs_{cmd.ctl}_sub/",
                pbs_jobname = $@"{nameof(svm_fs)}_{cmd.ctl}",
                pbs_mail_addr = "",
                pbs_mail_opt = "n",
                pbs_mem = null,
                pbs_stdout_filename = $"{nameof(svm_fs)}_{cmd.ctl}.pbs.stdout",
                pbs_stderr_filename = $"{nameof(svm_fs)}_{cmd.ctl}.pbs.stderr",
                pbs_walltime = TimeSpan.FromHours(240), //"240:00:00"
                pbs_nodes = 1,
                pbs_ppn = 64,
                program_stdout_filename = $@"{nameof(svm_fs)}_{cmd.ctl}_{env_jobid}_{env_jobname}.program.stdout",
                program_stderr_filename = $@"{nameof(svm_fs)}_{cmd.ctl}_{env_jobid}_{env_jobname}.program.stderr",
            };
        }

        public static pbs_params get_default_wkr_values()
        {
            return new pbs_params()
            {
                pbs_execution_directory = $@"{cmd_params.svm_fs_home}/pbs_{cmd.wkr}_sub/",
                pbs_jobname = $@"{nameof(svm_fs)}_{cmd.wkr}",
                pbs_mail_addr = "",
                pbs_mail_opt = "n",
                pbs_mem = null,
                pbs_stdout_filename = $"{nameof(svm_fs)}_{cmd.wkr}_%J_%I.pbs.stdout",
                pbs_stderr_filename = $"{nameof(svm_fs)}_{cmd.wkr}_%J_%I.pbs.stderr",
                pbs_walltime = TimeSpan.FromMinutes(30), //"00:30:00",
                pbs_nodes = 1,
                pbs_ppn = 16,
                program_stdout_filename = $@"{nameof(svm_fs)}_{cmd.wkr}_{env_jobid}_{env_jobname}_{env_arrayindex}.program.stdout",
                program_stderr_filename = $@"{nameof(svm_fs)}_{cmd.wkr}_{env_jobid}_{env_jobname}_{env_arrayindex}.program.stderr",
            };
        }
    }
}
