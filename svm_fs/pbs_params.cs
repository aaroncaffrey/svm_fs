using System;
using System.Collections.Generic;
using System.Text;

namespace svm_fs
{
    internal static class pbs_params
    {
        internal const string svm_fs_home = "/mmfs1/data/scratch/k1040015/svm_fs";
        internal const string user_home = "/home/k1040015";

        internal static string env_jobid = $@"${{JOBID}}${{PBS_JOBID}}${{MOAB_JOBID}}";
        internal static string env_jobname = $@"${{JOBNAME}}${{PBS_JOBNAME}}${{MOAB_JOBNAME}}";
        internal static string env_arrayindex = $@"${{PBS_ARRAYID}}${{MOAB_JOBARRAYINDEX}}";
        internal static int env_arrayincr = 1;

        internal static string program_runtime = System.Diagnostics.Process.GetCurrentProcess().MainModule.FileName;

        internal static string program_ctl_stderr_filename = $@"{nameof(svm_ctl)}_{env_jobid}_{env_jobname}_{env_arrayindex}.program.stderr";
        internal static string program_ctl_stdout_filename = $@"{nameof(svm_ctl)}_{env_jobid}_{env_jobname}_{env_arrayindex}.program.stdout";
        
        internal static string program_wkr_stderr_filename = $@"{nameof(svm_wkr)}_{env_jobid}_{env_jobname}_{env_arrayindex}.program.stderr";
        internal static string program_wkr_stdout_filename = $@"{nameof(svm_wkr)}_{env_jobid}_{env_jobname}_{env_arrayindex}.program.stdout";

        internal static string pbs_ctl_execution_directory = $@"{svm_fs_home}/pbs_ctl_sub/";
        //internal static string pbs_ctl_jobname = $@"svm_fs_ctl";
        internal static string pbs_ctl_mail_addr = ""; // 
        internal static string pbs_ctl_mail_opt = "n"; // abe| n
        internal static string pbs_ctl_mem = "192GB";
        internal static string pbs_ctl_stderr_filename = $@"{nameof(svm_ctl)}_%J_%I.pbs.stderr";
        internal static string pbs_ctl_stdout_filename = $@"{nameof(svm_ctl)}_%J_%I.pbs.stdout";
        internal static string pbs_ctl_submission_directory = $@"{svm_fs_home}/pbs_ctl_sub/";
        internal static string pbs_ctl_walltime = "240:00:00";
        internal static int pbs_ctl_nodes = 1;
        internal static int pbs_ctl_ppn = 64;
                 
        internal static string pbs_wkr_execution_directory = $@"{svm_fs_home}/pbs_wkr_sub/";
        //internal static string pbs_wkr_jobname = $@"svm_fs_wkr";
        internal static string pbs_wkr_mail_addr = ""; // 
        internal static string pbs_wkr_mail_opt = "n"; // abe|n
        internal static string pbs_wkr_mem = null;
        internal static string pbs_wkr_stderr_filename = $@"{nameof(svm_wkr)}_%J_%I.pbs.stderr";
        internal static string pbs_wkr_stdout_filename = $@"{nameof(svm_wkr)}_%J_%I.pbs.stdout";
        internal static string pbs_wkr_submission_directory = $@"{svm_fs_home}/pbs_wkr_sub/";
        internal static string pbs_wkr_walltime = "0:30:00";
        internal static int pbs_wkr_nodes = 1;
        internal static int pbs_wkr_ppn = 16;


    }
}
