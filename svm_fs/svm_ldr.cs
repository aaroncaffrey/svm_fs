using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace svm_fs
{
    public class svm_ldr
    {
        public static List<(cmd cmd, string job_id_filename, string pbs_script_filename, string options_filename, string finish_maker_filename, bool was_available, int state)> finish_marker_files = new List<(cmd cmd, string job_id_filename, string pbs_script_filename, string options_filename, string finish_maker_filename, bool was_available, int state)>();

        public static object finish_marker_files_lock = new object();

        public static void fix_controller_name(cmd_params controller_options)
        {
            if (string.IsNullOrWhiteSpace(controller_options.pbs_jobname))
            {
                controller_options.pbs_jobname = $@"{nameof(svm_ctl)}_{controller_options.experiment_name}";
            }

            if (controller_options.pbs_jobname.Length > 16)
            {
                controller_options.pbs_jobname = controller_options.pbs_jobname.Substring(controller_options.pbs_jobname.Length - 16);
            }
        }

        public static Task controller_task(cmd_params controller_options, CancellationToken ct)
        {

            var controller_task = Task.Run(() =>
            {
                var controller_job_id = "";

                while (!ct.IsCancellationRequested)
                {
                    var run_job_result = run_job(controller_options, controller_job_id);

                    controller_job_id = run_job_result.job_id;

                    Task.Delay(new TimeSpan(0, 0, 1, 0)).Wait();
                }
            });

            return controller_task;
        }

        public static Task worker_jobs_task(string job_submission_folder, CancellationToken ct)
        {
            svm_ctl.WriteLine($@"Monitoring '{job_submission_folder}' for new jobs to run...", nameof(svm_ldr), nameof(svm_ldr.worker_jobs_task));

            var worker_jobs_task = Task.Run(() =>
            {
                while (!ct.IsCancellationRequested)
                {

                    var job_ids = run_worker_jobs(job_submission_folder);

                    Task.Delay(new TimeSpan(0, 0, 0, 10)).Wait();
                }
            });

            return worker_jobs_task;
        }

        public static Task status_task(CancellationTokenSource cts)
        {
            var status_task = Task.Run(() =>
            {
                while (!cts.IsCancellationRequested)
                {
                    lock (finish_marker_files_lock)
                    {
                        try
                        {
                            finish_marker_files = finish_marker_files.Select(a => a.state == 0 ? a : (a.cmd, a.job_id_filename, a.pbs_script_filename, a.options_filename, a.finish_maker_filename, a.was_available ? a.was_available : svm_ctl.is_file_available(a.finish_maker_filename), a.state)).ToList();
                        }
                        catch (Exception)
                        {

                        }

                        try
                        {
                            finish_marker_files = finish_marker_files.Select(a => a.state == 0 ? a : (a.cmd, a.job_id_filename, a.pbs_script_filename, a.options_filename, a.finish_maker_filename, a.was_available, a.was_available && a.state != 0 ? int.Parse(File.ReadAllText(a.finish_maker_filename).Trim()) : a.state)).ToList();
                        }
                        catch (Exception )
                        {
                            
                        }

                        var jobs_completed = finish_marker_files.Where(a => a.state == 0).ToList();

                        foreach (var jc in jobs_completed)
                        {
                            //var pbs_options_filename = "";

                            try { File.Delete(jc.job_id_filename); } catch (Exception) { }
                            try { File.Delete(jc.finish_maker_filename); } catch (Exception) { }
                            try { File.Delete(jc.pbs_script_filename); } catch (Exception) { }
                            //try { File.Delete(jc.options_filename); } catch (Exception) { }
                        }

                        var num_jobs_completed_svm_ctl = finish_marker_files.Count(a => a.cmd==cmd.ctl && a.state == 0);
                        var num_jobs_completed = finish_marker_files.Count(a => a.state == 0);
                        var num_jobs_incompleted = finish_marker_files.Count(a => a.state == 1);
                        var num_jobs_other = finish_marker_files.Count(a => a.state != -1 && a.state != 0 && a.state != 1);
                        var num_jobs_not_started = finish_marker_files.Count(a => a.state == -1);

                        svm_ctl.WriteLine($@"jobs completed: {num_jobs_completed}; jobs incomplete: {num_jobs_incompleted}, jobs other: {num_jobs_other}, jobs not started: {num_jobs_not_started}", nameof(svm_ldr), nameof(svm_ldr.status_task));

                        if (num_jobs_completed_svm_ctl > 0)
                        {
                            cts.Cancel();
                            break;
                        }
                    }

                    Task.Delay(new TimeSpan(0, 0, 1, 0)).Wait();
                }
            });

            return status_task;
        }

        public static void start(cmd_params controller_options, CancellationTokenSource cts)
        {

            fix_controller_name(controller_options);
            var controller_task = svm_ldr.controller_task(controller_options, cts.Token);

            //// wait for controller task to load
            // while (controller_)

            var job_submission_folder = dataset_loader.convert_path(controller_options.pbs_submission_directory);
            if (!Directory.Exists(job_submission_folder))
            {
                try
                {
                    Directory.CreateDirectory(job_submission_folder);
                }
                catch (Exception)
                {

                }
            }

            var worker_jobs_task = svm_ldr.worker_jobs_task(job_submission_folder, cts.Token);


            var status_task = svm_ldr.status_task(cts);


            var tasks = new List<Task>();
            tasks.Add(controller_task);
            tasks.Add(worker_jobs_task);
            tasks.Add(status_task);
            Task.WaitAll(tasks.ToArray<Task>());
        }

        public static void write_job_id(cmd_params options, string job_id)
        {
            if (!string.IsNullOrWhiteSpace(job_id))
            {
                var job_id_filename = $"{options.options_filename}.job_id";

                job_id_filename = dataset_loader.convert_path(job_id_filename);

                File.WriteAllText(job_id_filename, job_id);
            }
        }

        public static string read_job_id(cmd_params options)
        {
            var job_id = "";
            var job_id_filename = $"{options.options_filename}.job_id";

            job_id_filename = dataset_loader.convert_path(job_id_filename);

            if (svm_ctl.is_file_available(job_id_filename))
            {
                job_id = File.ReadAllText(job_id_filename).Trim().Split().LastOrDefault();
            }

            return job_id;
        }

        public static bool check_job_exists(string job_id)
        {
            if (string.IsNullOrWhiteSpace(job_id))
            {
                return false;
            }
            // if job id is known, check the status is queued or running...

            var psi = new ProcessStartInfo()
            {
                FileName = $@"checkjob",
                Arguments = $@"{job_id}",
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                RedirectStandardInput = true,
            };


            using (var process = Process.Start(psi))
            {
                if (process != null)
                {
                    var stdout = process.StandardOutput.ReadToEnd();

                    var stderr = process.StandardError.ReadToEnd();

                    process.WaitForExit();

                    var stdout_lines = stdout.Split(new char[] { '\r', '\n' });

                    var state = stdout_lines.FirstOrDefault(a => a.StartsWith("State:"));

                    if (!string.IsNullOrWhiteSpace(state))
                    {
                        return true;
                    }
                }
            }

            return false;
        }

        public static object submitted_options_files_lock = new object();
        public static List<string> submitted_options_files = new List<string>();

        public static List<string> run_worker_jobs(string job_submission_folder)
        {
            lock (submitted_options_files_lock)
            {
                var options_files = Directory.GetFiles(job_submission_folder, "*.options", SearchOption.AllDirectories).ToList();

                options_files = options_files.Except(submitted_options_files).ToList();

                var cmd_params_list = options_files.Select((options_file, i) =>
                {
                    string[] fd = null;

                    try
                    {
                        fd = File.ReadAllLines(options_file);
                        
                    }
                    catch (Exception)
                    {
                        return null;

                    }

                    var r = new cmd_params(fd);

                    if (r.cmd == cmd.wkr)
                    {
                        try { File.Delete(options_file); } catch (Exception) { }
                    }
                    return r;

                }).ToList();

                

                var job_id_tasks = cmd_params_list.Select((cmd_params, i) => cmd_params != null && cmd_params.cmd == cmd.wkr ? run_job_async(cmd_params) : null).ToList();

                Task.WaitAll(job_id_tasks.Where(a => a != null).ToArray<Task>());

                var job_ids = job_id_tasks.Select((a, i) => a?.Result.job_id).ToList();

                submitted_options_files.AddRange(options_files.Where((a, i) => !string.IsNullOrWhiteSpace(job_ids[i])).ToList());

                return job_ids;
            }
        }

        public static Task<(string job_id, string job_id_filename, string pbs_script_filename, string pbs_finish_marker_filename)> run_job_async(cmd_params options, string job_id = null, bool rerunnable = true)
        {
            var task = Task.Run(() => { return run_job(options, job_id, rerunnable); });

            return task;
        }



        public static (string job_id, string job_id_filename, string pbs_script_filename, string pbs_finish_marker_filename/*, string msub_stdout, string msub_stderr*/) run_job(cmd_params options, string job_id = null, bool rerunnable = true)
        {
            var options_filename = $"{options.options_filename}";
            var job_id_filename = $"{options.options_filename}.job_id";
            var pbs_script_filename = $@"{options.options_filename}.pbs";
            var pbs_finish_marker_filename = $@"{options.options_filename}.fin";

            if (svm_ctl.is_file_available(job_id_filename) || svm_ctl.is_file_available(pbs_finish_marker_filename))
            {
                return default;
            }

            if (string.IsNullOrWhiteSpace(job_id))
            {
                job_id = read_job_id(options);
            }

            var job_exists = check_job_exists(job_id);

            if (!job_exists)
            {

                options.options_filename = dataset_loader.convert_path(options.options_filename);



                // 1. make pbs file to submit job
                var pbs_script_lines = new List<string>();
                if (!string.IsNullOrWhiteSpace(options.pbs_walltime)) pbs_script_lines.Add($@"#PBS -l walltime={options.pbs_walltime}");
                if (options.pbs_nodes > 0) pbs_script_lines.Add($@"#PBS -l nodes={options.pbs_nodes}{(options.pbs_ppn > 0 ? $@":ppn={options.pbs_ppn}" : "")}");
                if (!string.IsNullOrWhiteSpace(options.pbs_mem)) pbs_script_lines.Add($@"#PBS -l mem={options.pbs_mem}");
                if (!string.IsNullOrWhiteSpace(options.pbs_mem)) pbs_script_lines.Add($@"#PBS -r {(rerunnable ? "y" : "n")}");
                if (!string.IsNullOrWhiteSpace(options.pbs_jobname)) pbs_script_lines.Add($@"#PBS -N {options.pbs_jobname}");
                if (!string.IsNullOrWhiteSpace(options.pbs_mail_opt)) pbs_script_lines.Add($@"#PBS -m {options.pbs_mail_opt}");
                if (!string.IsNullOrWhiteSpace(options.pbs_mail_addr)) pbs_script_lines.Add($@"#PBS -M {options.pbs_mail_addr}");
                if (!string.IsNullOrWhiteSpace(options.pbs_stdout_filename)) pbs_script_lines.Add($@"#PBS -o {options.pbs_stdout_filename}");
                if (!string.IsNullOrWhiteSpace(options.pbs_stderr_filename)) pbs_script_lines.Add($@"#PBS -e {options.pbs_stderr_filename}");
                if (!string.IsNullOrWhiteSpace(options.pbs_execution_directory)) pbs_script_lines.Add($@"#PBS -d {options.pbs_execution_directory}");

                pbs_script_lines.Add($@"module load GCCcore");
                pbs_script_lines.Add($@"echo 1 > {pbs_finish_marker_filename}");
                pbs_script_lines.Add($@"date +""%a %Y/%m/%d %H:%M:%S""");
                pbs_script_lines.Add($@"{options.program_runtime} -j {options.options_filename}");
                pbs_script_lines.Add($@"date +""%a %Y/%m/%d %H:%M:%S""");
                pbs_script_lines.Add($@"echo 0 > {pbs_finish_marker_filename}");

                //var pbs_script_filename = $@"~/svm_fs/pbs_job_{options.pbs_jobname}_{options.job_id}.pbs";


                File.WriteAllLines(pbs_script_filename, pbs_script_lines);

                // 2. submit pbs script to scheduler
                var psi = new ProcessStartInfo()
                {
                    FileName = $@"msub",
                    Arguments = $@"{pbs_script_filename}",
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    RedirectStandardInput = true,
                };

                job_id = "";

                var stdout = "";
                var stderr = "";

                using (var process = Process.Start(psi))
                {
                    if (process != null)
                    {
                        stdout = process.StandardOutput.ReadToEnd();
                        stderr = process.StandardError.ReadToEnd();

                        job_id = stdout.Split().Where(a => !string.IsNullOrWhiteSpace(a)).LastOrDefault();

                        process.WaitForExit();
                    }
                }

                if (job_id.StartsWith("Moab."))
                {
                    // 3. save job id
                    write_job_id(options, job_id);

                    // 4. add to list of submitted jobs
                    //add_submitted_job(options);

                    svm_ctl.WriteLine($@"{options.cmd}: msub {pbs_script_filename} = {job_id}", nameof(svm_ldr), nameof(run_job));

                    lock (finish_marker_files_lock)
                    {
                        if (!finish_marker_files.Any(a => a.finish_maker_filename == pbs_finish_marker_filename))
                        {
                            finish_marker_files.Add((options.cmd, job_id_filename, pbs_script_filename, options_filename, pbs_finish_marker_filename, false, -1));
                        }
                    }
                }
                else
                {
                    if (!string.IsNullOrWhiteSpace(stdout)) svm_ctl.WriteLine(stdout, nameof(svm_ldr), nameof(run_job));
                    if (!string.IsNullOrWhiteSpace(stderr)) svm_ctl.WriteLine(stderr, nameof(svm_ldr), nameof(run_job));
                }
            }

            return (job_id, job_id_filename, pbs_script_filename, pbs_finish_marker_filename/*, stdout, stderr*/);
        }
    }
}
