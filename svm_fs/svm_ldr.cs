using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace svm_fs
{
    internal class svm_ldr
    {
        //internal static int total_jobs_completed = 0;

        //internal static List<(cmd cmd, string job_id_filename, string pbs_script_filename, string options_filename, string finish_maker_filename, bool was_available, int state)> finish_marker_files = new List<(cmd cmd, string job_id_filename, string pbs_script_filename, string options_filename, string finish_maker_filename, bool was_available, int state)>();

        //internal static readonly object finish_marker_files_lock = new object();



        //internal static void fix_controller_name(cmd_params controller_options)
        //{
        //    io_proxy.WriteLine($@"Method: fix_controller_name(cmd_params controller_options = {controller_options.options_filename})", nameof(svm_ldr), nameof(fix_controller_name));
        //
        //
        //    if (string.IsNullOrWhiteSpace(controller_options.pbs_ctl_jobname))
        //    {
        //        controller_options.pbs_ctl_jobname = $@"{nameof(svm_ctl)}_{controller_options.experiment_name}";
        //    }
        //
        //    if (controller_options.pbs_ctl_jobname.Length > 16)
        //    {
        //        controller_options.pbs_ctl_jobname = controller_options.pbs_ctl_jobname.Substring(controller_options.pbs_ctl_jobname.Length - 16);
        //    }
        //}

        //internal static Task controller_task(cmd_params controller_options, CancellationToken ct)
        //{
        //    io_proxy.WriteLine($@"Method: controller_task(cmd_params controller_options = {controller_options.options_filename}, CancellationToken ct = {ct})", nameof(svm_ldr), nameof(svm_ldr.controller_task));
        //
        //
        //    var controller_task = Task.Run(() =>
        //    {
        //        var controller_job_id = "";
        //
        //        while (!ct.IsCancellationRequested)
        //        {
        //            var run_job_result = run_job(controller_options, controller_job_id);
        //
        //            controller_job_id = run_job_result.job_id;
        //
        //            try
        //            {
        //                Task.Delay(new TimeSpan(0, 0, 1, 0), ct).Wait(ct);
        //            }
        //            catch (Exception e)
        //            {
        //                io_proxy.log_exception(e, "", nameof(svm_ldr), nameof(svm_ldr.controller_task));
        //            }
        //        }
        //
        //        io_proxy.WriteLine($@"Exiting task {nameof(svm_ldr.controller_task)}.", nameof(svm_ldr), nameof(svm_ldr.controller_task));
        //
        //    });
        //
        //    return controller_task;
        //}

        internal static Task worker_jobs_task(string job_submission_folder, CancellationToken ct)
        {
            io_proxy.WriteLine($@"Method: worker_jobs_task(string job_submission_folder = {job_submission_folder}, CancellationToken ct = {ct})", nameof(svm_ldr), nameof(worker_jobs_task));

            io_proxy.WriteLine($@"Monitoring '{job_submission_folder}' for new jobs to run...", nameof(svm_ldr), nameof(svm_ldr.worker_jobs_task));

            var worker_jobs_task1 = Task.Run(() =>
            {
                while (!ct.IsCancellationRequested)
                {

                    var job_ids = run_worker_jobs(job_submission_folder);

                    try
                    {
                        Task.Delay(new TimeSpan(0, 0, 0, 15), ct).Wait(ct);
                    }
                    catch (Exception e)
                    {
                        io_proxy.log_exception(e, "", nameof(svm_ldr), nameof(svm_ldr.worker_jobs_task));
                    }
                }

                io_proxy.WriteLine($@"Exiting task {nameof(svm_ldr.worker_jobs_task)}.", nameof(svm_ldr), nameof(worker_jobs_task));
            });

            return worker_jobs_task1;
        }

        //internal static Task status_task(CancellationTokenSource cts)
        //{
        //    io_proxy.WriteLine($@"Method: status_task(CancellationTokenSource cts = {cts})", nameof(svm_ldr), nameof(status_task));
        //
        //    var status_task1 = Task.Run(() =>
        //    {
        //        while (!cts.IsCancellationRequested)
        //        {
        //            lock (finish_marker_files_lock)
        //            {
        //                try
        //                {
        //                    finish_marker_files = finish_marker_files.Select(a => a.state == 0 ? a : (a.cmd, a.job_id_filename, a.pbs_script_filename, a.options_filename, a.finish_maker_filename, a.was_available ? a.was_available : io_proxy.is_file_available(a.finish_maker_filename, nameof(svm_ldr), nameof(status_task)), a.state)).ToList();
        //                }
        //                catch (Exception e)
        //                {
        //                    io_proxy.log_exception(e, "", nameof(svm_ldr), nameof(svm_ldr.status_task));
        //                }
        //
        //                try
        //                {
        //                    finish_marker_files = finish_marker_files.Select(a => a.state == 0 ? a : (a.cmd, a.job_id_filename, a.pbs_script_filename, a.options_filename, a.finish_maker_filename, a.was_available, a.was_available && a.state != 0 ? int.Parse(io_proxy.ReadAllText(a.finish_maker_filename).Trim(), CultureInfo.InvariantCulture) : a.state)).ToList();
        //                }
        //                catch (Exception e)
        //                {
        //                    io_proxy.log_exception(e, "", nameof(svm_ldr), nameof(svm_ldr.status_task));
        //                }
        //
        //                var jobs_completed = finish_marker_files.Where(a => a.state == 0).ToList();
        //
        //                var delete_job_files = false;
        //
        //                if (delete_job_files)
        //                {
        //                    foreach (var jc in jobs_completed)
        //                    {
        //                        io_proxy.Delete(jc.job_id_filename, nameof(svm_ldr), nameof(status_task));
        //                        io_proxy.Delete(jc.finish_maker_filename, nameof(svm_ldr), nameof(status_task));
        //                        io_proxy.Delete(jc.pbs_script_filename, nameof(svm_ldr), nameof(status_task));
        //                        //io_proxy.Delete(jc.options_filename); // deleted elsewhere
        //                    }
        //                }
        //
        //                //var num_jobs_completed_svm_ctl = finish_marker_files.Count(a => a.cmd==cmd.ctl && a.state == 0);
        //                //var num_jobs_completed = finish_marker_files.Count(a => a.state == 0);
        //                //var num_jobs_incompleted = finish_marker_files.Count(a => a.state == 1);
        //                //var num_jobs_other = finish_marker_files.Count(a => a.state != -1 && a.state != 0 && a.state != 1);
        //                //var num_jobs_not_started = finish_marker_files.Count(a => a.state == -1);
        //
        //                //svm_ldr.total_jobs_completed += num_jobs_completed;
        //
        //                //io_proxy.WriteLine($@"total jobs completed: {svm_ldr.total_jobs_completed}, jobs newly completed: {num_jobs_completed}, jobs incomplete: {num_jobs_incompleted}, jobs other: {num_jobs_other}, jobs not started: {num_jobs_not_started}", nameof(svm_ldr), nameof(svm_ldr.status_task));
        //
        //                //finish_marker_files = finish_marker_files.Where(a => a.state != 0).ToList();
        //
        //                //if (num_jobs_completed_svm_ctl > 0)
        //                //{
        //                //    cts.Cancel();
        //                //    break;
        //                //}
        //            }
        //
        //            try
        //            {
        //                Task.Delay(new TimeSpan(0, 0, 0, 30), cts.Token).Wait(cts.Token);
        //            }
        //            catch (Exception e)
        //            {
        //                io_proxy.log_exception(e, "", nameof(svm_ldr), nameof(svm_ldr.status_task));
        //            }
        //        }
        //
        //        io_proxy.WriteLine($@"Exiting task {nameof(svm_ldr.status_task)}.", nameof(svm_ldr), nameof(status_task));
        //
        //    });
        //
        //    return status_task1;
        //}

        internal static string start_ctl(string launch_options_file = "")
        {
            //fix_controller_name(controller_options);

            //var pbs_script_filename = Path.Combine(pbs_params.pbs_ctl_submission_directory, "svm_ctl.pbs");

            var pbs_script_filename = make_pbs_script(launch_options_file, 0, cmd.ctl, null, true);

            var controller_job_id = msub(pbs_script_filename, false);

            return controller_job_id;
        }

        internal static void start_ldr(CancellationTokenSource cts, string controller_launch_options_filename = "")
        {
            io_proxy.WriteLine($@"Method: {nameof(start_ldr)}(CancellationTokenSource cts = ""{cts}"", string controller_launch_options_filename = ""{controller_launch_options_filename}"")", nameof(svm_ldr), nameof(start_ldr));
            
            io_proxy.CreateDirectory(pbs_params.get_default_ctl_values().pbs_execution_directory);
            io_proxy.CreateDirectory(pbs_params.get_default_wkr_values().pbs_execution_directory);

            // start controller
            start_ctl(controller_launch_options_filename);

            // listen for worker job requests
            var worker_jobs_task = svm_ldr.worker_jobs_task(pbs_params.get_default_wkr_values().pbs_execution_directory, cts.Token);

            
            var tasks = new List<Task>();
            
            tasks.Add(worker_jobs_task);
            
            svm_ctl.wait_tasks(tasks.ToArray<Task>(), nameof(svm_ldr), nameof(start_ldr));
        }

        internal static void write_job_id(string job_id_filename, string job_id)
        {
            io_proxy.WriteLine($@"Method: write_job_id(string job_id_filename = {job_id_filename}, string job_id = {job_id})", nameof(svm_ldr), nameof(write_job_id));

            if (!string.IsNullOrWhiteSpace(job_id))
            {
                //job_id_filename = /*io_proxy.convert_path*/(job_id_filename);

                io_proxy.WriteAllText(job_id_filename, job_id);
            }
        }

        //internal static string read_job_id(string job_id_filename)
        //{
        //    io_proxy.WriteLine($@"Method: read_job_id(string job_id_filename = {job_id_filename})", nameof(svm_ldr), nameof(read_job_id));
        //
        //    var job_id = "";
        //    //var job_id_filename = $"{options.options_filename}.job_id";
        //    //job_id_filename = /*io_proxy.convert_path*/(job_id_filename);
        //
        //    if (io_proxy.is_file_available(job_id_filename, nameof(svm_ldr), nameof(read_job_id)))
        //    {
        //        job_id = io_proxy.ReadAllText(job_id_filename).Trim().Split().LastOrDefault();
        //    }
        //
        //    return job_id;
        //}

        //internal static bool check_job_exists(string job_id)
        //{
        //    io_proxy.WriteLine($@"Method: check_job_exists(string job_id = {job_id})", nameof(svm_ldr),
        //        nameof(check_job_exists));
        //
        //    if (string.IsNullOrWhiteSpace(job_id))
        //    {
        //        return false;
        //    }
        //    // if job id is known, check the status is queued or running...
        //
        //    var psi = new ProcessStartInfo()
        //    {
        //        FileName = $@"checkjob",
        //        Arguments = $@"{job_id}",
        //        RedirectStandardOutput = true,
        //        RedirectStandardError = true,
        //        RedirectStandardInput = true,
        //    };
        //
        //    var tries = 0;
        //    var max_tries = 1_000_000;
        //
        //    while (tries < max_tries)
        //    {
        //        tries++;
        //
        //        try
        //        {
        //            using (var process = Process.Start(psi))
        //            {
        //                if (process != null)
        //                {
        //                    var stdout_task = process.StandardOutput.ReadToEndAsync();
        //
        //                    var stderr_task = process.StandardError.ReadToEndAsync();
        //
        //
        //                    var exited = process.WaitForExit((int)(new TimeSpan(0, 0, 20)).TotalMilliseconds);
        //
        //                    var killed = false;
        //                    if (!process.HasExited)
        //                    {
        //                        try
        //                        {
        //                            process.Kill(true);
        //                            var kill_exited = process.WaitForExit((int)(new TimeSpan(0, 0, 20)).TotalMilliseconds);
        //                        }
        //                        catch (Exception e)
        //                        {
        //                            io_proxy.log_exception(e, "", nameof(svm_ldr), nameof(check_job_exists));
        //                        }
        //
        //                        killed = true;
        //                    }
        //
        //                    Task.WaitAll(new Task[] { stdout_task, stderr_task }, new TimeSpan(0, 0, 20));
        //
        //                    var stdout = stdout_task.IsCompleted ? stdout_task.Result : "";
        //                    var stderr = stderr_task.IsCompleted ? stderr_task.Result : "";
        //
        //                    //if (!string.IsNullOrWhiteSpace(stdout)) stdout.Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries).ToList().ForEach(a => io_proxy.WriteLine($@"{nameof(stdout)}: {a}", nameof(svm_ldr), nameof(check_job_exists)));
        //                    if (!string.IsNullOrWhiteSpace(stderr)) stderr.Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries).ToList().ForEach(a => io_proxy.WriteLine($@"{nameof(stderr)}: {a}", nameof(svm_ldr), nameof(check_job_exists)));
        //
        //                    var exit_code = process.HasExited ? (int?)process.ExitCode : null;
        //
        //                    var stdout_lines = stdout?.Split(new char[] { '\r', '\n' });
        //
        //                    var state = stdout_lines?.FirstOrDefault(a => a.StartsWith("State:", StringComparison.InvariantCulture));
        //
        //                    if (exit_code == 0 && !string.IsNullOrWhiteSpace(stdout))
        //                    {
        //                        var job_exists = !string.IsNullOrWhiteSpace(state);
        //
        //                        return job_exists;
        //                    }
        //                    else if (killed)
        //                    {
        //                        io_proxy.WriteLine($"Error: Killed. Process did not respond. {Path.GetFileName(psi.FileName)}. Stdout: {stdout.Length}. Stderr: {stderr.Length}. Tries: {tries}. Exit code: {exit_code}.", nameof(svm_ldr), nameof(check_job_exists));
        //
        //                    }
        //                    else
        //                    {
        //                        io_proxy.WriteLine($"Error: non zero exit code for process {Path.GetFileName(psi.FileName)}. Tries: {tries}. Exit code: {exit_code}.", nameof(svm_ldr), nameof(check_job_exists));
        //                    }
        //                }
        //            }
        //
        //            io_proxy.WriteLine($"Error: process could not start {Path.GetFileName(psi.FileName)}. Tries {tries}.", nameof(svm_ldr), nameof(check_job_exists));
        //        }
        //        catch (Exception e)
        //        {
        //            io_proxy.log_exception(e, "", nameof(svm_ldr), nameof(svm_ldr.check_job_exists));
        //        }
        //
        //        try
        //        {
        //            Task.Delay(new TimeSpan(0, 0, 15)).Wait();
        //        }
        //        catch (Exception e)
        //        {
        //            io_proxy.log_exception(e, "", nameof(svm_ldr), nameof(svm_ldr.check_job_exists));
        //        }
        //    }
        //
        //    return false;
        //}

        //internal static object submitted_options_files_lock = new object();
        //internal static List<string> submitted_options_files = new List<string>();

        private static object submitted_job_list_files_lock = new object();
        private static List<string> submitted_job_list_files = new List<string>();

        internal static List<string> run_worker_jobs(string job_submission_folder)
        {
            io_proxy.WriteLine($@"Method: run_worker_jobs(string job_submission_folder = ""{job_submission_folder}"")", nameof(svm_ldr), nameof(run_worker_jobs));

            var default_ctl_values = pbs_params.get_default_ctl_values();

            var max_job_array_size = 50_000;
            var nodes = 24;
            var node_vcpus = 64;
            var total_vcpus = 1440 - (default_ctl_values.pbs_nodes * default_ctl_values.pbs_ppn);
            var node_ram = 256 - node_vcpus; // leave 1gb free per vcpu for other tasks = 192
            var ram_per_vcpu = node_ram / node_vcpus;  // 192 / 64 = 3gb per vcpu


            

            


            var job_ids = new List<string>();

            lock (submitted_job_list_files_lock)
            {
                // run a worker array with input of the text file list and the line number to read

                var parameter_list_files = Directory.GetFiles(job_submission_folder, "job_list_*.txt", SearchOption.AllDirectories).Except(submitted_job_list_files).ToList();


                foreach (var parameter_list_file in parameter_list_files)
                {
                    if (submitted_job_list_files.Contains(parameter_list_file)) continue;

                    var pbs_params = svm_fs.pbs_params.get_default_wkr_values();

                    var max_concurrent_jobs = total_vcpus / pbs_params.pbs_ppn; //1440 / 16 = 90
                    //var max_mem = ram_per_vcpu * pbs_params.pbs_ppn;


                    var array_size = io_proxy.ReadAllLines(parameter_list_file, nameof(svm_ldr), nameof(run_worker_jobs)).Length;

                    var array_step = array_size / max_concurrent_jobs;

                    var num_array_jobs = array_size / array_step;

                    pbs_params.pbs_walltime = TimeSpan.FromHours(240);// TimeSpan.FromMinutes(((array_window_size * 3) / pbs_params.pbs_ppn) + 10); // 3 minutes per item ... 
                    //pbs_params.pbs_mem = $"{max_mem}gb";

                    if (num_array_jobs > max_job_array_size)
                    {
                        throw new Exception($@"num_array_jobs > max_job_array_size");
                    }




                    var pbs_script_filename = make_pbs_script(parameter_list_file, array_step, cmd.wkr, pbs_params, true);

                    var wkr_job_id = msub(pbs_script_filename, true, 0, array_size - 1, array_step);

                    if (!string.IsNullOrWhiteSpace(wkr_job_id))
                    {
                        submitted_job_list_files.Add(parameter_list_file);


                        job_ids.Add(wkr_job_id);

                        // don't delete any files here in case job crashes and needs to be rerun
                    }
                }
            }

            return job_ids;
        }

        //internal static Task<(string job_id, /*string job_id_filename,*/ string pbs_script_filename/*, string pbs_finish_marker_filename*/)> run_job_async(cmd_params options, string job_id = null, bool rerunnable = true)
        //{
        //    io_proxy.WriteLine($@"Method: run_job_async(cmd_params options = {options.options_filename}, string job_id = {job_id}, bool rerunnable = {rerunnable})", nameof(svm_ldr), nameof(run_job_async));
        //
        //    var task = Task.Run(() => { return msub(options, job_id, rerunnable); });
        //
        //    return task;
        //}


        //(string job_id, /*string job_id_filename,*/ string pbs_script_filename/*, string pbs_finish_marker_filename*//*, string msub_stdout, string msub_stderr*/)
        internal static string msub(string pbs_script_filename, bool array = false, int array_start = 0, int array_end = 0, int array_step = 1)//cmd_params options, string job_id = null, bool rerunnable = true)
        {
            //io_proxy.WriteLine($@"Method: run_job(cmd_params options = {options.options_filename}, string job_id = {job_id}, bool rerunnable = {rerunnable})", nameof(svm_ldr), nameof(msub));

            const string sub_cmd = "msub";

            var job_id = "";

            //var options_filename = /*io_proxy.convert_path*/($"{options.options_filename}");
            //var job_id_filename = /*io_proxy.convert_path*/($"{options.options_filename}.job_id");
            //var pbs_script_filename = /*io_proxy.convert_path*/($@"{options.options_filename}.pbs");
            //var pbs_finish_marker_filename = /*io_proxy.convert_path*/($@"{options.options_filename}.fin");

            //var debug_file = /*io_proxy.convert_path*/($@"{options.options_filename}.debug");

            //if (io_proxy.is_file_available(job_id_filename, nameof(svm_ldr), nameof(run_job)) || io_proxy.is_file_available(pbs_finish_marker_filename, nameof(svm_ldr), nameof(run_job)))
            //{
            //    return default;
            //}

            //if (string.IsNullOrWhiteSpace(job_id))
            //{
            //job_id = read_job_id(job_id_filename);
            //}

            //var job_exists = check_job_exists(job_id);
            var job_exists = false;

            //var k = 0;

            if (!job_exists)
            {

                //File.AppendAllLines(debug_file, new List<string>(){$@"make_pbs_script(options, rerunnable, pbs_finish_marker_filename, pbs_script_filename);"});
                //make_pbs_script(options, rerunnable, /*pbs_finish_marker_filename,*/ pbs_script_filename);


                // 2. submit pbs script to scheduler
                //var psi = new ProcessStartInfo();

                //var use_pbs = Environment.OSVersion.Platform != PlatformID.Win32NT;

                //var use_pbs = true;

                //if (use_pbs)
                //{

                //File.AppendAllLines(debug_file, new List<string>() {$@"var psi = new ProcessStartInfo() {{FileName = sub_cmd,Arguments = $@""{{pbs_script_filename}}"", UseShellExecute = false, RedirectStandardOutput = true, RedirectStandardError = true, RedirectStandardInput = true,}};"});

                var psi = new ProcessStartInfo()
                {
                    FileName = sub_cmd,
                    Arguments = (array ? $@"-t {array_start}-{array_end}:{array_step} " : "") + pbs_script_filename,
                    UseShellExecute = false,
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    RedirectStandardInput = true,
                };

                var cmd_line = $"{psi.FileName} {psi.Arguments}";

                io_proxy.WriteLine(cmd_line, nameof(svm_ldr), nameof(msub));

                //}
                //else
                //{
                //    psi = new ProcessStartInfo()
                //    {
                //        FileName = /*io_proxy.convert_path*/(options.program_runtime),
                //        Arguments = $@"-j {/*io_proxy.convert_path*/(options.options_filename)}",
                //        RedirectStandardOutput = true,
                //        RedirectStandardError = true,
                //        RedirectStandardInput = true,
                //    };
                //}



                //File.AppendAllLines(debug_file, new List<string>() { $@"job_id = "";" });

                var exit_code = (int?)null;
                //File.AppendAllLines(debug_file, new List<string>() { $@"var exit_code = (int?) null;" });

                var tries = 0;
                //File.AppendAllLines(debug_file, new List<string>() { $@"var tries = 0;" });

                var max_tries = 1_000_000;
                //File.AppendAllLines(debug_file, new List<string>() {$@"var max_tries = 1_000_000;"});


                while (tries < max_tries)
                {
                    //File.AppendAllLines(debug_file, new List<string>() { $@"while ({tries} < {max_tries})" });

                    try
                    {
                        tries++;
                        //File.AppendAllLines(debug_file, new List<string>() { $@"tries++;" });

                        using (var process = Process.Start(psi))
                        {
                            //File.AppendAllLines(debug_file, new List<string>() { $@"using (var process = Process.Start(psi))" });

                            if (process != null)
                            {
                                //File.AppendAllLines(debug_file, new List<string>() { $@"if (process != null)" });

                                var stdout_task = process.StandardOutput.ReadToEndAsync();
                                //File.AppendAllLines(debug_file, new List<string>() { $@"var stdout = process.StandardOutput.ReadToEnd();" });

                                var stderr_task = process.StandardError.ReadToEndAsync();
                                //File.AppendAllLines(debug_file, new List<string>() { $@"var stderr = process.StandardError.ReadToEnd();" });

                                var exited = process.WaitForExit((int)(new TimeSpan(0, 0, 30)).TotalMilliseconds);

                                var killed = false;

                                if (!process.HasExited)
                                {
                                    try
                                    {
                                        process.Kill(true);

                                        var kill_exited = process.WaitForExit((int)(new TimeSpan(0, 0, 30)).TotalMilliseconds);
                                    }
                                    catch (Exception e)
                                    {
                                        io_proxy.log_exception(e, "", nameof(svm_ldr), nameof(msub));
                                    }

                                    killed = true;
                                }

                                Task.WaitAll(new Task[] { stdout_task, stderr_task }, new TimeSpan(0, 0, 30));

                                var stdout = stdout_task.IsCompleted ? stdout_task.Result : "";
                                var stderr = stderr_task.IsCompleted ? stderr_task.Result : "";

                                //File.AppendAllLines(debug_file, new List<string>() { $@"process.WaitForExit();" });

                                if (!string.IsNullOrWhiteSpace(stdout)) stdout.Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries).ToList().ForEach(a => io_proxy.WriteLine($@"{nameof(stdout)}: {a}", nameof(svm_ldr), nameof(msub)));
                                if (!string.IsNullOrWhiteSpace(stderr)) stderr.Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries).ToList().ForEach(a => io_proxy.WriteLine($@"{nameof(stderr)}: {a}", nameof(svm_ldr), nameof(msub)));

                                exit_code = process.HasExited ? (int?)process.ExitCode : null;

                                if (process.ExitCode == 0 && !string.IsNullOrWhiteSpace(stdout))
                                {
                                    //File.AppendAllLines(debug_file, new List<string>() { $@"if (process.ExitCode == 0)" });

                                    job_id = stdout.Trim().Split().LastOrDefault() ?? "";

                                    //File.AppendAllLines(debug_file, new List<string>() { $@"job_id = stdout.Trim().Split().LastOrDefault() ?? "";" });
                                    //File.AppendAllLines(debug_file, new List<string>() { $@"job_id = {stdout.Trim().Split().LastOrDefault()} ?? "";" });

                                    if (job_id.StartsWith($@"Moab.", StringComparison.InvariantCultureIgnoreCase))
                                    {
                                        //File.AppendAllLines(debug_file, new List<string>() { $@"if (job_id.StartsWith($@""Moab."", StringComparison.InvariantCultureIgnoreCase))" });

                                        break;
                                    }
                                    else
                                    {
                                        io_proxy.WriteLine($@"Error: invalid job_id. Tries: {tries}. Exit code: {exit_code}. ( {pbs_script_filename} )", nameof(svm_ldr), nameof(msub));
                                    }
                                }
                                else if (killed)
                                {
                                    io_proxy.WriteLine($@"Error: Killed. Process did not respond. Tries: {tries}. Exit code: {exit_code}. Stdout: {stdout.Length}. Stderr: {stderr.Length}. ( {pbs_script_filename} )", nameof(svm_ldr), nameof(msub));
                                }
                                else
                                {
                                    io_proxy.WriteLine($@"Error: non zero exit code / no stdout. Tries: {tries}. Exit code: {exit_code}. ( {pbs_script_filename} )", nameof(svm_ldr), nameof(msub));
                                }
                            }
                            else
                            {
                                io_proxy.WriteLine($@"Error: process could not be launched. Tries: {tries}. ( {pbs_script_filename} )", nameof(svm_ldr), nameof(msub));
                            }
                        }

                    }
                    catch (Exception e)
                    {
                        io_proxy.log_exception(e, $"{nameof(pbs_script_filename)} = {pbs_script_filename}", nameof(svm_ldr), nameof(svm_ldr.msub));
                    }

                    io_proxy.WriteLine($@"Error: process could not start. Tries: {tries}. Exit code: {exit_code}. ( {pbs_script_filename} )", nameof(svm_ldr), nameof(msub));

                    try
                    {
                        Task.Delay(new TimeSpan(0, 0, 15)).Wait();
                    }
                    catch (Exception e)
                    {
                        io_proxy.log_exception(e, $"{nameof(pbs_script_filename)} = {pbs_script_filename}", nameof(svm_ldr), nameof(svm_ldr.msub));
                    }
                }


                if (job_id.StartsWith($@"Moab.", StringComparison.InvariantCultureIgnoreCase))
                {
                    // 3. save job id
                    //write_job_id(job_id_filename, job_id);

                    // 4. add to list of submitted jobs
                    //add_submitted_job(options);

                    io_proxy.WriteLine($@"{cmd_line} -> {nameof(job_id)} = {job_id}.", nameof(svm_ldr), nameof(msub));

                    //lock (finish_marker_files_lock)
                    //{
                    //    if (!finish_marker_files.Any(a => string.Equals(a.finish_maker_filename, pbs_finish_marker_filename, StringComparison.InvariantCultureIgnoreCase)))
                    //    {
                    //        finish_marker_files.Add((options.cmd, job_id_filename, pbs_script_filename, options_filename, pbs_finish_marker_filename, false, -1));
                    //    }
                    //}
                }
                else
                {
                    io_proxy.WriteLine($@"Error: {cmd_line} failed.  Exit code: {exit_code}.  {nameof(job_id)} = {job_id}.", nameof(svm_ldr), nameof(msub));
                }


            }
            else
            {
                //if (options.cmd == cmd.wkr)
                {
                    io_proxy.WriteLine($@"Error: job already exists ( {pbs_script_filename} )", nameof(svm_ldr), nameof(msub));
                }
            }

            return job_id; //(job_id, /*job_id_filename,*/ pbs_script_filename/*, pbs_finish_marker_filename*//*, stdout, stderr*/);
        }

        private static object make_pbs_script_lock = new object();
        private static int ctl_pbs_script_index = 0;
        private static int wkr_pbs_script_index = 0;

        private static string make_pbs_script(string input_file, int array_step, cmd cmd, pbs_params pbs_params = null, bool rerunnable = true)
        {
            io_proxy.WriteLine($@"Method: make_pbs_script(string input_file = {input_file}, cmd cmd = {cmd}, bool rerunnable = {rerunnable}, )", nameof(svm_ldr), nameof(make_pbs_script));

            var script_index = 0;

            lock (make_pbs_script_lock)
            {
                if (cmd == cmd.ctl)
                {
                    script_index = ctl_pbs_script_index++;
                }
                else if (cmd == cmd.wkr)
                {
                    script_index = wkr_pbs_script_index++;
                }
            }

            var pbs_script_lines = new List<string>();

            if (pbs_params == null)
            {
                if (cmd == cmd.ctl)
                {
                    pbs_params = pbs_params.get_default_ctl_values();
                }
                else if (cmd == cmd.wkr)
                {
                    pbs_params = pbs_params.get_default_wkr_values();
                }
                else
                {
                    pbs_params = new pbs_params();
                }
            }

            pbs_params.pbs_jobname = $@"{nameof(svm_fs)}_{cmd}_{script_index}";
            var pbs_script_filename = Path.Combine(pbs_params.pbs_execution_directory, $"{pbs_params.pbs_jobname}.pbs");

            if (pbs_params.pbs_walltime != null && pbs_params.pbs_walltime.TotalSeconds > 0) pbs_script_lines.Add($@"#PBS -l walltime={Math.Floor(pbs_params.pbs_walltime.TotalHours):00}:{pbs_params.pbs_walltime.Minutes:00}:{pbs_params.pbs_walltime.Seconds:00}");
            if (pbs_params.pbs_nodes > 0) pbs_script_lines.Add($@"#PBS -l nodes={pbs_params.pbs_nodes}{(pbs_params.pbs_ppn > 0 ? $@":ppn={pbs_params.pbs_ppn}" : "")}");
            if (!string.IsNullOrWhiteSpace(pbs_params.pbs_mem)) pbs_script_lines.Add($@"#PBS -l mem={pbs_params.pbs_mem}");
            pbs_script_lines.Add($@"#PBS -r {(rerunnable ? "y" : "n")}");
            if (!string.IsNullOrWhiteSpace(pbs_params.pbs_jobname)) pbs_script_lines.Add($@"#PBS -N {pbs_params.pbs_jobname}");
            if (!string.IsNullOrWhiteSpace(pbs_params.pbs_mail_opt)) pbs_script_lines.Add($@"#PBS -m {pbs_params.pbs_mail_opt}");
            if (!string.IsNullOrWhiteSpace(pbs_params.pbs_mail_addr)) pbs_script_lines.Add($@"#PBS -M {pbs_params.pbs_mail_addr}");
            if (!string.IsNullOrWhiteSpace(pbs_params.pbs_stdout_filename)) pbs_script_lines.Add($@"#PBS -o {pbs_params.pbs_stdout_filename}");
            if (!string.IsNullOrWhiteSpace(pbs_params.pbs_stderr_filename)) pbs_script_lines.Add($@"#PBS -e {pbs_params.pbs_stderr_filename}");
            if (!string.IsNullOrWhiteSpace(pbs_params.pbs_execution_directory)) pbs_script_lines.Add($@"#PBS -d {pbs_params.pbs_execution_directory}");

            var run_line = $@"{pbs_params.program_runtime} -cm {cmd} -ji {pbs_params.env_jobid} -jn {pbs_params.env_jobname} -ai {pbs_params.env_arrayindex} -ac {array_step} {(cmd==cmd.wkr? $@"-in {input_file}" : $@"-of {input_file}")}{(!string.IsNullOrEmpty(pbs_params.program_stdout_filename) ? $@" 1> {pbs_params.program_stdout_filename}" : "")}{(!string.IsNullOrEmpty(pbs_params.program_stderr_filename) ? $@" 2> {pbs_params.program_stderr_filename}" : "")}";

            pbs_script_lines.Add($@"module load GCCcore");
            pbs_script_lines.Add(run_line);

            io_proxy.WriteAllLines(pbs_script_filename, pbs_script_lines);
            io_proxy.WriteLine(run_line, nameof(svm_ldr), nameof(make_pbs_script));

            return pbs_script_filename;

        }
    }
}

