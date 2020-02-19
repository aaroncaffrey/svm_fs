using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace svm_fs
{
    internal static class libsvm
    {


        internal static (string cmd_line, string stdout, string stderr) train(
            string libsvm_train_exe_file,
            string train_file,
            string model_out_file,
            string stdout_file = null,
            string stderr_file = null,
            double? cost = null,
            double? gamma = null,
            double? epsilon = null,
            double? coef0 = null,
            double? degree = null,
            List<(int class_id, double weight)> weights = null,
            common.libsvm_svm_type svm_type = common.libsvm_svm_type.c_svc,
            common.libsvm_kernel_type kernel = common.libsvm_kernel_type.rbf,

            int? inner_cv_folds = null,
            bool probability_estimates = false,
            bool shrinking_heuristics = true,
            TimeSpan? process_max_time = null,
            bool quiet_mode = true,
            int memory_limit_mb = 1024
            )
        {
            libsvm_train_exe_file = io_proxy.convert_path(libsvm_train_exe_file);
            train_file = io_proxy.convert_path(train_file);
            model_out_file = io_proxy.convert_path(model_out_file);
            stdout_file = io_proxy.convert_path(stdout_file);
            stderr_file = io_proxy.convert_path(stderr_file);

            //var quiet_mode = true;
            //var memory_limit_mb = 1024;

            var libsvm_params = new List<string>();


            if (quiet_mode)
            {
                libsvm_params.Add("-q");
            }

            if (memory_limit_mb != 100)
            {
                libsvm_params.Add($@"-m {memory_limit_mb}");
            }

            if (probability_estimates)
            {
                libsvm_params.Add($@"-b {(probability_estimates ? "1" : "0")}");
            }

            if (svm_type != common.libsvm_svm_type.c_svc)
            {
                libsvm_params.Add($@"-s {(int)svm_type}");
            }


            if (kernel != common.libsvm_kernel_type.rbf)
            {
                libsvm_params.Add($@"-t {(int)kernel}");
            }


            if (inner_cv_folds != null && inner_cv_folds >= 2)
            {
                libsvm_params.Add($@"-v {inner_cv_folds}");
            }

            if (cost != null)
            {
                libsvm_params.Add($@"-c {cost.Value}");
            }

            if (gamma != null && kernel != common.libsvm_kernel_type.linear)
            {
                libsvm_params.Add($@"-g {gamma.Value}");
            }

            if (epsilon != null && (svm_type == common.libsvm_svm_type.epsilon_svr || svm_type == common.libsvm_svm_type.nu_svr))
            {
                libsvm_params.Add($@"-p {epsilon.Value}");
            }

            if (coef0 != null && (kernel == common.libsvm_kernel_type.sigmoid || kernel == common.libsvm_kernel_type.polynomial))
            {
                libsvm_params.Add($@"-r {coef0.Value}");
            }

            if (degree != null && kernel == common.libsvm_kernel_type.polynomial)
            {
                libsvm_params.Add($@"-d {degree.Value}");
            }

            if (weights != null && weights.Count > 0)
            {
                foreach (var weight in weights.OrderBy(a => a).ToList())
                {
                    libsvm_params.Add($@"-w{weight.class_id} {weight.weight}");
                }
            }

            if (!shrinking_heuristics)
            {
                libsvm_params.Add($@"-h {(shrinking_heuristics ? "1" : "0")}");
            }

            libsvm_params = libsvm_params.OrderBy(a => a).ToList();

            var train_file_param = train_file; //Path.GetFileName(train_file);
            var model_file_param = model_out_file; // Path.GetFileName(model_out_file);

            if (!String.IsNullOrWhiteSpace(train_file)) { libsvm_params.Add($@"{train_file_param}"); }
            if (!String.IsNullOrWhiteSpace(model_out_file) && (inner_cv_folds == null || inner_cv_folds <= 1)) { libsvm_params.Add($@"{model_file_param}"); }

            var wd = Path.GetDirectoryName(train_file);

            //var exe_file = $@"c:\libsvm-3.24\windows\svm-train.exe";

            var args = string.Join(" ", libsvm_params);

            var start = new ProcessStartInfo()
            {
                FileName = libsvm_train_exe_file,
                Arguments = args,
                UseShellExecute = false,
                CreateNoWindow = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                RedirectStandardInput = true,
                //WorkingDirectory = wd ?? "" // Path.GetDirectoryName(exe_file) ?? ""
            };

            var cmd_line = string.Join(" ", libsvm_train_exe_file, args);

            var priority_boost_enabled = false;
            var priority_class = ProcessPriorityClass.AboveNormal;

            if (inner_cv_folds == null || inner_cv_folds < 2)
            {
                priority_class = ProcessPriorityClass.High;
            }

            var retry_index = -1;
            var retry = false;
            do
            {
                retry_index++;

                try
                {
                    using (var process = Process.Start(start))
                    {
                        if (process == null)
                        {
                            retry = true;
                            continue;
                            //   return (cmd_line, null, null);
                        }

                        io_proxy.WriteLine($"Spawned process: {process.Id}", nameof(libsvm), nameof(train));

                        try { process.PriorityBoostEnabled = priority_boost_enabled; } catch (Exception) { }
                        try { process.PriorityClass = priority_class; } catch (Exception) { }

                        var stdout = process.StandardOutput.ReadToEndAsync();
                        var stderr = process.StandardError.ReadToEndAsync();

                        var tasks = new List<Task>() { stdout, stderr };



                        if (!process.HasExited && process_max_time != null && process_max_time.Value.Ticks > 0)
                        {
                            //var time_taken = DateTime.Now - process.StartTime;

                            try
                            { 
                                var cpu_time = process.TotalProcessorTime;

                                do
                                {
                                    try { Task.WaitAll(tasks.ToArray(), process_max_time.Value); } catch (Exception) { }
                                    //time_taken = DateTime.Now - process.StartTime;
                                    cpu_time = process.TotalProcessorTime;
                                } while (tasks.Any(a => !a.IsCompleted) && cpu_time < process_max_time.Value && !process.HasExited);

                                if (tasks.Any(a => !a.IsCompleted) && !process.HasExited)
                                {
                                    try { process.CancelOutputRead(); } catch (Exception) { } finally { }
                                    try { process.CancelErrorRead(); } catch (Exception) { } finally { }
                                    try { process.CloseMainWindow(); } catch (Exception) { } finally { }
                                    try { process.Close(); } catch (Exception) { } finally { }
                                    try { process.Kill(); } catch (Exception) { } finally { }
                                    try { process.Dispose(); } catch (Exception) { } finally { }
                                }
                            }
                            catch (Exception e)
                            {
                                io_proxy.WriteLine($"Process: {process.Id}. {e.ToString()}", nameof(libsvm), nameof(train));
                            }
                        }
                        
                        try { Task.WaitAll(tasks.ToArray<Task>()); } catch (Exception) { }
                        

                        process.WaitForExit();
                        io_proxy.WriteLine($"Exited process: {process.Id}", nameof(libsvm), nameof(train));

                        var stdout_result = "";
                        var stderr_result = "";

                        try { stdout_result = stdout?.Result; } catch (Exception) { }
                        try { stderr_result = stderr?.Result; } catch (Exception) { }


                        if (!string.IsNullOrWhiteSpace(stdout_file) && !string.IsNullOrWhiteSpace(stdout_result))
                        {
                            io_proxy.AppendAllText(stdout_file, stdout_result);
                        }

                        if (!string.IsNullOrWhiteSpace(stderr_file) && !string.IsNullOrWhiteSpace(stderr_result))
                        {
                            io_proxy.AppendAllText(stderr_file, stderr_result);
                        }

                        return (cmd_line, stdout_result, stderr_result);
                    }
                }
                catch (Exception e)
                {
                    retry = true;

                    io_proxy.WriteLine(e.ToString(), nameof(libsvm), nameof(train));
                    Task.Delay(new TimeSpan(0, 0, 0, 10)).Wait();
                }
            }
            while (retry && retry_index < 10_000);

            return (cmd_line, null, null);
        }

        internal static (string cmd_line, string stdout, string stderr) predict(string libsvm_predict_exe_file, string test_file, string model_file, string predictions_out_file, bool probability_estimates, string stdout_file = null, string stderr_file = null)
        {
            libsvm_predict_exe_file = io_proxy.convert_path(libsvm_predict_exe_file);
            test_file = io_proxy.convert_path(test_file);
            model_file = io_proxy.convert_path(model_file);
            predictions_out_file = io_proxy.convert_path(predictions_out_file);
            stdout_file = io_proxy.convert_path(stdout_file);
            stderr_file = io_proxy.convert_path(stderr_file);

            var libsvm_params = new List<string>();

            if (probability_estimates)
            {
                libsvm_params.Add($@"-b 1");
            }

            libsvm_params = libsvm_params.OrderBy(a => a).ToList();

            var test_file_param = test_file; // Path.GetFileName(test_file);
            var model_file_param = model_file; //Path.GetFileName(model_file)
            var prediction_file_param = predictions_out_file; //Path.GetFileName(predictions_out_file)

            if (!String.IsNullOrWhiteSpace(test_file)) { libsvm_params.Add($@"{test_file_param}"); }
            if (!String.IsNullOrWhiteSpace(model_file)) { libsvm_params.Add($@"{model_file_param}"); }
            if (!String.IsNullOrWhiteSpace(predictions_out_file)) { libsvm_params.Add($@"{prediction_file_param}"); }

            //var exe_file = $@"C:\libsvm-3.24\windows\svm-predict.exe";

            var args = String.Join(" ", libsvm_params);

            var start = new ProcessStartInfo
            {
                FileName = libsvm_predict_exe_file,
                Arguments = args,
                UseShellExecute = false,
                CreateNoWindow = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                RedirectStandardInput=true,
                //WorkingDirectory = Path.GetDirectoryName(exe_file) ?? ""
            };

            var cmd_line = string.Join(" ", libsvm_predict_exe_file, args);



            var priority_boost_enabled = false;
            var priority_class = ProcessPriorityClass.High;

            var retry_index = -1;
            var retry = false;
            do
            {
                retry_index++;

                try
                {
                    using (var process = Process.Start(start))
                    {
                        if (process == null)
                        {
                            retry = true;
                            continue;
                            //   return (cmd_line, null, null);
                        }

                        io_proxy.WriteLine($"Spawned process: {process.Id}", nameof(libsvm), nameof(predict));
                        try { process.PriorityBoostEnabled = priority_boost_enabled; } catch (Exception) { }
                        try { process.PriorityClass = priority_class; } catch (Exception) { }

                        var stdout = process.StandardOutput.ReadToEndAsync();
                        var stderr = process.StandardError.ReadToEndAsync();

                        process.WaitForExit();
                        io_proxy.WriteLine("Exited process: " + process.Id, nameof(libsvm), nameof(predict));

                        var tasks = new List<Task>() { stdout, stderr };

                        try { Task.WaitAll(tasks.ToArray<Task>()); } catch (Exception) { }

                        var stdout_result = "";
                        var stderr_result = "";

                        try { stdout_result = stdout?.Result; } catch (Exception) { }
                        try { stderr_result = stderr?.Result; } catch (Exception) { }


                        if (!string.IsNullOrWhiteSpace(stdout_file) && !string.IsNullOrWhiteSpace(stdout_result))
                        {
                            io_proxy.AppendAllText(stdout_file, stdout_result);
                        }

                        if (!string.IsNullOrWhiteSpace(stderr_file) && !string.IsNullOrWhiteSpace(stderr_result))
                        {
                            io_proxy.AppendAllText(stderr_file, stderr_result);
                        }

                        return (cmd_line, stdout_result, stderr_result);
                    }
                }
                catch (Exception e)
                {
                    retry = true;
                    io_proxy.WriteLine(e.ToString(), nameof(libsvm), nameof(predict));
                    Task.Delay(new TimeSpan(0, 0, 0, 10)).Wait();
                }
            }
            while (retry && retry_index < 10_000);

            return (cmd_line, null, null);
        }
    }
}
