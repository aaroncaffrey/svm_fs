﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime;
using System.Runtime.Loader;
using System.Threading;

namespace svm_fs
{
    public static class program
    {



        //args = new string[] { $@"C:\Temp\svm_fs\results\iteration_0\group_Normal.1.aa_average_seq_positions.subsequence_1d.aa_unsplit_average_seq_positions_Normal_dist_normal._._\rand_0_outerfold_0\iteration_0_group_4_rand_0_outerfold_0_job_4.options" };
        //start cmd /c C:\Users\aaron\Desktop\svm_fs\svm_fs\bin\Debug\netcoreapp3.0\svm_fs.exe 
        //args = new string[] { $@"C:\Temp\svm_fs\results\iteration_0\group_Normal.1.aa_motifs.subsequence_1d.aa_unsplit_motifs_1_Normal_dist_normal._._\rand_0\outer_fold_0\0_0_0_0_0.options" };
        //Cross Validation Accuracy = 76.0135%
        ////var exe_file = $@"c:\libsvm-3.24\windows\svm-train.exe";
        //var exe_file = $@"c:\libsvm-3.24\windows\svm-train.exe";
        ////var exe_file = $@"C:\Windows\System32\cmd.exe";
        //var psi = new ProcessStartInfo()
        //{
        //    FileName = exe_file,
        //    Arguments = "-b 1 -c 8192 -g 8 -m 1024 -q -v 10 c:\\temp\\iteration_0\\rand_0\\outer_fold_0\\group_0\\0_0_0_0_0.train.libsvm", //$@"c:\temp\iteration_0\rand_0\outer_fold_0\group_0\0_0_0_0_0.train.libsvm",
        //    UseShellExecute = false,
        //    CreateNoWindow = false,
        //    RedirectStandardOutput = true,
        //    RedirectStandardError = true,
        //    RedirectStandardInput =true,
        //};
        //var e = Process.Start(psi);
        //var x = e.StandardOutput.ReadToEnd();
        //io_proxy.WriteLine("Read: " + x);
        //return;
        //args = new string[] { $@"c:\temp\iteration_0\rand_0\outer_fold_0\group_0\0_0_0_0_0.options" };

        public static void close_notifications(CancellationTokenSource cts)
        {
            Console.CancelKeyPress += (sender, eventArgs) =>
            {
                io_proxy.WriteLine($@"Console.CancelKeyPress", nameof(program), nameof(Main));
                cts.Cancel();
            };
            AssemblyLoadContext.Default.Unloading += context =>
            {
                io_proxy.WriteLine($@"AssemblyLoadContext.Default.Unloading", nameof(program), nameof(Main));
                cts.Cancel();
            };
            AppDomain.CurrentDomain.ProcessExit += (sender, eventArgs) =>
            {
                io_proxy.WriteLine($@"AppDomain.CurrentDomain.ProcessExit", nameof(program), nameof(Main));
                cts.Cancel();
            };
        }

        public static void Main(string[] args)
        {
            GCSettings.LatencyMode = GCLatencyMode.Batch;

            var cts = new CancellationTokenSource();
            close_notifications(cts);

            //var x0 = new string[] { "f1", "f1_ppf", "f1_ppg" };
            //var x1 = new string[] { "yes filter", "no filter" };
            //var x2 = new string[] { "yes inner-cv", "no inner-cv" };
            //var x3 = new string[] { "2d", "3d", "2d and 3d", "2d then 3d", "3d then 2d" };
            //var x4 = new string[] { "Interface", "Neighbourhood", "Protein", "Interface", "Interface Neighbourhood Protein", "Interface Neighbourhood", "Interface Protein", "Neighbourhood Protein"  };

           
            var required_default = false;

            var required_matches = new List<(bool required, string alphabet, string dimension, string category, string source, string group, string member, string perspective)>();

            //required_matches.Add((required: true, alphabet: null, dimension: null, category: null, source: null, group: null, member: null, perspective: null));
            //required_matches.Add((required: true, alphabet: null, dimension: null, category: null, source: null, group: null, member: null, perspective: null));


            var tp = new cmd_params();

            //var dataset = dataset_loader.read_binary_dataset($@"e:\input\", "2i", tp.negative_class_id, tp.positive_class_id, tp.class_names, use_parallel: true, perform_integrity_checks: true, fix_double: false, required_default: required_default, required_matches: required_matches, fix_dataset: true);
            //return;

            //var cms = performance_measure.confusion_matrix.load(@"C:\Temp\svm_fs\results\iteration_0\group_Normal.1.aa_oaac.subsequence_1d.aa_unsplit_oaac_Normal_dist_normal._._\iteration_0_group_2.test_predict_cm_all.csv", 68);
            //return;

            bool is64Bit = IntPtr.Size == 8;
            if (!is64Bit)
            {
                throw new Exception("Must run in 64bit mode");
            }

            var sw_program = new Stopwatch();
            sw_program.Start();


            var options = new cmd_params();
            //options.options_filename = Path.Combine(options.pbs_submission_directory, $@"");

           

            //AppDomain.ProcessExit = AppDomainOnProcessExit;
            var bootstrap = true;

            if (args.Length > 0 && args[0].Length > 0)
            {
                var options_index = 0;

                if (args[0] == "-d")
                {
                    // print default settings and exit

                    var ot = options.get_options_ini_text().ToList();

                    ot.ForEach(a => io_proxy.WriteLine(a));

                    return;
                }

                if (args[0] == "-j")
                {
                    // run job

                    bootstrap = false;
                    options_index = 1;
                }

                var options_filename = args[options_index];
                options_filename = io_proxy.convert_path(options_filename);

                if (io_proxy.is_file_available(options_filename))
                {
                    var file_data = io_proxy.ReadAllLines(options_filename, nameof(program), nameof(Main));
                    options = new cmd_params(file_data);
                    options.options_filename = options_filename;
                }
                else
                {
                    io_proxy.WriteLine($@"File not available: {options_filename}", nameof(program), nameof(Main));
                    return;
                }
            }
            

            if (options.cmd == cmd.none)
            {
                var test_params = new cmd_params(options)
                {
                    experiment_name = "program_test",
                    options_filename = io_proxy.convert_path(Path.Combine(options.pbs_ctl_submission_directory, $@"{nameof(svm_ctl)}.options")),
                    cmd = cmd.ctl,
                }.get_options_ini_text();

                options = new cmd_params(test_params);

                io_proxy.WriteAllLines(options.options_filename, options.get_options_ini_text());
            }

            if (bootstrap)
            {
                if (options.cmd == cmd.ctl)
                {


                    svm_ldr.start(options, cts);
                    
                }
                else
                {
                    throw new ArgumentOutOfRangeException();
                }
            }
            else
            {
                switch (options.cmd)
                {
                    case cmd.ctl:
                        svm_ctl.feature_selection(options);
                        break;

                    case cmd.wkr:
                        svm_wkr.cross_validation(options);
                        break;

                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }
            
            sw_program.Stop();
            
            io_proxy.WriteLine($@"Exiting {(bootstrap?"bootstrap":"job")}: {options.cmd}. Elapsed: {sw_program.Elapsed.ToString()}", nameof(program), nameof(Main));
        }

    }
}
