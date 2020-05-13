using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime;
using System.Runtime.Loader;
using System.Threading;
using System.Threading.Tasks;

namespace svm_fs
{
    internal static class program
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

        internal static void close_notifications(CancellationTokenSource cts)
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

        public static void check_x64()
        {
            bool is64Bit = IntPtr.Size == 8;
            if (!is64Bit)
            {
                throw new Exception("Must run in 64bit mode");
            }
        }

        public static void set_gc_mode()
        {
            //GCSettings.LatencyMode = GCLatencyMode.Batch;
            GCSettings.LatencyMode = GCLatencyMode.SustainedLowLatency;
        }

        internal static void Main(string[] args)
        {
            //-cm ctl -ji 52887.kuhpcmgt-vip.kuhpc.local -jn svm_fs_ctl_0 -ai -ac 0 -of

            //var x = new cmd_params();
            //var a =x.get_options(true);
            //var b = x.get_options(false);
            //var c = x.get_options_ini_text(true);
            //var d = x.get_options_ini_text(false);
            //return;

            io_proxy.WriteLine(Environment.CommandLine);

            var cts = new CancellationTokenSource();
            close_notifications(cts);
            check_x64();
            set_gc_mode();

            //var x0 = new string[] { "f1", "f1_ppf", "f1_ppg" };
            //var x1 = new string[] { "yes pre-filter", "no pre-filter" };
            //var x2 = new string[] { "yes inner-cv", "no inner-cv" };
            //var x3 = new string[] { "2d", "3d", "2d and 3d", "2d then 3d", "3d then 2d" };
            //var x4 = new string[] { "Interface", "Neighbourhood", "Protein", "Interface", "Interface Neighbourhood Protein", "Interface Neighbourhood", "Interface Protein", "Neighbourhood Protein"  };


            //var required_default = false;

            //var required_matches = new List<(bool required, string alphabet, string dimension, string category, string source, string group, string member, string perspective)>();

            //required_matches.Add((required: true, alphabet: null, dimension: null, category: null, source: null, group: null, member: null, perspective: null));
            //required_matches.Add((required: true, alphabet: null, dimension: null, category: null, source: null, group: null, member: null, perspective: null));


            //var tp = new cmd_params();

            //var dataset = dataset_loader.read_binary_dataset($@"e:\input\", "2i", tp.negative_class_id, tp.positive_class_id, tp.class_names, use_parallel: true, perform_integrity_checks: true, fix_double: false, required_default: required_default, required_matches: required_matches, fix_dataset: true);
            //return;

            //var cms = performance_measure.confusion_matrix.load(@"C:\Temp\svm_fs\results\iteration_0\group_Normal.1.aa_oaac.subsequence_1d.aa_unsplit_oaac_Normal_dist_normal._._\iteration_0_group_2.test_predict_cm_all.csv", 68);
            //return;



            //var sw_program = new Stopwatch();
            //sw_program.Start();



            var cm_arg_index = args.ToList().FindIndex(a => a == "-cm");
            var ji_arg_index = args.ToList().FindIndex(a => a == "-ji");
            var jn_arg_index = args.ToList().FindIndex(a => a == "-jn");
            var ai_arg_index = args.ToList().FindIndex(a => a == "-ai");
            var ac_arg_index = args.ToList().FindIndex(a => a == "-ac");
            var in_arg_index = args.ToList().FindIndex(a => a == "-in");
            var of_arg_index = args.ToList().FindIndex(a => a == "-of");
            var pc_arg_index = args.ToList().FindIndex(a => a == "-pc");
            var pt_arg_index = args.ToList().FindIndex(a => a == "-pt");
            var en_arg_index = args.ToList().FindIndex(a => a == "-en");

            var arg_key_indexes = new int[]
            {
                cm_arg_index ,
                ji_arg_index ,
                jn_arg_index ,
                ai_arg_index ,
                ac_arg_index ,
                in_arg_index ,
                of_arg_index ,
                pc_arg_index ,
                pt_arg_index ,
                en_arg_index ,
            };

            var pbs_job_index = "";
            var pbs_job_name = "";

            //var pbs_job_array_count = "";
            var input_file = "";
            //var options_filename = "";
            var options_filename_list = new List<string>();
            var cmd = svm_fs.cmd.none;

            var array_start = 0;
            var array_step = 1;

            int total_vcpus_per_wkr_process = 1;
            int total_wkr_vcpus = 1440 - (64 + 320);

            var experiment_name = "";

            if (cm_arg_index > -1 && args.Length - 1 >= cm_arg_index + 1 && !arg_key_indexes.Contains(cm_arg_index + 1)) cmd = (svm_fs.cmd)Enum.Parse(typeof(svm_fs.cmd), args[cm_arg_index + 1]);
            if (ji_arg_index > -1 && args.Length - 1 >= ji_arg_index + 1 && !arg_key_indexes.Contains(ji_arg_index + 1)) pbs_job_index = args[ji_arg_index + 1];
            if (jn_arg_index > -1 && args.Length - 1 >= jn_arg_index + 1 && !arg_key_indexes.Contains(jn_arg_index + 1)) pbs_job_name = args[jn_arg_index + 1];
            if (ai_arg_index > -1 && args.Length - 1 >= ai_arg_index + 1 && !arg_key_indexes.Contains(ai_arg_index + 1)) array_start = int.TryParse(args[ai_arg_index + 1], out var array_start2) ? array_start2 : 0;
            if (ac_arg_index > -1 && args.Length - 1 >= ac_arg_index + 1 && !arg_key_indexes.Contains(ac_arg_index + 1)) array_step = int.TryParse(args[ac_arg_index + 1], out var array_step2) ? array_step2 : 1;
            if (in_arg_index > -1 && args.Length - 1 >= in_arg_index + 1 && !arg_key_indexes.Contains(in_arg_index + 1)) input_file = args[in_arg_index + 1];
            if (of_arg_index > -1 && args.Length - 1 >= of_arg_index + 1 && !arg_key_indexes.Contains(of_arg_index + 1)) options_filename_list.Add(args[of_arg_index + 1]);
            if (pc_arg_index > -1 && args.Length - 1 >= pc_arg_index + 1 && !arg_key_indexes.Contains(pc_arg_index + 1)) total_vcpus_per_wkr_process = int.TryParse(args[pc_arg_index + 1], out var total_vcpus_per_process2) ? total_vcpus_per_process2 : 1;
            if (pt_arg_index > -1 && args.Length - 1 >= pt_arg_index + 1 && !arg_key_indexes.Contains(pt_arg_index + 1)) total_wkr_vcpus = int.TryParse(args[pt_arg_index + 1], out var total_wkr_vcpus2) ? total_wkr_vcpus2 : total_wkr_vcpus;
            if (en_arg_index > -1 && args.Length - 1 >= en_arg_index + 1 && !arg_key_indexes.Contains(en_arg_index + 1)) experiment_name = args[en_arg_index + 1];

            //io_proxy.WriteLine($@"pbs_job_index = ""{pbs_job_index}"", pbs_job_name = ""{pbs_job_name}"", pbs_job_array_index = ""{pbs_job_array_index}"", pbs_job_array_count = ""{pbs_job_array_count}"", input_file = ""{input_file}"", options_filename_list = ""{string.Join("; ", options_filename_list)}""");




            switch (cmd)
            {
                case cmd.wkr:
                    {
                        if (!string.IsNullOrWhiteSpace(input_file))
                        {
                            var file_data = io_proxy.ReadAllLines(input_file, nameof(program), nameof(Main));

                            for (var i = 0; i < array_step; i++)
                            {
                                if (file_data.Length - 1 >= array_start + i)
                                {
                                    var options_filename = file_data[array_start + i];

                                    if (!string.IsNullOrWhiteSpace(options_filename))
                                    {
                                        options_filename_list.Add(options_filename);
                                    }
                                }
                            }

                            options_filename_list = options_filename_list.Distinct().ToList();
                        }

                        //foreach (var options_filename in options_filename_list)
                        Parallel.For(0, options_filename_list.Count, options_filename_index =>
                        {
                            var options_filename = options_filename_list[options_filename_index];

                            var file_data = io_proxy.ReadAllLines(options_filename, nameof(program), nameof(Main));

                            var options = new cmd_params(file_data);

                            if (!string.IsNullOrWhiteSpace(experiment_name))
                            {
                                options.experiment_name = experiment_name;
                            }

                            svm_wkr.inner_cross_validation(options);
                            
                        });

                        break;
                    }


                case cmd.ctl:
                    {
                        var options = new cmd_params();
                        options.cmd = cmd;

                        if (options_filename_list.Count == 1)
                        {
                            options = new cmd_params(io_proxy.ReadAllLines(options_filename_list.First()));
                        }

                        if (!string.IsNullOrWhiteSpace(experiment_name))
                        {
                            options.experiment_name = experiment_name;
                        }
                        
                        svm_ctl.feature_selection(options);
                        break;
                    }


                case cmd.ldr:
                    {
                        svm_ldr.start_ldr(cts, experiment_name, total_vcpus_per_wkr_process, total_wkr_vcpus);
                        break;
                    }
            }
        }

    }
}



