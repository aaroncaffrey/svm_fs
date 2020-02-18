﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime;
using System.Threading;
using System.Threading.Tasks;

namespace svm_fs
{
    public static class svm_ctl
    {
        public static List<(int randomisation_cv_index, int outer_cv_index, List<int> indexes)> folds(int examples, int outer_cv_folds, int randomisation_cv_folds, int? size_limit = null)
        {
            /*int first_index, int last_index, int num_items,*/

            var fold_sizes = new int[outer_cv_folds];

            if (examples > 0 && outer_cv_folds > 0)
            {
                var n = examples / outer_cv_folds;

                for (var i = 0; i < outer_cv_folds; i++)
                {
                    fold_sizes[i] = n;
                }

                for (var i = 0; i < (examples % outer_cv_folds); i++)
                {
                    fold_sizes[i]++;
                }
            }

            if (size_limit != null)
            {
                fold_sizes = fold_sizes.Select(a => a > size_limit ? size_limit.Value : a).ToArray();
            }

            var rand = new Random(1);

            var indexes_pool = Enumerable.Range(0, examples).ToList();

            var x = new List<(int randomisation, int fold, List<int> indexes)>();

            var rdm = randomisation_cv_folds == 0 ? 1 : randomisation_cv_folds;

            for (var r = 0; r < rdm; r++)
            {
                if (randomisation_cv_folds != 0) indexes_pool.shuffle(rand);

                var y = fold_sizes.Select((fold_size, fold_index) => (randomisation_cv_index: r, outer_cv_index: fold_index, indexes: indexes_pool.Skip(fold_sizes.Where((b, j) => fold_index < j).Sum()).Take(fold_size).OrderBy(b => b).ToList())).ToList();

                x.AddRange(y);
            }

            return x;
        }

        public static double standard_deviation_population(List<double> values)
        {
            if (values.Count == 0) return 0;

            var mean = values.Average();

            return Math.Sqrt(values.Sum(x => Math.Pow(x - mean, 2)) / (values.Count));
        }

        public static double standard_deviation_sample(List<double> values)
        {
            if (values.Count < 2) return 0;

            var mean = values.Average();

            return Math.Sqrt(values.Sum(x => Math.Pow(x - mean, 2)) / (values.Count - 1));
        }

        public static double sqrt_sumofsqrs(List<double> list)
        {
            return Math.Sqrt(list.Sum(a => Math.Abs(a) * Math.Abs(a)));
        }

        public static double scale(double value, List<double> list, double non_zero, double abs_sum, double srsos, double column_min, double column_max, double average, double stdev, common.scale_function scale_function)
        {
            switch (scale_function)
            {
                case common.scale_function.none:
                    return value;

                case common.scale_function.rescale:
                    var scale_min = -1;
                    var scale_max = +1;

                    var x = (scale_max - scale_min) * (value - column_min);
                    var y = (column_max - column_min);
                    var z = scale_min;

                    if (y == 0) return 0;

                    var rescale = (x / y) + z;

                    return rescale;

                case common.scale_function.normalisation:

                    if (column_max - column_min == 0) return 0;

                    var mean_norm = (value - average) / (column_max - column_min);

                    return mean_norm;

                case common.scale_function.standardisation:

                    if (stdev == 0) return 0;

                    var standardisation = (value - average) / stdev;

                    return standardisation;

                case common.scale_function.L0_norm:

                    if (non_zero == 0) return 0;

                    return value / non_zero;

                case common.scale_function.L1_norm:

                    if (abs_sum == 0) return 0;

                    return value / abs_sum;

                case common.scale_function.L2_norm:

                    if (srsos == 0) return 0;

                    return value / srsos;

                default:
                    throw new ArgumentOutOfRangeException(nameof(scale_function));//return 0;
            }
        }

        public static int for_loop_instance_id(List<(int current, int max)> points)
        {
            //var jid = (i * max_j * max_k) + (j * max_k) + k;
            //var job_id = (i * max_j * max_k * max_l) + (j * max_k * max_l) + (k * max_l) + l;
            var v = 0;

            for (var i = 0; i < points.Count - 1; i++)
            {
                var t = points[i].current;
                for (var j = i + 1; j < points.Count; j++)
                {
                    t = t * points[j].max;
                }
                v = v + t;
            }
            v = v + points.Last().current;
            return v;
        }

        public static void wait_tasks(Task[] tasks, string module_name = "", string function_name = "")
        {
            if (tasks == null || tasks.Length == 0) return;

            //var sw1 = new Stopwatch();
            //sw1.Start();

            var start_time = DateTime.Now;

            do
            {
                var incomplete_tasks = tasks.Where(a => !a.IsCompleted).ToList();

                if (incomplete_tasks.Count > 0)
                {
                    var completed_task_index = Task.WaitAny(incomplete_tasks.ToArray<Task>());

                    if (completed_task_index > -1)
                    {
                        var incomplete = tasks.Count(a => !a.IsCompleted);
                        var complete = tasks.Length - incomplete;
                        var pct = ((double)complete / (double)tasks.Length) * 100;

                        var time_remaining = TimeSpan.FromTicks((long)(DateTime.Now.Subtract(start_time).Ticks * ((double)incomplete / (double)(complete == 0 ? 1 : complete))));

                        io_proxy.WriteLine($@"{module_name}.{function_name} -> ", nameof(svm_ctl), nameof(feature_selection));
                        io_proxy.WriteLine($@"{module_name}.{function_name} -> {complete} / {tasks.Length} ( {pct:0.00} % ) [ {time_remaining:dd\:hh\:mm\:ss\.fff} ]", nameof(svm_ctl), nameof(wait_tasks));
                        io_proxy.WriteLine($@"{module_name}.{function_name} -> Memory usage: {(GC.GetTotalMemory(false) / 1_000_000_000d):F2} GB", nameof(svm_ctl), nameof(feature_selection));
                    }
                }

                //GC.Collect();
            } while (tasks.Any(a => !a.IsCompleted));

            Task.WaitAll(tasks.ToArray<Task>());
        }

        public static void feature_selection(cmd_params p)
        {
            //p.feature_count = 0;
            //p.group_count = 0;

            // Load dataset
            //io_proxy.WriteLine("Start: Loading dataset...");



            var required_default = false;

            var required_matches = new List<(bool required, string alphabet, string dimension, string category, string source, string group, string member, string perspective)>();

            //required_matches.Add((required: true, alphabet: null, dimension: null, category: null, source: null, group: null, member: null, perspective: null));

            //required_matches.Add((required: true, alphabet: null, dimension: null, category: null, source: null, group: null, member: null, perspective: null));



            var dataset = dataset_loader.read_binary_dataset(
                p.dataset_dir,
                "2i",
                p.negative_class_id,
                p.positive_class_id,
                p.class_names,
                use_parallel: true,
                perform_integrity_checks: true,
                fix_double: false,
                required_default,
                required_matches
                );



            //io_proxy.WriteLine("End: Loading dataset...");

            var class_ids = dataset.dataset_instance_list.Select(a => a.class_id).Distinct().ToList();

            var class_sizes = class_ids.Select(a => (class_id: a, class_size: dataset.dataset_instance_list.Count(b => b.class_id == a))).ToList();
            p.class_sizes = class_sizes;

            var class_folds = class_sizes.Select(a => (class_id: a.class_id, size: a.class_size, folds: svm_ctl.folds(a.class_size, p.outer_cv_folds, p.randomisation_cv_folds /*, p.class_sizes.Min(m => m.class_size)*/
            ))).ToList();

            var downsampled_training_class_folds = class_folds.Select(a => (class_id: a.class_id, size: a.size, folds: a.folds.Select(b =>
            {
                var min_num_items_in_fold = class_folds.Min(c => c.folds.Where(e => e.randomisation_cv_index == b.randomisation_cv_index && e.outer_cv_index == b.outer_cv_index).Min(e => e.indexes.Count));

                return (randomisation_cv_index: b.randomisation_cv_index, outer_cv_index: b.outer_cv_index, indexes: b.indexes.Take(min_num_items_in_fold).ToList());
            }).ToList())).ToList();

            var dataset_instance_list_grouped = dataset.dataset_instance_list.GroupBy(a => a.class_id).Select(a => (class_id: a.Key, examples: a.ToList())).ToList();

            //var alphabet = b[1];
            //var dimension = b[2];
            //var category = b[3];
            //var source = b[4];
            //var group = b[5];
            //var member = b[6];
            //var perspective = b[7];

            var groups = p.group_features ? dataset.dataset_headers.GroupBy(a => (a.alphabet, a.dimension, a.category, a.source, a.group, member: "", perspective: "")).Skip(1).Select((a, i) => (index: i, key: a.Key, list: a.ToList(), columns: a.Select(b => b.fid).ToList())).ToList() : dataset.dataset_headers.GroupBy(a => a.fid).Skip(1).Select((a, i) => (index: i, key: (a.First().alphabet, a.First().dimension, a.First().category, a.First().source, a.First().group, a.First().member, a.First().perspective), list: a.ToList(), columns: a.Select(b => b.fid).ToList())).ToList();
            var group_keys = groups.Select(a => a.key).ToList();
            var group_keys_distinct = group_keys.Distinct().ToList();

            if (group_keys.Count != group_keys_distinct.Count) throw new Exception();

            io_proxy.WriteLine("", nameof(svm_ctl), nameof(feature_selection));
            io_proxy.WriteLine($@"--------------- Performing greedy feature selection on {groups.Count} groups ---------------", nameof(svm_ctl), nameof(feature_selection));

            //groups = groups.Take(10).ToList();

            //group count -- total groups currently selected/testing
            //feature count -- total features currently selected/testing

            //io_proxy.WriteLine("", nameof(svm_ctl), nameof(interactive));
            //io_proxy.WriteLine($@"--------------- Performing greedy feature selection on {groups.Count} groups ---------------", nameof(svm_ctl), nameof(interactive));

            var iteration_index = -1;
            var winning_iteration = 0;
            //var last_winning_iteration = 0;

            var highest_score_last_iteration_group_index = -1;
            var highest_score_this_iteration_group_index = -1;

            (string alphabet, string dimension, string category, string source, string @group, string member, string perspective) highest_score_last_iteration_group_key = default;
            (string alphabet, string dimension, string category, string source, string @group, string member, string perspective) highest_score_this_iteration_group_key = default;

            var iterations_not_better_than_last = 0;
            var iterations_not_better_than_all = 0;
            var currently_selected_group_indexes = new List<int>();
            var highest_scoring_group_indexes = new List<int>();
            var highest_score_last_iteration = 0d;
            var highest_score_this_iteration = 0d;
            var highest_score_all_iteration = 0d;
            var score_increase_from_last = 0d;
            var score_increase_from_all = 0d;
            var score_better_than_last = false;
            var score_better_than_all = false;
            var sw_fs = new Stopwatch();

            var previous_tests = new List<List<int>>();

            var limit_iteration_not_better_than_last = 5;
            var limit_iteration_not_better_than_all = 10;

            var root_folder = io_proxy.convert_path(p.results_root_folder);
            var summary_fn = io_proxy.convert_path(Path.Combine(root_folder, "summary.csv"));

            var summary_lines = new List<string>();
            
            //lock (wait_ready_lock)
            //{
            //    wait_items = groups.Count;
            //    wait_items_ready = 0;
            //    wait_ready = false;
            //}

            sw_fs.Start();

            do
            {
                highest_score_last_iteration_group_index = highest_score_this_iteration_group_index;
                highest_score_last_iteration_group_key = highest_score_this_iteration_group_key;
                highest_score_this_iteration_group_key = default;
                highest_score_this_iteration_group_index = -1;
                highest_score_last_iteration = highest_score_this_iteration;
                highest_score_this_iteration = 0d;
                score_better_than_last = false;
                score_better_than_all = false;
                score_increase_from_last = 0d;
                score_increase_from_all = 0d;
                iteration_index++;

                io_proxy.WriteLine("", nameof(svm_ctl), nameof(feature_selection));
                io_proxy.WriteLine($@"--------------- Start of iteration {iteration_index} ---------------", nameof(svm_ctl), nameof(feature_selection));
                io_proxy.WriteLine("", nameof(svm_ctl), nameof(feature_selection));

                var sw_iteration = new Stopwatch();
                sw_iteration.Start();


                var iteration_folder =   Path.Combine(p.results_root_folder, $@"itr_{iteration_index}");
                var checkpoint_fn =      Path.Combine(p.results_root_folder, $@"itr_{iteration_index}", $@"{nameof(currently_selected_group_indexes)}.txt");
                var previous_tests_fn =  Path.Combine(p.results_root_folder, $@"itr_{iteration_index}", $@"previous_tests.txt");

                if (io_proxy.is_file_available(checkpoint_fn))
                {
                    io_proxy.WriteLine($@"--------------- Checkpoint loaded: {checkpoint_fn} ---------------", nameof(svm_ctl), nameof(feature_selection));


                    currently_selected_group_indexes.Clear();
                    highest_scoring_group_indexes.Clear();

                    var checkpoint_data1 =    io_proxy.ReadAllLines(checkpoint_fn, nameof(svm_ctl), nameof(feature_selection));
                    var previous_tests_data = io_proxy.ReadAllLines(previous_tests_fn, nameof(svm_ctl), nameof(feature_selection));
                    previous_tests = previous_tests_data.Select(a => a.Split(';').Select(b => int.Parse(b, CultureInfo.InvariantCulture)).ToList()).ToList();

                    foreach (var cpd in checkpoint_data1)
                    {
                        var cpd_key = cpd.Substring(0, cpd.IndexOf('='));
                        var cpd_value = cpd.Substring(cpd_key.Length + 1);

                        switch (cpd_key)
                        {
                            case nameof(iteration_index):// "iteration_index":
                                iteration_index = int.Parse(cpd_value, CultureInfo.InvariantCulture);
                                break;

                            case nameof(winning_iteration):// winning_iteration":
                                winning_iteration = int.Parse(cpd_value, CultureInfo.InvariantCulture);
                                break;

                            case nameof(iterations_not_better_than_last):// "iterations_not_better_than_last":
                                iterations_not_better_than_last = int.Parse(cpd_value, CultureInfo.InvariantCulture);
                                break;

                            case nameof(iterations_not_better_than_all):// "iterations_not_better_than_all":
                                iterations_not_better_than_all = int.Parse(cpd_value, CultureInfo.InvariantCulture);
                                break;

                            case nameof(currently_selected_group_indexes):// "currently_selected_group_indexes":
                                currently_selected_group_indexes.AddRange(cpd_value.Split(';').Select(a => int.Parse(a, CultureInfo.InvariantCulture)).ToList());
                                break;

                            case nameof(highest_scoring_group_indexes):// "highest_scoring_group_indexes":
                                highest_scoring_group_indexes.AddRange(cpd_value.Split(';').Select(a => int.Parse(a, CultureInfo.InvariantCulture)).ToList());
                                break;

                            case nameof(highest_score_last_iteration):// "highest_score_last_iteration":
                                highest_score_last_iteration = double.Parse(cpd_value, NumberStyles.Float, CultureInfo.InvariantCulture);
                                break;

                            case nameof(highest_score_this_iteration):// "highest_score_this_iteration":
                                highest_score_this_iteration = double.Parse(cpd_value, NumberStyles.Float, CultureInfo.InvariantCulture);
                                break;

                            case nameof(highest_score_all_iteration):// "highest_score_all_iteration":
                                highest_score_all_iteration = double.Parse(cpd_value, NumberStyles.Float, CultureInfo.InvariantCulture);
                                break;

                            case nameof(score_increase_from_last):// "score_increase_from_last":
                                score_increase_from_last = double.Parse(cpd_value, NumberStyles.Float, CultureInfo.InvariantCulture);
                                break;

                            case nameof(score_increase_from_all):// "score_increase_from_all":
                                score_increase_from_all = double.Parse(cpd_value, NumberStyles.Float, CultureInfo.InvariantCulture);
                                break;

                            case nameof(score_better_than_last):// "score_better_than_last":
                                score_better_than_last = bool.Parse(cpd_value);
                                break;

                            case nameof(score_better_than_all):// "score_better_than_all":
                                score_better_than_all = bool.Parse(cpd_value);
                                break;

                            default:
                                break;
                        }
                    }

                    currently_selected_group_indexes = currently_selected_group_indexes.Distinct().OrderBy(a => a).ToList();
                    highest_scoring_group_indexes = highest_scoring_group_indexes.Distinct().OrderBy(a => a).ToList();


                    continue;
                }
                else
                {


                    // 1. iteration winning score
                    // 2. selected features


                    //io_proxy.WriteLine($@"--------------- Start of iteration {iteration_index} ---------------", nameof(svm_ctl), nameof(interactive));

                    io_proxy.WriteLine("", nameof(svm_ctl), nameof(feature_selection));
                    io_proxy.WriteLine($@"Memory usage: {(GC.GetTotalMemory(false) / 1_000_000_000d):F2} GB", nameof(svm_ctl), nameof(feature_selection));
                    io_proxy.WriteLine("", nameof(svm_ctl), nameof(feature_selection));

                    //score_improved = false;
                    //var _iteration_index = iteration_index;
                    var currently_selected_groups = currently_selected_group_indexes.Select(a => groups[a]).ToList();
                    var currently_selected_groups_columns = currently_selected_groups.SelectMany(a => a.columns).OrderBy(a => a).Distinct().ToList();

                    //var jobs_group_level = Enumerable.Range(0, groups.Count).AsParallel().AsOrdered().Select(group_index =>


                    /*var jobs_group_level = new 
                        List<(
                            List<(List<string> wait_file_list, cmd_params cmd_params, cmd_params merge_cmd_params, List<(bool wait_first, bool has_header, string average_out_filename, string merge_out_filename, string merge_in_filename, string average_header)> to_merge)> jobs_randomisation_level,
                            List<((string test_file, string test_comments_file, string prediction_file, string cm_file) filenames, (List<performance_measure.prediction> prediction_list, List<performance_measure.confusion_matrix> cm_list) cms, cmd_params cmd_params)> merge_cm_inputs, 
                            List<int> this_test_group_indexes
                        )>();
                        */
                    //var group_tasks = new List<Task<(
                    //    List<(List<string> wait_file_list, cmd_params cmd_params, cmd_params merge_cmd_params, List<(bool wait_first, bool has_header, string average_out_filename, string merge_out_filename, string merge_in_filename, string average_header)> to_merge)> jobs_randomisation_level,
                    //    List<((string test_file, string test_comments_file, string prediction_file, string cm_file) filenames, (List<performance_measure.prediction> prediction_list, List<performance_measure.confusion_matrix> cm_list) cms, cmd_params cmd_params)> merge_cm_inputs,
                    //    List<int> this_test_group_indexes
                    //    )>>();

                    var group_tasks1 =
                        new List<
                            Task<
                                (
                                List<string> wait_file_list, 
                                cmd_params p, 
                                List<int> this_test_group_indexes, 
                                List<(bool wait_first, bool has_header, string average_out_filename, string merge_out_filename, string merge_in_filename, string average_header)> post_tm, 
                                List<(List<string> wait_file_list, cmd_params cmd_params, cmd_params merge_cmd_params, List<(bool wait_first, bool has_header, string average_out_filename, string merge_out_filename, string merge_in_filename, string average_header)> to_merge)> rets,
                                List<cmd_params> wkr_cmd_params_list
                                )>>();
                    
                    //GCSettings.IsServerGC
                    for (var _group_index = 0; _group_index < groups.Count; _group_index++)
                    {
                        var group_index = _group_index;

                        var task = Task.Run(() =>
                        {
                            //GC.TryStartNoGCRegion(1024 * 1000 * 1000);
                            var is_group_index_selected = currently_selected_group_indexes.Contains(group_index);

                            var is_forward = !is_group_index_selected;

                            var this_test_group_indexes = currently_selected_group_indexes.ToList();


                            if (is_forward)
                            {
                                this_test_group_indexes.Add(group_index);
                            }
                            else
                            {
                                this_test_group_indexes.Remove(group_index);

                                // do not remove if the only selected feature 
                                if (currently_selected_group_indexes.Count == 1)
                                {
                                    io_proxy.WriteLine(
                                        $@"{nameof(iteration_index)}={iteration_index}, {nameof(group_index)}={group_index}, currently_selected_group_indexes.Count == 1",
                                        nameof(svm_ctl), nameof(feature_selection));

                                    //jobs_group_level.Add(default);
                                    return default;
                                }


                                // do not remove if was just added
                                if (highest_score_last_iteration_group_index == group_index)
                                {
                                    io_proxy.WriteLine(
                                        $@"{nameof(iteration_index)}={iteration_index}, {nameof(group_index)}={group_index}, highest_score_last_iteration_group_index == group_index",
                                        nameof(svm_ctl), nameof(feature_selection));

                                    //jobs_group_level.Add(default);

                                    return default;
                                }
                            }

                            this_test_group_indexes = this_test_group_indexes.OrderBy(a => a).Distinct().ToList();


                            var already_tested = previous_tests.Any(a => a.SequenceEqual(this_test_group_indexes));

                            // do not repeat the same test
                            if (already_tested)
                            {

                                io_proxy.WriteLine(
                                    $@"{nameof(iteration_index)}={iteration_index}, {nameof(group_index)}={group_index}, already tested: {string.Join(",", this_test_group_indexes)}",
                                    nameof(svm_ctl), nameof(feature_selection));

                                //jobs_group_level.Add(default);

                                return default;
                            }

                            var group = group_index > -1 ? groups[group_index] : default;

                            var group_key = group.key;

                            var old_group_count = currently_selected_groups.Count;
                            var new_group_count =
                                this_test_group_indexes.Count; //old_group_count + (is_group_index_selected ? -1 : 1);

                            var query_cols = is_group_index_selected
                                ? currently_selected_groups_columns.Except(group.columns).OrderBy(a => a).Distinct()
                                    .ToList()
                                : currently_selected_groups_columns.Union(group.columns).OrderBy(a => a).Distinct()
                                    .ToList();

                            var old_feature_count = currently_selected_groups_columns.Count;
                            var new_feature_count = query_cols.Count;




                            var jobs_randomisation_level = group_cv_part1(
                                0,
                                p,
                                downsampled_training_class_folds,
                                class_folds,
                                dataset_instance_list_grouped,
                                iteration_index,
                                group_index,
                                groups,
                                query_cols,
                                is_forward,
                                old_feature_count,
                                new_feature_count,
                                old_group_count,
                                new_group_count,
                                group_key,
                                this_test_group_indexes);

                                return jobs_randomisation_level;

                                //jobs_group_level.Add(jobs_randomisation_level);




                                //GC.EndNoGCRegion();
                        });


                        group_tasks1.Add(task);
                    }//).ToList();

                    wait_tasks(group_tasks1.ToArray<Task>(), nameof(svm_ctl), nameof(feature_selection));
                    var jobs_group_level_part1 = group_tasks1.Select(a => a.Result)/*.Where(a=> a != default)*/.ToList();
                    group_tasks1.Clear();

                    var wait_file_list = jobs_group_level_part1.SelectMany(a => a.wait_file_list).ToList();
                    wait_for_results(wait_file_list);

                    io_proxy.WriteLine("", nameof(svm_ctl), nameof(feature_selection));
                    io_proxy.WriteLine($@"Memory usage: {(GC.GetTotalMemory(false) / 1_000_000_000d):F2} GB", nameof(svm_ctl), nameof(feature_selection));
                    io_proxy.WriteLine("", nameof(svm_ctl), nameof(feature_selection));

                    if (jobs_group_level_part1 == null || jobs_group_level_part1.Count == 0)
                    {
                        io_proxy.WriteLine($@"{nameof(iteration_index)}={iteration_index}: jobs_group_level == null || jobs_group_level.Count == 0", nameof(svm_ctl), nameof(feature_selection));

                        break;
                    }

                    var group_tasks2 = new List<Task<(List<(List<string> wait_file_list, cmd_params cmd_params, cmd_params merge_cmd_params, List<(bool wait_first, bool has_header, string average_out_filename, string merge_out_filename, string merge_in_filename, string average_header)> to_merge)> jobs_randomisation_level, List<((string test_file, string test_comments_file, string prediction_file, string cm_file) filenames, (List<performance_measure.prediction> prediction_list, List<performance_measure.confusion_matrix> cm_list) cms, cmd_params cmd_params)> merge_cm_inputs, List<int> this_test_group_indexes)>>();

                    for (var _group_index = 0; _group_index < groups.Count; _group_index++)
                    {
                        var group_index = _group_index;

                        var task = Task.Run(() =>
                        {
                            var part1_result = jobs_group_level_part1[group_index];

                            if (part1_result == default) return default;

                            var part2_result = group_cv_part2(part1_result);

                            return part2_result;
                        });


                        group_tasks2.Add(task);
                    }


                    wait_tasks(group_tasks2.ToArray<Task>(), nameof(svm_ctl), nameof(feature_selection));
                    var jobs_group_level_part2 = group_tasks2.Select(a => a.Result)/*.Where(a=> a != default)*/.ToList();
                    group_tasks2.Clear();

                    jobs_group_level_part2.ForEach(a => previous_tests.Add(a.this_test_group_indexes));

                    // get ranks by group
                    var cm_inputs = get_cm_inputs(p, jobs_group_level_part2, groups, iteration_folder, iteration_index);

                    if (cm_inputs == null || cm_inputs.Count <= 0)
                    {
                        io_proxy.WriteLine($@"{nameof(iteration_index)}={iteration_index}: cm_inputs == null || cm_inputs.Count <= 0", nameof(svm_ctl), nameof(feature_selection));

                        break;
                    }


                    // add best group to selected groups


                    //currently_selected_group_indexes.Add(cm_inputs.First().cmd_params.group_index);


                    var winner = cm_inputs.First();
                    var winner_group = groups[winner.cmd_params.group_index];

                    var forward = !currently_selected_group_indexes.Contains(winner.cmd_params.group_index);

                    highest_score_this_iteration_group_index = winner.cmd_params.group_index;
                    highest_score_this_iteration_group_key = winner_group.key;

                    highest_score_this_iteration = winner.cms.cm_list.Where(b => p.feature_selection_classes == null || p.feature_selection_classes.Count == 0 || p.feature_selection_classes.Contains(b.class_id.Value)).Average(b => b.get_perf_value_strings().Where(c => p.feature_selection_metrics.Any(d => string.Equals(c.name, d, StringComparison.InvariantCultureIgnoreCase))).Average(c => c.value));

                    score_increase_from_last = highest_score_this_iteration - highest_score_last_iteration;
                    score_increase_from_all = highest_score_this_iteration - highest_score_all_iteration;

                    var score_increase_from_last_pct = highest_score_last_iteration != 0 ? score_increase_from_last / highest_score_last_iteration : 0;
                    var score_increase_from_all_pct = highest_score_last_iteration != 0 ? score_increase_from_all / highest_score_all_iteration : 0;

                    score_better_than_last = score_increase_from_last > 0;
                    score_better_than_all = score_increase_from_all > 0;


                    iterations_not_better_than_last = score_better_than_last ? 0 : iterations_not_better_than_last + 1;
                    iterations_not_better_than_all = score_better_than_all ? 0 : iterations_not_better_than_all + 1;




                    //if (score_better_than_last) // commented out so that more iterations can be tried to not get trapped in a low local optima
                    //{
                    if (forward)
                    {
                        currently_selected_group_indexes.Add(winner.cmd_params.group_index);
                    }
                    else
                    {
                        currently_selected_group_indexes.RemoveAll(a => a == winner.cmd_params.group_index);
                    }

                    currently_selected_group_indexes = currently_selected_group_indexes.OrderBy(a => a).Distinct().ToList();
                    //}

                    if (score_better_than_all)
                    {
                        winning_iteration = iteration_index;

                        highest_score_all_iteration = highest_score_this_iteration;

                        highest_scoring_group_indexes = currently_selected_group_indexes.ToList();
                    }

                    io_proxy.WriteLine($@"Score {(score_better_than_last ? "improved" : "not improved")} from last iteration; score {(score_better_than_last ? "increased" : "decreased")} by {score_increase_from_last} from {highest_score_last_iteration} to {highest_score_this_iteration}.", nameof(svm_ctl), nameof(feature_selection));
                    io_proxy.WriteLine($@"Best score {(score_better_than_last ? "is" : "would have been")} to {(forward ? "add" : "remove")} group {winner.cmd_params.group_index} {groups[winner.cmd_params.group_index].key}", nameof(svm_ctl), nameof(feature_selection));

                    // highest_score_last_iteration


                    // 1. check if grid search results cache are loaded?
                    // 2. rerun file if kernel refuses

                    // 1. per feature: output cm per outer-fold
                    // 2. per feature: combine outer-fold cm into one file
                    // 3. per feature: output cm (all results combined into one prediction file)
                    // 4. all features: output cm with features ordered by predictive ability rank

                    io_proxy.WriteLine($@"", nameof(svm_ctl), nameof(feature_selection));
                    io_proxy.WriteLine($@"Memory usage: {(GC.GetTotalMemory(false) / 1_000_000_000d):F2} GB", nameof(svm_ctl), nameof(feature_selection));
                    io_proxy.WriteLine($@"", nameof(svm_ctl), nameof(feature_selection));
                    //io_proxy.WriteLine($@"--------------- End of iteration {iteration_index} ---------------", nameof(svm_ctl), nameof(interactive));


                    sw_iteration.Stop();
                    var elapsed_iteration = $@"{sw_iteration.Elapsed:dd\:hh\:mm\:ss}";
                    var elapsed_fs = $@"{sw_fs.Elapsed:dd\:hh\:mm\:ss}";

                    var checkpoint_data = new List<(string key, string value)>();
                    checkpoint_data.Add(($@"{nameof(iteration_index)}", $@"{iteration_index}"));
                    checkpoint_data.Add(($@"{nameof(winning_iteration)}", $@"{winning_iteration}"));
                    checkpoint_data.Add(($@"{nameof(highest_score_last_iteration_group_index)}", $@"{highest_score_last_iteration_group_index}"));
                    checkpoint_data.Add(($@"{nameof(highest_score_this_iteration_group_index)}", $@"{highest_score_this_iteration_group_index}"));
                    checkpoint_data.Add(($@"{nameof(highest_score_last_iteration_group_key)}", $@"{highest_score_last_iteration_group_key.ToString().Replace(",", ";").Replace("(", "").Replace(")", "")}"));
                    checkpoint_data.Add(($@"{nameof(highest_score_this_iteration_group_key)}", $@"{highest_score_this_iteration_group_key.ToString().Replace(",", ";").Replace("(", "").Replace(")", "")}"));
                    checkpoint_data.Add(($@"{nameof(forward)}", $@"{forward}"));
                    checkpoint_data.Add(($@"{nameof(iterations_not_better_than_last)}", $@"{iterations_not_better_than_last}"));
                    checkpoint_data.Add(($@"{nameof(iterations_not_better_than_all)}", $@"{iterations_not_better_than_all}"));
                    //checkpoint_data.Add(($@"{nameof(selected_group_indexes)}", $@"{selected_group_indexes}"));
                    checkpoint_data.Add(($@"{nameof(highest_score_last_iteration)}", $@"{highest_score_last_iteration:G17}"));
                    checkpoint_data.Add(($@"{nameof(highest_score_this_iteration)}", $@"{highest_score_this_iteration:G17}"));
                    checkpoint_data.Add(($@"{nameof(highest_score_all_iteration)}", $@"{highest_score_all_iteration:G17}"));
                    checkpoint_data.Add(($@"{nameof(score_increase_from_last)}", $@"{score_increase_from_last:G17}"));
                    checkpoint_data.Add(($@"{nameof(score_increase_from_last_pct)}", $@"{score_increase_from_last_pct:G17}"));
                    checkpoint_data.Add(($@"{nameof(score_increase_from_all)}", $@"{score_increase_from_all:G17}"));
                    checkpoint_data.Add(($@"{nameof(score_increase_from_all_pct)}", $@"{score_increase_from_all_pct:G17}"));
                    checkpoint_data.Add(($@"{nameof(score_better_than_last)}", $@"{score_better_than_last}"));
                    checkpoint_data.Add(($@"{nameof(score_better_than_all)}", $@"{score_better_than_all}"));
                    checkpoint_data.Add(($@"{nameof(elapsed_iteration)}", $@"{elapsed_iteration}"));
                    checkpoint_data.Add(($@"{nameof(elapsed_fs)}", $@"{elapsed_fs}"));
                    checkpoint_data.Add(($@"{nameof(currently_selected_group_indexes)}", $@"{string.Join(";", currently_selected_group_indexes)}"));
                    checkpoint_data.Add(($@"{nameof(highest_scoring_group_indexes)}", $@"{string.Join(";", highest_scoring_group_indexes)}"));

                    checkpoint_data.AddRange(winner.cmd_params.get_options().Select(a => ($"p_{a.key}", a.value)).ToList());
                    //checkpoint_data.AddRange(winner.cms.);

                    io_proxy.WriteAllLines(checkpoint_fn, checkpoint_data.Select(a => $"{a.key}={a.value}").ToList());

                    //var summary_fn = io_proxy.convert_path(Path.Combine(iteration_folder, "summary.csv"));



                    if (summary_lines == null || summary_lines.Count == 0)
                    {
                        summary_lines.Add($"{string.Join(",", checkpoint_data.Select(a => a.key).ToList())},{string.Join(",", performance_measure.confusion_matrix.csv_header.Select(h => $"cm_{h}").ToList())}");
                    }
                    //  todo: add winnter cmd_params

                    winner.cms.cm_list.ForEach(c => summary_lines.Add($"{string.Join(",", checkpoint_data.Select(a => a.value).ToList())},{c.ToString()}"));

                    io_proxy.WriteAllLines(summary_fn, summary_lines);

                    var previous_tests_data = previous_tests.Select(a => string.Join(";", a)).ToList();
                    io_proxy.WriteAllLines(previous_tests_fn, previous_tests_data);

                }

                // previous winning iterations
                // route taken 


                io_proxy.WriteLine($@"score_better_than_last = {score_better_than_last}", nameof(svm_ctl), nameof(feature_selection));
                io_proxy.WriteLine($@"(iterations_not_better_than_last < limit_iteration_not_better_than_last && iterations_not_better_than_all < limit_iteration_not_better_than_all) = {(iterations_not_better_than_last < limit_iteration_not_better_than_last && iterations_not_better_than_all < limit_iteration_not_better_than_all)}", nameof(svm_ctl), nameof(feature_selection));
                io_proxy.WriteLine($@"currently_selected_group_indexes.Count < groups.Count = {currently_selected_group_indexes.Count < groups.Count}", nameof(svm_ctl), nameof(feature_selection));

                io_proxy.WriteLine($@"--------------- End of iteration {iteration_index} ---------------", nameof(svm_ctl), nameof(feature_selection));


            }
            while
            (

                (score_better_than_last || (iterations_not_better_than_last < limit_iteration_not_better_than_last && iterations_not_better_than_all < limit_iteration_not_better_than_all))

                &&

                (currently_selected_group_indexes.Count < groups.Count)

            );

            io_proxy.WriteLine($@"", nameof(svm_ctl), nameof(feature_selection));
            io_proxy.WriteLine($@"Finished after {iteration_index} iterations", nameof(svm_ctl), nameof(feature_selection));

            io_proxy.WriteLine($@"", nameof(svm_ctl), nameof(feature_selection));
            io_proxy.WriteLine($@"Winning iteration: {winning_iteration}", nameof(svm_ctl), nameof(feature_selection));
            io_proxy.WriteLine($@"Winning score: {highest_score_all_iteration}", nameof(svm_ctl), nameof(feature_selection));
            io_proxy.WriteLine($@"Winning group indexes: {string.Join(", ", highest_scoring_group_indexes)}", nameof(svm_ctl), nameof(feature_selection));


            // todo: save final list

            var final_list_fn = Path.Combine(root_folder, "final_list.csv");
            var fl = highest_scoring_group_indexes.Select(a => $"{a},{groups[a].key.ToString().Replace(",", ";").Replace("(", "").Replace(")", "")},{string.Join(";", groups[a].columns)}").ToList();
            fl.Insert(0, "group_index,group_key,columns");
            io_proxy.WriteAllLines(final_list_fn, fl);

            io_proxy.WriteLine($@"", nameof(svm_ctl), nameof(feature_selection));


            // after finding winning solution, check performance with different kernels and scaling methods

            var find_best_params = true;

            if (find_best_params)
            {
                test_group_kernel_scaling_perf(p, iteration_index, highest_scoring_group_indexes, groups, downsampled_training_class_folds, class_folds, dataset_instance_list_grouped);
            }

            // output winner details
        }

        private static void test_group_kernel_scaling_perf(cmd_params p, int iteration_index, List<int> highest_scoring_group_indexes,
            List<(int index, (string alphabet, string dimension, string category, string source, string @group, string member, string perspective) key, List<(int fid, string alphabet, string dimension, string category, string source, string @group, string member, string perspective, int alphabet_id, int dimension_id, int category_id, int source_id, int group_id, int member_id, int perspective_id)> list, List<int> columns)> groups, List<(int class_id, int size, List<(int randomisation_cv_index, int outer_cv_index, List<int> indexes)> folds)> downsampled_training_class_folds, List<(int class_id, int size, List<(int randomisation_cv_index, int outer_cv_index, List<int> indexes)> folds)> class_folds, List<(int class_id, List<(int class_id, int example_id, int class_example_id, List<(string comment_header, string comment_value)> comment_columns, List<(int fid, double fv)> feature_data)> examples)> dataset_instance_list_grouped)
        {
            // todo: make it do this for the winning iteration (rather than the last iteration)

            var kernel_types = ((common.libsvm_kernel_type[]) Enum.GetValues(typeof(common.libsvm_kernel_type))).Where(a => a != common.libsvm_kernel_type.precomputed).ToList();
            var scale_functions = ((common.scale_function[]) Enum.GetValues(typeof(common.scale_function))) /*.Where(a => a != common.scale_function.none)*/.ToList();
            
            io_proxy.WriteLine($@"Starting to do kernel and scale ranking...", nameof(svm_ctl), nameof(feature_selection));

            iteration_index++;
            var jobs_group_level1 = Enumerable.Range(0, kernel_types.Count).AsParallel().AsOrdered().Select(kernel_index =>
            {
                var kernel_type = kernel_types[kernel_index];

                var jobs_group_level2 = Enumerable.Range(0, scale_functions.Count).AsParallel().AsOrdered().Select(
                    scale_function_index =>
                    {
                        var scale_function = scale_functions[scale_function_index];

                        var p2 = new cmd_params(p);
                        p2.svm_kernel = kernel_type;
                        p2.scale_function = scale_function;
                        p2.iteration = -1;

                        io_proxy.WriteLine($@"Trying kernel {p2.svm_kernel} with scale function {p2.scale_function} ",
                            nameof(svm_ctl), nameof(feature_selection));

                        var currently_selected_groups = highest_scoring_group_indexes.Select(a => groups[a]).ToList();
                        var currently_selected_groups_columns = currently_selected_groups.SelectMany(a => a.columns)
                            .OrderBy(a => a).Distinct().ToList();
                        var query_cols = currently_selected_groups_columns.OrderBy(a => a).Distinct().ToList();


                        var group_index = -1;
                        //var query_cols = ;//highest_scoring_group_indexes.ToList();
                        var forward = true;
                        var old_feature_count = query_cols.Count;
                        var new_feature_count = query_cols.Count;
                        var old_group_count = highest_scoring_group_indexes.Count;
                        var new_group_count = highest_scoring_group_indexes.Count;
                        var group_key = ("", "", "", "", "", "", "");
                        var this_test_group_indexes = highest_scoring_group_indexes.ToList();


                        var group_cv_result1 = group_cv_part1(1, p2, downsampled_training_class_folds, class_folds,
                            dataset_instance_list_grouped, iteration_index, group_index, groups, query_cols, forward,
                            old_feature_count, new_feature_count, old_group_count, new_group_count, group_key,
                            this_test_group_indexes);

                        wait_for_results(group_cv_result1.wait_file_list);

                        var group_cv_result2 = group_cv_part2(group_cv_result1);

                        return group_cv_result2;
                    }).ToList();


                return jobs_group_level2;
            }).ToList();

            var jobs_group_level0 = jobs_group_level1.Where(a => a != default)
                .SelectMany(a => a.Where(b => b != default).ToList()).ToList();

            io_proxy.WriteLine($@"Finished to do kernel and scale ranking...", nameof(svm_ctl), nameof(feature_selection));

            //foreach (common.libsvm_kernel_type kernel_type in Enum.GetValues(typeof(common.libsvm_kernel_type)))
            //{
            //    if (kernel_type == common.libsvm_kernel_type.precomputed) continue;
            //    foreach (common.scale_function scale_function in Enum.GetValues(typeof(common.scale_function)))
            //    {
            //        if (scale_function == common.scale_function.none) continue;
            //    }
            //}

            // get ranks by kernel/scaling


            var iteration_folder1 = io_proxy.convert_path(Path.Combine(p.results_root_folder, $"itr_{iteration_index}"));

            var cm_inputs1 = get_cm_inputs(p, jobs_group_level0, groups, iteration_folder1, iteration_index);

            io_proxy.WriteLine($@"Finished ranking the highest scoring group...", nameof(svm_ctl), nameof(feature_selection));
            
        }

        private static List<((string test_file, string test_comments_file, string prediction_file, string cm_file) filenames, (List<performance_measure.prediction> prediction_list, List<performance_measure.confusion_matrix> cm_list) cms, cmd_params cmd_params)>

            get_cm_inputs(
                cmd_params ranking_metric_params,
                List<(List<(List<string> wait_file_list, cmd_params cmd_params, cmd_params merge_cmd_params, List<(bool wait_first, bool has_header, string average_out_filename, string merge_out_filename, string merge_in_filename, string average_header)> to_merge)> jobs_randomisation_level, List<((string test_file, string test_comments_file, string prediction_file, string cm_file) filenames, (List<performance_measure.prediction> prediction_list, List<performance_measure.confusion_matrix> cm_list) cms, cmd_params cmd_params)> merge_cm_inputs, List<int> this_test_group_indexes)> jobs_group_level,
                List<(int index, (string alphabet, string dimension, string category, string source, string @group, string member, string perspective) key, List<(int fid, string alphabet, string dimension, string category, string source, string @group, string member, string perspective, int alphabet_id, int dimension_id, int category_id, int source_id, int group_id, int member_id, int perspective_id)> list, List<int> columns)> groups,
                string iteration_folder,
                int iteration_index,
                string ranks_fn = null)
        {
            jobs_group_level = jobs_group_level?.Where(a => a != default).ToList();

            if (jobs_group_level == null || jobs_group_level.Count == 0) return default;


            //var jobs = jobs_group_level.SelectMany(a => a.jobs_randomisation_level.Where(b => b != default).SelectMany(b => b).ToList()).ToList();

            var cm_inputs = jobs_group_level.SelectMany(a => a.merge_cm_inputs.Where(b => b != default).ToList()).ToList();


            // reorder all groups by rank
            cm_inputs = cm_inputs.OrderByDescending(a =>
                a.cms.cm_list.Where(b => ranking_metric_params.feature_selection_classes == null || ranking_metric_params.feature_selection_classes.Count == 0 || ranking_metric_params.feature_selection_classes.Contains(b.class_id.Value)).Average(b => b.get_perf_value_strings().Where(c => ranking_metric_params.feature_selection_metrics.Any(d => string.Equals(c.name, d, StringComparison.InvariantCultureIgnoreCase))).Average(c => c.value)))
                .ThenBy(a => a.cmd_params.new_feature_count)
                .ToList();


            var ranked_scores = cm_inputs.Select((a, i) => (group_index: a.cmd_params.group_index, rank_index: i,
                group_key: a.cmd_params.group_index > -1 ? groups[a.cmd_params.group_index].key : ("_", "_", "_", "_", "_", "_", "_"),
                score: a.cms.cm_list.Where(b => ranking_metric_params.feature_selection_classes == null || ranking_metric_params.feature_selection_classes.Count == 0 || ranking_metric_params.feature_selection_classes.Contains(b.class_id.Value))
                    .Average(b => b.get_perf_value_strings().Where(c => ranking_metric_params.feature_selection_metrics.Any(d => string.Equals(c.name, d, StringComparison.InvariantCultureIgnoreCase))).Average(c => c.value)))).ToList();

            io_proxy.WriteLine("", nameof(svm_ctl), nameof(feature_selection));
            ranked_scores.ForEach(a => io_proxy.WriteLine($"Rank index: {a.rank_index}, score: {a.score}, group_index: {a.group_index}, item: {a.group_key}", nameof(svm_ctl), nameof(feature_selection)));
            io_proxy.WriteLine("", nameof(svm_ctl), nameof(feature_selection));



            // save rank ordered confusion matrixes - all groups 
            // will this work for the svm type, svm kernel, scaling type?
            if (string.IsNullOrWhiteSpace(ranks_fn))
            {
                ranks_fn = Path.Combine(iteration_folder, $@"ranks_cm_{iteration_index}.csv");
            }

            if (!io_proxy.is_file_available(ranks_fn))
            {
                var cms_header = $"{string.Join(",", cmd_params.csv_header)},{string.Join(",", performance_measure.confusion_matrix.csv_header)}";
                var cms_csv = cm_inputs.SelectMany(a => a.cms.cm_list.Select(b => $"{string.Join(",", a.cmd_params.get_options().Select(c => c.value).ToList())},{b.ToString()}").ToList()).ToList();
                cms_csv.Insert(0, cms_header);

                io_proxy.WriteAllLines(ranks_fn, cms_csv);
                io_proxy.WriteLine($"Saving: {ranks_fn}", nameof(svm_ctl), nameof(feature_selection));
            }

            return cm_inputs;
        }

        //public static void check_solution(List<int> group_indexes, List<int> column_indexes)
        //{
        //    // check a proposed solution

        //    // perform outer-cv x10 with given svm_type, kernel_type, scale_function, etc.


        //}

        public static

            //(List<(List<string> wait_file_list, cmd_params cmd_params, cmd_params merge_cmd_params, List<(bool wait_first, bool has_header, string average_out_filename, string merge_out_filename, string merge_in_filename, string average_header)> to_merge)> jobs_randomisation_level, List<((string test_file, string test_comments_file, string prediction_file, string cm_file) filenames, (List<performance_measure.prediction> prediction_list, List<performance_measure.confusion_matrix> cm_list) cms, cmd_params cmd_params)> merge_cm_inputs, List<int> this_test_group_indexes)
            //List<List<(List<string> wait_file_list, cmd_params cmd_params, cmd_params merge_cmd_params, List<(bool wait_first, bool has_header, string average_out_filename, string merge_out_filename, string merge_in_filename, string average_header)> to_merge)>> 

            (List<string> wait_file_list, cmd_params p, List<int> this_test_group_indexes, List<(bool wait_first, bool has_header, string average_out_filename, string merge_out_filename, string merge_in_filename, string average_header)> post_tm, List<(List<string> wait_file_list, cmd_params cmd_params, cmd_params merge_cmd_params, List<(bool wait_first, bool has_header, string average_out_filename, string merge_out_filename, string merge_in_filename, string average_header)> to_merge)> rets, List<cmd_params> wkr_cmd_params_list)
            group_cv_part1(
                int stage,
                cmd_params p,
                List<(int class_id, int size, List<(int randomisation_cv_index, int outer_cv_index, List<int> indexes)> folds)> downsampled_training_class_folds,
                List<(int class_id, int size, List<(int randomisation_cv_index, int outer_cv_index, List<int> indexes)> folds)> class_folds,
                List<(int class_id, List<(int class_id, int example_id, int class_example_id, List<(string comment_header, string comment_value)> comment_columns, /*string comment_columns_hash,*/ List<(int fid, double fv)> feature_data/*, string feature_data_hash*/)> examples)> dataset_instance_list_grouped,
                int iteration_index,
                int group_index,
                List<(int index, (string alphabet, string dimension, string category, string source, string @group, string member, string perspective) key, List<(int fid, string alphabet, string dimension, string category, string source, string @group, string member, string perspective, int alphabet_id, int dimension_id, int category_id, int source_id, int group_id, int member_id, int perspective_id)> list, List<int> columns)> groups,
                List<int> query_cols,
                bool forward = true,
                int old_feature_count = -1,
                int new_feature_count = -1,
                int old_group_count = -1,
                int new_group_count = -1,
                (string alphabet, string dimension, string category, string source, string @group, string member, string perspective) group_key = default,
                List<int> this_test_group_indexes = default
                )
        {
            io_proxy.WriteLine($@"{nameof(iteration_index)}={iteration_index}, {nameof(group_index)}={group_index}", nameof(svm_ctl), nameof(group_cv_part1));

            query_cols = query_cols.ToList();

            // remove duplicate columns (may exist in separate groups)
            var query_col_dupe_check = dataset_instance_list_grouped.SelectMany(a => a.examples).SelectMany(a => query_cols.Select(b => (query_col: b, fv: a.feature_data[b].fv)).ToList()).GroupBy(b => b.query_col).Select(b => (query_col: b.Key, values: b.Select(c => c.fv).ToList())).ToList();
            var dupe_clusters = new List<List<int>>();
            for (var i = 0; i < query_col_dupe_check.Count; i++)
            {
                for (var j = 0; j < query_col_dupe_check.Count; j++)
                {
                    if (i <= j) continue;

                    if (query_col_dupe_check[i].values.SequenceEqual(query_col_dupe_check[j].values))
                    {
                        var cluster = new List<int>() { query_col_dupe_check[i].query_col, query_col_dupe_check[j].query_col };
                        var x = dupe_clusters.Where(a => a.Any(b => cluster.Any(c => b == c))).ToList();
                        x.ForEach(a => { cluster.AddRange(a); dupe_clusters.Remove(a); });
                        cluster = cluster.OrderBy(a => a).Distinct().ToList();
                        dupe_clusters.Add(cluster);
                    }
                }
            }
            foreach (var dc in dupe_clusters)
            {
                //var keep = dc.First();
                var remove = dc.Skip(1).ToList();
                query_cols.RemoveAll(a => remove.Any(b => a == b));
            }
            ///

            var rets =
                new List<(List<string> wait_file_list, cmd_params cmd_params, cmd_params merge_cmd_params, List<(bool
                    wait_first, bool has_header, string average_out_filename, string merge_out_filename, string
                    merge_in_filename, string average_header)> to_merge)>();

            //var jobs_randomisation_level = Enumerable.Range(0, p.randomisation_cv_folds).AsParallel().AsOrdered().Select(randomisation_cv_index =>
            for (var _randomisation_cv_index = 0; _randomisation_cv_index < p.randomisation_cv_folds; _randomisation_cv_index++)
            {
                var randomisation_cv_index = _randomisation_cv_index;

                //var jobs_outerfold_level = Enumerable.Range(0, p.outer_cv_folds).AsParallel().AsOrdered().Select(outer_cv_index =>
                for (var _outer_cv_index = 0; _outer_cv_index < p.outer_cv_folds; _outer_cv_index++)
                {
                    var outer_cv_index = _outer_cv_index;

                    // get the fold index ranges for this outer-cv-fold
                    var training_fold_indexes = downsampled_training_class_folds.Select(a => (a.class_id, outer_cv_index: outer_cv_index, indexes: a.folds.Where(b => b.randomisation_cv_index == randomisation_cv_index && b.outer_cv_index != outer_cv_index).SelectMany(b => b.indexes).OrderBy(b => b).ToList())).ToList();

                    var testing_fold_indexes = class_folds.Select(a => (a.class_id, outer_cv_index: outer_cv_index, indexes: a.folds.Where(b => b.randomisation_cv_index == randomisation_cv_index && b.outer_cv_index == outer_cv_index).SelectMany(b => b.indexes).OrderBy(b => b).ToList())).ToList();

                    

                    var training_examples = training_fold_indexes.Select(a => (a.class_id, examples: a.indexes.Select(row_index => dataset_instance_list_grouped.First(b => a.class_id == b.class_id).examples[row_index]).ToList())).ToList();

                    var testing_examples = testing_fold_indexes.Select(a => (a.class_id, examples: a.indexes.Select(row_index => dataset_instance_list_grouped.First(b => a.class_id == b.class_id).examples[row_index]).ToList())).ToList();


                    var training_id_text_header = "";
                    var testing_id_text_header = "";

                    var training_meta_text_header = "";
                    var testing_meta_text_header = "";

                    var job_id = for_loop_instance_id(new List<(int current, int max)>() { (iteration_index, 0), (group_index, groups.Count), (randomisation_cv_index, p.randomisation_cv_folds), (outer_cv_index, p.outer_cv_folds) });

                    // for each training example, get the specific columns to include for the group being iterated over, 
                    var training_example_columns = training_examples.Select(a =>
                        (
                            a.class_id,
                            examples: a.examples.Select(b => (example: b, columns: query_cols.Select(c => b.feature_data[c].fid == c ? b.feature_data[c] : b.feature_data.First(d => d.fid == c)).ToList()/*, columns_hash: hash.calc_hash(string.Join(" ", query_cols.Select(c => b.feature_data[c].fid == c ? b.feature_data[c] : b.feature_data.First(d => d.fid == c)).ToList()))*/)).ToList())).ToList();

                    var testing_example_columns = testing_examples.Select(a => (a.class_id, examples: a.examples.Select(b => (example: b, columns: query_cols.Select(c => b.feature_data[c].fid == c ? b.feature_data[c] : b.feature_data.First(d => d.fid == c)).ToList()/*, columns_hash: hash.calc_hash(string.Join(" ", query_cols.Select(c => b.feature_data[c].fid == c ? b.feature_data[c] : b.feature_data.First(d => d.fid == c)).ToList()))*/)).ToList())).ToList();

                    var training_scaling_params = training_example_columns.SelectMany(a => a.examples.SelectMany(b => b.columns).ToList()).GroupBy(a => a.fid).Select(a =>
                    {
                        var x = a.Select(b => b.fv).ToList();

                        return (fid: a.Key, list: x, non_zero: x.Count(y => y != 0), abs_sum: x.Sum(y => Math.Abs(y)), srsos: sqrt_sumofsqrs(x), average: x.Average(), stdev: standard_deviation_sample(x), min: x.Min(), max: x.Max());
                    }).ToList();

                    var training_example_columns_scaled = training_example_columns.Select(a => (a.class_id, examples: a.examples.Select(b => (b.example, columns: b.columns.Select(c =>
                    {
                        var x = training_scaling_params.First(y => y.fid == c.fid);

                        return (fid: c.fid, fv: scale(c.fv, x.list, x.non_zero, x.abs_sum, x.srsos, x.min, x.max, x.average, x.stdev, p.scale_function));
                    }).ToList()/*, b.columns_hash*/)).ToList())).ToList();

                    var testing_example_columns_scaled = testing_example_columns.Select(a => (a.class_id, examples: a.examples.Select(b => (b.example, columns: b.columns.Select(c =>
                    {
                        var x = training_scaling_params.First(y => y.fid == c.fid);

                        return (fid: c.fid, fv: scale(c.fv, x.list, x.non_zero, x.abs_sum, x.srsos, x.min, x.max, x.average, x.stdev, p.scale_function));
                    }).ToList()/*, b.columns_hash*/)).ToList())).ToList();

                    var training_text = training_example_columns_scaled.SelectMany(a => a.examples.Select(b => $"{a.class_id} {String.Join(" ", b.columns.Where(c => c.fv != 0).OrderBy(c => c.fid).Select(c => $"{c.fid}:{c.fv}").ToList())}").ToList()).ToList();

                    var testing_text = testing_example_columns_scaled.SelectMany(a => a.examples.Select(b => $"{a.class_id} {String.Join(" ", b.columns.Where(c => c.fv != 0).OrderBy(c => c.fid).Select(c => $"{c.fid}:{c.fv}").ToList())}").ToList()).ToList();

                    if (string.IsNullOrWhiteSpace(training_id_text_header))
                        training_id_text_header = $"class_id,example_id,class_example_id";//",feature_data_hash,comment_columns_hash,columns_hash";

                    if (string.IsNullOrWhiteSpace(testing_id_text_header))
                        testing_id_text_header = $"class_id,example_id,class_example_id";//",feature_data_hash,comment_columns_hash,columns_hash";

                    if (string.IsNullOrWhiteSpace(training_meta_text_header))
                        training_meta_text_header = string.Join(",", training_example_columns.First().examples.First().example.comment_columns.Select(c => "c_" + c.comment_header).ToList());

                    if (string.IsNullOrWhiteSpace(testing_meta_text_header))
                        testing_meta_text_header = string.Join(",", testing_example_columns.First().examples.First().example.comment_columns.Select(c => "c_" + c.comment_header).ToList());

                    var training_id_text = training_example_columns.SelectMany(a => a.examples.Select(b => $"{a.class_id},{b.example.example_id},{b.example.class_example_id}").ToList()).ToList();//,{ b.example.feature_data_hash},{b.example.comment_columns_hash},{b.columns_hash}").ToList()).ToList();

                    var testing_id_text = testing_example_columns.SelectMany(a => a.examples.Select(b => $"{a.class_id},{b.example.example_id},{b.example.class_example_id}").ToList()).ToList();//,{ b.example.feature_data_hash},{b.example.comment_columns_hash},{b.columns_hash}").ToList()).ToList();

                    var training_meta_text = training_example_columns.SelectMany(a => a.examples.Select(b => string.Join(",", b.example.comment_columns.Select(c => c.comment_value).ToList())).ToList()).ToList();

                    var testing_meta_text = testing_example_columns.SelectMany(a => a.examples.Select(b => string.Join(",", b.example.comment_columns.Select(c => c.comment_value).ToList())).ToList()).ToList();

                    training_meta_text.Insert(0, training_meta_text_header);

                    testing_meta_text.Insert(0, testing_meta_text_header);

                    training_id_text.Insert(0, training_id_text_header);

                    testing_id_text.Insert(0, testing_id_text_header);

                    var training_sizes = training_example_columns_scaled.Select(a => (class_id: a.class_id, training_size: a.examples.Count)).ToList();

                    var testing_sizes = testing_example_columns_scaled.Select(a => (class_id: a.class_id, testing_size: a.examples.Count)).ToList();

                    var job_info = (
                        forward: forward,
                        training_sizes: training_sizes,
                        testing_sizes: testing_sizes,
                        old_feature_count: old_feature_count,
                        new_feature_count: new_feature_count,
                        old_group_count: old_group_count,
                        new_group_count: new_group_count,
                        job_id: job_id,
                        randomisation_cv_index: randomisation_cv_index,
                        iteration_index: iteration_index,
                        outer_cv_index: outer_cv_index,
                        group_key: group_key,
                        group_index: group_index,
                        training_text: training_text,
                        training_id_text: training_id_text,
                        training_meta_text: training_meta_text,
                        testing_text: testing_text,
                        testing_id_text: testing_id_text,
                        testing_meta_text: testing_meta_text);


                    var ret = do_job(stage, p, (groups != null && groups.Count > job_info.group_index && group_index > -1) ? groups[job_info.group_index] : default, job_info);

                    if (ret != default)
                    {
                        rets.Add(ret);
                    }

                    //return ret;
                }//).ToList();

                //jobs_outerfold_level = jobs_outerfold_level.Where(a => a != default).ToList();

                //return jobs_outerfold_level;
            }//).ToList();

            //jobs_randomisation_level = jobs_randomisation_level.Where(a => a != default).ToList();
            //return jobs_randomisation_level;

            // merge job inputs (training/testing/etc. files, etc.)
            //var tm = jobs_randomisation_level.SelectMany(a => a.SelectMany(b => b.to_merge).ToList()).ToList();
            var tm = rets.SelectMany(a => a.to_merge).ToList();
            var pre_tm = tm.Where(a => !a.wait_first).ToList();
            var post_tm = tm.Where(a => a.wait_first).ToList();
            merge_pre_results(pre_tm);

            // submit jobs to scheduler
            //var wkr_cmd_params_list = jobs_randomisation_level.SelectMany(a => a.Select(b => b.cmd_params).ToList()).ToList();
            var wkr_cmd_params_list = rets.Select(a => a.cmd_params).ToList();


            //var iteration_folder = io_proxy.convert_path(Path.Combine(p.results_root_folder, $"itr_{iteration_index}"));

            // wait for results
            //var wait_file_list = jobs_randomisation_level.SelectMany(a => a.SelectMany(b => b.wait_file_list).ToList()).ToList();
            var wait_file_list = rets.SelectMany(a => a.wait_file_list).ToList();
            //wait_for_results(wait_file_list);


            var result = (wait_file_list, p, this_test_group_indexes, post_tm, rets, wkr_cmd_params_list);

            return result;

            //return group_cv_part2(p, this_test_group_indexes, post_tm, rets, wkr_cmd_params_list);
        }

        public static 
            (List<(List<string> wait_file_list, cmd_params cmd_params, cmd_params merge_cmd_params, List<(bool wait_first, bool has_header, string average_out_filename, string merge_out_filename, string merge_in_filename, string average_header)> to_merge)> jobs_randomisation_level, List<((string test_file, string test_comments_file, string prediction_file, string cm_file) filenames, (List<performance_measure.prediction> prediction_list, List<performance_measure.confusion_matrix> cm_list) cms, cmd_params cmd_params)> merge_cm_inputs, List<int> this_test_group_indexes) 
            group_cv_part2
            (
                (List<string> wait_file_list,
                cmd_params p,
                List<int> this_test_group_indexes, 
                List<(bool wait_first, bool has_header, string average_out_filename, string merge_out_filename, string merge_in_filename, string average_header)> post_tm, 
                List<(List<string> wait_file_list, cmd_params cmd_params, cmd_params merge_cmd_params, List<(bool wait_first, bool has_header, string average_out_filename, string merge_out_filename, string merge_in_filename, string average_header)> to_merge)> rets,
                List<cmd_params> wkr_cmd_params_list) x
            )
        {
            // merge prediction results... to create a single confusion matrix
            merge_post_results(x.post_tm);

            //var merge_cmd_params_list = jobs_randomisation_level.SelectMany(a => a.Select(b => b.merge_cmd_params).ToList()).ToList();
            var merge_cmd_params_list = x.rets.Select(a => a.merge_cmd_params).ToList();

            var merge_cm_inputs = merge_cmd_params_list.GroupBy(a =>
            (
                test_file: a.test_filename,
                //test_labels_filename: a.test_labels_filename,
                test_comments_file: a.test_meta_filename,
                prediction_file: a.test_predict_filename,
                cm_file: Path.Combine(Path.GetDirectoryName(a.test_predict_cm_filename),
                    $@"{Path.GetFileNameWithoutExtension(a.test_predict_cm_filename)}_merged_predictions{Path.GetExtension(a.test_predict_cm_filename)}")
            )).Select(a => (filenames: a.Key,
                cms: performance_measure.load_prediction_file(a.Key.test_file, a.Key.test_comments_file, a.Key.prediction_file,
                    x.p.output_threshold_adjustment_performance), cmd_params: new cmd_params(a.ToList()))).ToList();

            merge_cm_inputs.ForEach(a => update_cm(x.p, a.cms.cm_list));
            merge_cm_inputs.ForEach(a => update_cm(a.cmd_params, a.cms.cm_list));


            var cms_header =
                $@"{string.Join(",", cmd_params.csv_header)},{string.Join(",", performance_measure.confusion_matrix.csv_header)}";

            for (var i = 0; i < merge_cm_inputs.Count; i++)
            {
                var item = merge_cm_inputs[i];

                var fn = item.filenames.cm_file;
                if (!io_proxy.is_file_available(fn))
                {
                    var cm_data = new List<string>();
                    cm_data.Add(cms_header);
                    cm_data.AddRange(item.cms.cm_list.Select(a =>
                            $@"{string.Join(",", item.cmd_params.get_options().Select(c => c.value).ToList())},{a.ToString()}")
                        .ToList());


                    io_proxy.WriteAllLines(fn, cm_data);

                    io_proxy.WriteLine($@"Saved merged cm: {fn}", nameof(svm_ctl), nameof(feature_selection));
                }
            }

            // post merge, delete individual outer-cv files
            delete_temp_wkr_files(x.wkr_cmd_params_list);

            delete_temp_merge_files(merge_cmd_params_list);
            // return results


            return (x.rets, merge_cm_inputs, x.this_test_group_indexes);
        }

        public static void delete_temp_wkr_files(cmd_params p)
        {
            //try { io_proxy.Delete(p.program_wkr_stderr_filename, nameof(svm_ctl), nameof(delete_temp_wkr_files)); } catch (Exception) { }
            //try { io_proxy.Delete(p.program_wkr_stdout_filename, nameof(svm_ctl), nameof(delete_temp_wkr_files)); } catch (Exception) { }
            try { io_proxy.Delete(p.options_filename, nameof(svm_ctl), nameof(delete_temp_wkr_files)); } catch (Exception) { }
            try { io_proxy.Delete(p.pbs_wkr_stderr_filename, nameof(svm_ctl), nameof(delete_temp_wkr_files)); } catch (Exception) { }
            try { io_proxy.Delete(p.pbs_wkr_stdout_filename, nameof(svm_ctl), nameof(delete_temp_wkr_files)); } catch (Exception) { }
            try { io_proxy.Delete(p.test_filename, nameof(svm_ctl), nameof(delete_temp_wkr_files)); } catch (Exception) { }
            try { io_proxy.Delete(p.test_labels_filename, nameof(svm_ctl), nameof(delete_temp_wkr_files)); } catch (Exception) { }
            try { io_proxy.Delete(p.test_id_filename, nameof(svm_ctl), nameof(delete_temp_wkr_files)); } catch (Exception) { }
            try { io_proxy.Delete(p.test_meta_filename, nameof(svm_ctl), nameof(delete_temp_wkr_files)); } catch (Exception) { }
            try { io_proxy.Delete(p.test_predict_cm_filename, nameof(svm_ctl), nameof(delete_temp_wkr_files)); } catch (Exception) { }
            try { io_proxy.Delete(p.test_predict_filename, nameof(svm_ctl), nameof(delete_temp_wkr_files)); } catch (Exception) { }
            try { io_proxy.Delete(p.train_filename, nameof(svm_ctl), nameof(delete_temp_wkr_files)); } catch (Exception) { }
            try { io_proxy.Delete(p.train_grid_filename, nameof(svm_ctl), nameof(delete_temp_wkr_files)); } catch (Exception) { }
            try { io_proxy.Delete(p.train_id_filename, nameof(svm_ctl), nameof(delete_temp_wkr_files)); } catch (Exception) { }
            try { io_proxy.Delete(p.train_meta_filename, nameof(svm_ctl), nameof(delete_temp_wkr_files)); } catch (Exception) { }
            try { io_proxy.Delete(p.train_model_filename, nameof(svm_ctl), nameof(delete_temp_wkr_files)); } catch (Exception) { }
            try { io_proxy.Delete(p.train_predict_cm_filename, nameof(svm_ctl), nameof(delete_temp_wkr_files)); } catch (Exception) { }
            try { io_proxy.Delete(p.train_predict_filename, nameof(svm_ctl), nameof(delete_temp_wkr_files)); } catch (Exception) { }
        }

        public static void delete_temp_wkr_files(List<cmd_params> cmd_params_list)
        {
            foreach (var p in cmd_params_list)
            {
                delete_temp_wkr_files(p);
            }
        }

        public static void delete_temp_merge_files(cmd_params p)
        {
            //try { io_proxy.Delete(p.pbs_wkr_stderr_filename, nameof(svm_ctl), nameof(delete_temp_merge_files)); } catch (Exception) { }
            //try { io_proxy.Delete(p.pbs_wkr_stdout_filename, nameof(svm_ctl), nameof(delete_temp_merge_files)); } catch (Exception) { }
            //try { io_proxy.Delete(p.program_wkr_stderr_filename, nameof(svm_ctl), nameof(delete_temp_merge_files)); } catch (Exception) { }
            //try { io_proxy.Delete(p.program_wkr_stdout_filename, nameof(svm_ctl), nameof(delete_temp_merge_files)); } catch (Exception) { }

            try { io_proxy.Delete(p.options_filename, nameof(svm_ctl), nameof(delete_temp_merge_files)); } catch (Exception) { }
            try { io_proxy.Delete(p.test_id_filename, nameof(svm_ctl), nameof(delete_temp_merge_files)); } catch (Exception) { }
            try { io_proxy.Delete(p.test_meta_filename, nameof(svm_ctl), nameof(delete_temp_merge_files)); } catch (Exception) { }
            try { io_proxy.Delete(p.test_filename, nameof(svm_ctl), nameof(delete_temp_merge_files)); } catch (Exception) { }
            //try { io_proxy.Delete(p.test_labels_filename, nameof(svm_ctl), nameof(delete_temp_merge_files)); } catch (Exception) { }
            //try { io_proxy.Delete(p.test_predict_cm_filename, nameof(svm_ctl), nameof(delete_temp_merge_files)); } catch (Exception) { }
            //try { io_proxy.Delete(p.test_predict_filename, nameof(svm_ctl), nameof(delete_temp_merge_files)); } catch (Exception) { }
            try { io_proxy.Delete(p.train_filename, nameof(svm_ctl), nameof(delete_temp_merge_files)); } catch (Exception) { }
            try { io_proxy.Delete(p.train_grid_filename, nameof(svm_ctl), nameof(delete_temp_merge_files)); } catch (Exception) { }
            try { io_proxy.Delete(p.train_id_filename, nameof(svm_ctl), nameof(delete_temp_merge_files)); } catch (Exception) { }
            try { io_proxy.Delete(p.train_meta_filename, nameof(svm_ctl), nameof(delete_temp_merge_files)); } catch (Exception) { }
            try { io_proxy.Delete(p.train_model_filename, nameof(svm_ctl), nameof(delete_temp_merge_files)); } catch (Exception) { } 
            //try { io_proxy.Delete(p.train_predict_cm_filename, nameof(svm_ctl), nameof(delete_temp_merge_files)); } catch (Exception) { }
            //try { io_proxy.Delete(p.train_predict_filename, nameof(svm_ctl), nameof(delete_temp_merge_files)); } catch (Exception) { }
        }

        public static void delete_temp_merge_files(List<cmd_params> cmd_params_list)
        {
            foreach (var p in cmd_params_list)
            {
                delete_temp_wkr_files(p);
            }
        }

        private static void merge_post_results(List<(bool wait_first, bool has_header, string average_out_filename, string merge_out_filename, string merge_in_filename, string average_header)> to_merge)
        {
            foreach (var t in to_merge.Where(a => a.wait_first).GroupBy(a => (a.merge_out_filename, a.average_out_filename)))
            {
                var key = t.Key;
                var fn = key.merge_out_filename;

                if (io_proxy.is_file_available(fn)) continue;

                var list = t.ToList();

                var read = list.SelectMany((a, i) => io_proxy.ReadAllLines(a.merge_in_filename, nameof(svm_ctl), nameof(merge_post_results)).Skip((i == 0 || !a.has_header) ? 0 : 1).ToList()).ToList();

                var av_head = list.FirstOrDefault(a => !string.IsNullOrWhiteSpace(a.average_header)).average_header;

                if (!string.IsNullOrWhiteSpace(av_head))
                {
                    var read_split = read.Select((line, line_index) => line.Split(',').ToList()).ToList();

                    var h = read_split.FirstOrDefault();
                    var read_split_data = read_split.Skip(1).ToList();

                    var c = h.FindIndex(a => string.Equals(a, av_head, StringComparison.InvariantCultureIgnoreCase));

                    var d = read_split_data.GroupBy(a => a[c]).Select(a => (key: a.Key, list: a.ToList())).ToList();

                    var is_numeric = new bool[h.Count];

                    for (var i = 0; i < is_numeric.Length; i++)
                    {
                        is_numeric[i] = read_split_data.Select(a => a[i]).All(a => double.TryParse(a, NumberStyles.Float, CultureInfo.InvariantCulture, out var r));
                    }

                    var av_list = new List<string[]>();
                    var min_list = new List<string[]>();
                    var max_list = new List<string[]>();
                    var sum_list = new List<string[]>();

                    foreach (var y in d)
                    {
                        var av_values = new string[is_numeric.Length];
                        var min_values = new string[is_numeric.Length];
                        var max_values = new string[is_numeric.Length];
                        var sum_values = new string[is_numeric.Length];

                        av_list.Add(av_values);
                        min_list.Add(min_values);
                        max_list.Add(max_values);
                        sum_list.Add(sum_values);

                        for (var i = 0; i < is_numeric.Length; i++)
                        {
                            var values = y.list.Select(a => a[i]).ToList();

                            if (is_numeric[i])
                            {
                                av_values[i] = values.Average(a => double.Parse(a, NumberStyles.Float, CultureInfo.InvariantCulture)).ToString("G17", CultureInfo.InvariantCulture);
                                min_values[i] = values.Min(a => double.Parse(a, NumberStyles.Float, CultureInfo.InvariantCulture)).ToString("G17", CultureInfo.InvariantCulture);
                                max_values[i] = values.Max(a => double.Parse(a, NumberStyles.Float, CultureInfo.InvariantCulture)).ToString("G17", CultureInfo.InvariantCulture);
                                sum_values[i] = values.Sum(a => double.Parse(a, NumberStyles.Float, CultureInfo.InvariantCulture)).ToString("G17", CultureInfo.InvariantCulture);
                            }
                            else
                            {
                                if (values.Distinct().Count() == 1)
                                {
                                    av_values[i] = values.FirstOrDefault();
                                    min_values[i] = values.FirstOrDefault();
                                    max_values[i] = values.FirstOrDefault();
                                    sum_values[i] = values.FirstOrDefault();
                                }
                            }
                        }
                    }

                    var lines_data = new List<string>();
                    if (av_list.Count > 0)
                    {
                        lines_data.Add("");
                        lines_data.Add("Average");
                        lines_data.Add(string.Join(",", read.FirstOrDefault().Split(',').Select(a => a + "_average").ToList()));
                        lines_data.AddRange(av_list.Select(a => string.Join(",", a)).ToList());

                    }

                    if (min_list.Count > 0)
                    {
                        lines_data.Add("");
                        lines_data.Add("Min");
                        lines_data.Add(string.Join(",", read.FirstOrDefault().Split(',').Select(a => a + "_min").ToList()));
                        lines_data.AddRange(min_list.Select(a => string.Join(",", a)).ToList());

                    }

                    if (max_list.Count > 0)
                    {
                        lines_data.Add("");
                        lines_data.Add("Max");
                        lines_data.Add(string.Join(",", read.FirstOrDefault().Split(',').Select(a => a + "_max").ToList()));
                        lines_data.AddRange(max_list.Select(a => string.Join(",", a)).ToList());

                    }

                    if (sum_list.Count > 0)
                    {
                        lines_data.Add("");
                        lines_data.Add("Sum");
                        lines_data.Add(string.Join(",", read.FirstOrDefault().Split(',').Select(a => a + "_sum").ToList()));
                        lines_data.AddRange(sum_list.Select(a => string.Join(",", a)).ToList());

                    }

                    //var fn2 = key.average_out_filename;
                    //io_proxy.WriteAllLines(fn2, lines_data);
                    //io_proxy.WriteLine("Saved: " + fn2);
                    read.AddRange(lines_data);
                }


                io_proxy.WriteAllLines(fn, read);
                io_proxy.WriteLine("Saved merged results: " + fn, nameof(svm_ctl), nameof(merge_post_results));
            }
        }

        private static void merge_pre_results(List<(bool wait_first, bool has_header, string average_out_filename, string merge_out_filename, string merge_in_filename, string average_header)> to_merge)
        {
            foreach (var t in to_merge.Where(a => !a.wait_first).GroupBy(a => a.merge_out_filename))
            {
                var key = t.Key;
                var fn = key;

                if (io_proxy.is_file_available(fn)) continue;

                var list = t.ToList();

                var read = list.SelectMany((a, i) => io_proxy.ReadAllLines(a.merge_in_filename, nameof(svm_ctl), nameof(merge_pre_results)).Skip(i == 0 || !a.has_header ? 0 : 1).ToList()).ToList();


                io_proxy.WriteAllLines(fn, read);
                io_proxy.WriteLine("Saved merged inputs: " + fn, nameof(svm_ctl), nameof(merge_pre_results));
            }
        }

        public static (
            List<string> wait_file_list,
            cmd_params cmd_params,
            cmd_params merge_cmd_params,
            List<(bool wait_first, bool has_header, string average_out_filename, string merge_out_filename, string merge_in_filename, string average_header)> to_merge

            ) do_job
        (
                int stage,
                cmd_params p,
            (
                int index,
                (string alphabet, string dimension, string category, string source, string group, string member, string perspective) key,
                List<(int fid, string alphabet, string dimension, string category, string source, string group, string member, string perspective, int alphabet_id, int dimension_id, int category_id, int source_id, int group_id, int member_id, int perspective_id)> list,
                List<int> columns
            ) group,
            (
                bool forward,
                List<(int class_id, int training_size)> training_sizes,
                List<(int class_id, int training_size)> testing_sizes,
                int old_feature_count,
                int new_feature_count,
                int old_group_count,
                int new_group_count,
                int job_id,
                int randomisation_cv_index,
                int iteration_index,
                int outer_cv_index,
                (string alphabet, string dimension, string category, string source, string group, string member, string perspective) group_key,
                int group_index,
                List<string> training_text,
                List<string> training_id_text,
                List<string> training_meta_text,
                List<string> testing_text,
                List<string> testing_id_text,
                List<string> testing_meta_text
            ) job
        )
        {
            io_proxy.WriteLine($@"Memory usage: {(GC.GetTotalMemory(false) / 1_000_000_000d):F2} GB", nameof(svm_ctl), nameof(do_job));

            //var merge_param_list = new List<cmd_params>();

            //var cmds = new List<string>();

            // each job is 1 outer-cv fold ... 1 grid search ... 1 prediction ... 1 fold cm  

            //var group_str = string.Join(".", new string[]
            //{
            //            job.group_key.alphabet, job.group_key.dimension, job.group_key.category,
            //            job.group_key.source, job.group_key.group, job.group_key.member, job.group_key.perspective,
            //}.Select(a => string.IsNullOrWhiteSpace(a) ? "_" : a).ToArray()); // todo: if used in future, also remove any invalid filename chars

            var iteration_folder =  Path.Combine(p.results_root_folder, $@"itr_{job.iteration_index}");
            var group_folder =      Path.Combine(p.results_root_folder, $@"itr_{job.iteration_index}", $@"grp_{job.group_index}", $@"svm_{(int)p.svm_type}_kl_{(int)p.svm_kernel}_sl_{(int)p.scale_function}");
            var outer_fold_folder = Path.Combine(p.results_root_folder, $@"itr_{job.iteration_index}", $@"grp_{job.group_index}", $@"svm_{(int)p.svm_type}_kl_{(int)p.svm_kernel}_sl_{(int)p.scale_function}", $@"rnd_{job.randomisation_cv_index}_cv_{job.outer_cv_index}");
            
            var job_output_folder = outer_fold_folder;
            var merge_output_folder = group_folder;

            Directory.CreateDirectory(iteration_folder);
            Directory.CreateDirectory(group_folder);
            Directory.CreateDirectory(outer_fold_folder);


            // /home/k1040015/itr_0/grp_0/svm_0_kl_0_sl_0/rnd_0_cv_0/itr_0_grp_0_rnd_0_cv_0_svm_0_kl_0_sl_0
            var filename = $@"itr_{job.iteration_index}_grp_{job.group_index}_svm_{(int)p.svm_type}_kl_{(int)p.svm_kernel}_sl_{(int)p.scale_function}_rnd_{job.randomisation_cv_index}_cv_{job.outer_cv_index}";//"_job_{job.job_id}";

            var pbs_jobname = $@"{nameof(svm_wkr)}_{job.job_id}";
            var pbs_walltime = new TimeSpan(0, 30, 0);

            var wkr_cmd_params = new cmd_params(p);
            //{
            wkr_cmd_params.cmd = cmd.wkr;
            wkr_cmd_params.pbs_wkr_jobname = pbs_jobname;
            wkr_cmd_params.pbs_wkr_walltime = $@"{pbs_walltime:hh\:mm\:ss}";
            
            wkr_cmd_params.pbs_wkr_stdout_filename = $"{filename}.pbs.stdout";
            wkr_cmd_params.pbs_wkr_stderr_filename = $"{filename}.pbs.stderr";
            wkr_cmd_params.program_wkr_stdout_filename = $"{filename}.{nameof(svm_fs)}.stdout";
            wkr_cmd_params.program_wkr_stderr_filename = $"{filename}.{nameof(svm_fs)}.stderr";

            //cmd_params.pbs_nodes = 1;
            //cmd_params.pbs_ppn = 4;
            //cmd_params.pbs_mem = $@"{(1024 * 8)}mb";

            if (wkr_cmd_params.inner_cv_folds <= 1)
            {
                wkr_cmd_params.pbs_wkr_nodes = 1;
                wkr_cmd_params.pbs_wkr_ppn = 1;
            }

            wkr_cmd_params.job_id = job.job_id;
            wkr_cmd_params.feature_id = group.columns != null && group.columns.Count == 1 && group.list != null && group.list.Count > 0 ? group.list.First().fid : -1;
            wkr_cmd_params.member = group.columns != null && group.columns.Count == 1 && group.list != null && group.list.Count > 0 ? group.list.First().member : "";
            wkr_cmd_params.perspective = group.columns != null && group.columns.Count == 1 && group.list != null && group.list.Count > 0 ? group.list.First().perspective : "";
            wkr_cmd_params.forward = job.forward;
            wkr_cmd_params.randomisation_cv_index = job.randomisation_cv_index;
            wkr_cmd_params.iteration = job.iteration_index;
            wkr_cmd_params.outer_cv_index = job.outer_cv_index;
            wkr_cmd_params.group_index = job.group_index;
            wkr_cmd_params.old_feature_count = job.old_feature_count;
            wkr_cmd_params.new_feature_count = job.new_feature_count;
            wkr_cmd_params.old_group_count = job.old_group_count;
            wkr_cmd_params.new_group_count = job.new_group_count;
            wkr_cmd_params.class_training_sizes = job.training_sizes;
            wkr_cmd_params.class_testing_sizes = job.testing_sizes;
            wkr_cmd_params.alphabet = job.group_key.alphabet;
            wkr_cmd_params.category = job.group_key.category;
            wkr_cmd_params.dimension = job.group_key.dimension;
            wkr_cmd_params.source = job.group_key.source;
            wkr_cmd_params.group = job.group_key.group;
            wkr_cmd_params.options_filename = Path.Combine(job_output_folder, $"{filename}.options");
            wkr_cmd_params.train_filename = Path.Combine(job_output_folder, $"{filename}.train.libsvm");
            wkr_cmd_params.train_id_filename = Path.Combine(job_output_folder, $"{filename}.train_id.csv");
            wkr_cmd_params.train_meta_filename = Path.Combine(job_output_folder, $"{filename}.train_meta.csv");
            wkr_cmd_params.train_grid_filename = Path.Combine(job_output_folder, $"{filename}.train_grid.csv");
            wkr_cmd_params.train_model_filename = Path.Combine(job_output_folder, $"{filename}.train_model.libsvm");
            wkr_cmd_params.test_filename = Path.Combine(job_output_folder, $"{filename}.test.libsvm");
            wkr_cmd_params.test_labels_filename = Path.Combine(job_output_folder, $"{filename}.test_labels.libsvm");
            wkr_cmd_params.test_id_filename = Path.Combine(job_output_folder, $"{filename}.test_id.csv");
            wkr_cmd_params.test_meta_filename = Path.Combine(job_output_folder, $"{filename}.test_meta.csv");
            wkr_cmd_params.test_predict_cm_filename = Path.Combine(job_output_folder, $"{filename}.test_predict_cm.csv");
            wkr_cmd_params.test_predict_filename = Path.Combine(job_output_folder, $"{filename}.test_predict.libsvm");
            //};

            wkr_cmd_params.convert_paths();

            var options_text = wkr_cmd_params.get_options_ini_text();

            var cached = true;

            if (!io_proxy.is_file_available(wkr_cmd_params.options_filename))
            {
                io_proxy.WriteAllLines(wkr_cmd_params.options_filename, options_text);
                io_proxy.WriteLine("Saved options: " + wkr_cmd_params.options_filename, nameof(svm_ctl), nameof(do_job));
                cached = false;
            }
            else
            {
                io_proxy.WriteLine($"Using cached options: {wkr_cmd_params.options_filename}", nameof(svm_ctl), nameof(do_job));
            }

            if (!io_proxy.is_file_available(wkr_cmd_params.train_filename))
            {
                io_proxy.WriteAllLines(wkr_cmd_params.train_filename, job.training_text);
                io_proxy.WriteLine($"Saved training data: {wkr_cmd_params.train_filename}", nameof(svm_ctl), nameof(do_job));
                cached = false;
            }
            else
            {
                io_proxy.WriteLine($"Using cached training data: {wkr_cmd_params.train_filename}", nameof(svm_ctl), nameof(do_job));
            }

            if (p.save_train_id)
            {
                if (!io_proxy.is_file_available(wkr_cmd_params.train_id_filename))
                {
                    io_proxy.WriteAllLines(wkr_cmd_params.train_id_filename, job.training_id_text);
                    io_proxy.WriteLine($"Saved training ids: {wkr_cmd_params.train_id_filename}", nameof(svm_ctl), nameof(do_job));
                    cached = false;
                }
                else
                {
                    io_proxy.WriteLine($"Using cached training ids: {wkr_cmd_params.train_id_filename}", nameof(svm_ctl), nameof(do_job));
                }
            }

            if (p.save_train_meta)
            {
                if (!io_proxy.is_file_available(wkr_cmd_params.train_meta_filename))
                {
                    io_proxy.WriteAllLines(wkr_cmd_params.train_meta_filename, job.training_meta_text);
                    io_proxy.WriteLine($"Saved training meta: {wkr_cmd_params.train_meta_filename}", nameof(svm_ctl), nameof(do_job));
                    cached = false;
                }
                else
                {
                    io_proxy.WriteLine($"Using cached training meta: {wkr_cmd_params.train_meta_filename}", nameof(svm_ctl), nameof(do_job));
                }
            }

            if (!io_proxy.is_file_available(wkr_cmd_params.test_filename))
            {
                io_proxy.WriteAllLines(wkr_cmd_params.test_filename, job.testing_text);
                io_proxy.WriteLine($"Saved testing data: {wkr_cmd_params.test_filename}", nameof(svm_ctl), nameof(do_job));
                cached = false;
            }
            else
            {
                io_proxy.WriteLine($"Using cached testing data: {wkr_cmd_params.test_filename}", nameof(svm_ctl), nameof(do_job));
            }

            if (!io_proxy.is_file_available(wkr_cmd_params.test_labels_filename))
            {
                var test_labels = job.testing_text.Select(a => a.Length > 0 ? a.Substring(0, a.IndexOf(' ') > -1 ? a.IndexOf(' ') : 0) : "").ToList();
                io_proxy.WriteAllLines(wkr_cmd_params.test_labels_filename, test_labels);
                io_proxy.WriteLine($"Saved testing labels data: {wkr_cmd_params.test_labels_filename}", nameof(svm_ctl), nameof(do_job));
                cached = false;
            }
            else
            {
                io_proxy.WriteLine($"Using cached testing labels data: {wkr_cmd_params.test_labels_filename}", nameof(svm_ctl), nameof(do_job));
            }

            if (p.save_test_id)
            {
                if (!io_proxy.is_file_available(wkr_cmd_params.test_id_filename))
                {
                    io_proxy.WriteAllLines(wkr_cmd_params.test_id_filename, job.testing_id_text);
                    io_proxy.WriteLine($"Saved testing ids: {wkr_cmd_params.test_id_filename}", nameof(svm_ctl), nameof(do_job));
                    cached = false;
                }
                else
                {
                    io_proxy.WriteLine($"Using cached testing ids: {wkr_cmd_params.test_id_filename}", nameof(svm_ctl), nameof(do_job));
                }
            }

            if (p.save_test_meta)
            {
                if (!io_proxy.is_file_available(wkr_cmd_params.test_meta_filename))
                {
                    io_proxy.WriteAllLines(wkr_cmd_params.test_meta_filename, job.testing_meta_text);
                    io_proxy.WriteLine($"Saved testing meta: {wkr_cmd_params.test_meta_filename}", nameof(svm_ctl), nameof(do_job));
                    cached = false;
                }
                else
                {
                    io_proxy.WriteLine($"Using cached testing meta: {wkr_cmd_params.test_meta_filename}", nameof(svm_ctl), nameof(do_job));
                }
            }
            //cmds.Add($@"start cmd /c {cmd_params.program_runtime} -j {cmd_params.options_filename}");

            var wait_file_list = new List<string>();

            if (wkr_cmd_params.inner_cv_folds > 1)
            {
                wait_file_list.Add(wkr_cmd_params.train_grid_filename);
            }

            //wait_file_list.Add(cmd_params.train_model_filename);
            wait_file_list.Add(wkr_cmd_params.test_predict_filename);
            wait_file_list.Add(wkr_cmd_params.test_predict_cm_filename);

            if (cached)
            {
                foreach (var f in wait_file_list)
                {
                    if (!io_proxy.is_file_available(f))
                    {
                        cached = false;
                        break;
                    }
                }
            }

            //var merge_filename = $"itr_{job.iteration_index}_grp_{job.group_index}_rnd_{job.randomisation_cv_index}_cv_{job.outer_cv_index}_svm_{(int)p.svm_type}_kl_{(int)p.svm_kernel}_sl_{(int)p.scale_function}";//"_job_{job.job_id}";

            var merge_filename = $"itr_{job.iteration_index}_grp_{job.group_index}_svm_{(int)p.svm_type}_kl_{(int)p.svm_kernel}_sl_{(int)p.scale_function}"; // note: randomsiations are included as part of the outer-cv, not separate results ... if wanted seperate use: _{job.randomisation}"; // note: no job id

            var merge_cmd_params = new cmd_params(wkr_cmd_params)
            {
                pbs_ctl_stdout_filename = Path.Combine(merge_output_folder, $"{merge_filename}.merge.{nameof(svm_ctl)}.pbs.stdout"),
                pbs_ctl_stderr_filename = Path.Combine(merge_output_folder, $"{merge_filename}.merge.{nameof(svm_ctl)}.pbs.stderr"),
                pbs_wkr_stdout_filename = Path.Combine(merge_output_folder, $"{merge_filename}.merge.{nameof(svm_wkr)}.pbs.stdout"),
                pbs_wkr_stderr_filename = Path.Combine(merge_output_folder, $"{merge_filename}.merge.{nameof(svm_wkr)}.pbs.stderr"),
                program_ctl_stdout_filename = Path.Combine(merge_output_folder, $"{merge_filename}.merge.{nameof(svm_ctl)}.{nameof(svm_fs)}.stdout"),
                program_ctl_stderr_filename = Path.Combine(merge_output_folder, $"{merge_filename}.merge.{nameof(svm_ctl)}.{nameof(svm_fs)}.stderr"),
                program_wkr_stdout_filename = Path.Combine(merge_output_folder, $"{merge_filename}.merge.{nameof(svm_wkr)}.{nameof(svm_fs)}.stdout"),
                program_wkr_stderr_filename = Path.Combine(merge_output_folder, $"{merge_filename}.merge.{nameof(svm_wkr)}.{nameof(svm_fs)}.stderr"),

                train_filename = Path.Combine(merge_output_folder, $"{merge_filename}.train.libsvm"),
                train_id_filename = Path.Combine(merge_output_folder, $"{merge_filename}.train_id.csv"),
                train_meta_filename = Path.Combine(merge_output_folder, $"{merge_filename}.train_meta.csv"),
                train_grid_filename = Path.Combine(merge_output_folder, $"{merge_filename}.train_grid.csv"),
                train_model_filename = Path.Combine(merge_output_folder, $"{merge_filename}.train_model.libsvm"),
                test_filename = Path.Combine(merge_output_folder, $"{merge_filename}.test.libsvm"),
                test_labels_filename = Path.Combine(merge_output_folder, $"{merge_filename}.test_labels.libsvm"),
                test_id_filename = Path.Combine(merge_output_folder, $"{merge_filename}.test_id.csv"),
                test_meta_filename = Path.Combine(merge_output_folder, $"{merge_filename}.test_meta.csv"),
                test_predict_cm_filename = Path.Combine(merge_output_folder, $"{merge_filename}.test_predict_cm.csv"),
                test_predict_filename = Path.Combine(merge_output_folder, $"{merge_filename}.test_predict.libsvm"),
            };

            merge_cmd_params.convert_paths();

            var to_merge = new List<(bool wait_first, bool has_header, string average_out_filename, string merge_out_filename, string merge_in_filename, string average_header)>();

            if (p.save_train_id) { to_merge.Add((false, true, null, merge_cmd_params.train_id_filename, wkr_cmd_params.train_id_filename, null)); }
            if (p.save_train_meta) { to_merge.Add((false, true, null, merge_cmd_params.train_meta_filename, wkr_cmd_params.train_meta_filename, null)); }

            if (p.save_test_id) { to_merge.Add((false, true, null, merge_cmd_params.test_id_filename, wkr_cmd_params.test_id_filename, null)); }
            if (p.save_test_meta) { to_merge.Add((false, true, null, merge_cmd_params.test_meta_filename, wkr_cmd_params.test_meta_filename, null)); }

            if (wkr_cmd_params.inner_cv_folds > 1) { to_merge.Add((true, true, null, merge_cmd_params.train_grid_filename, wkr_cmd_params.train_grid_filename, null)); }
            to_merge.Add((false, false, null, merge_cmd_params.test_filename, wkr_cmd_params.test_filename, null));
            to_merge.Add((false, false, null, merge_cmd_params.test_labels_filename, wkr_cmd_params.test_labels_filename, null));
            to_merge.Add((true, p.libsvm_train_probability_estimates, null, merge_cmd_params.test_predict_filename, wkr_cmd_params.test_predict_filename, null));
            to_merge.Add((true, true, null, merge_cmd_params.test_predict_cm_filename, wkr_cmd_params.test_predict_cm_filename, "class_id"));

            if (!cached)
            {
                submit_pbs_job(wkr_cmd_params);
            }

            var ret = (
                       wait_file_list,
                       wkr_cmd_params,
                       merge_cmd_params,
                       to_merge
                       );

            return ret;
        }

        public static void submit_pbs_job(cmd_params cmd_params) //(string jobs_fn)
        {
            var sub_dir = "";

            if (cmd_params.cmd == cmd.ctl) { sub_dir = cmd_params.pbs_ctl_submission_directory; }
            else if (cmd_params.cmd == cmd.wkr) { sub_dir = cmd_params.pbs_wkr_submission_directory; }

            var source = cmd_params.options_filename;
            var dest = Path.Combine(Path.GetDirectoryName(sub_dir), Path.GetFileName(cmd_params.options_filename));

            if (source != dest)
            {
                io_proxy.Copy(source, dest, false, nameof(svm_ctl), nameof(submit_pbs_job));
            }

            //var psi = new ProcessStartInfo()
            //{
            //    FileName = jobs_fn,
            //
            //    RedirectStandardError = true,
            //    RedirectStandardInput = true,
            //    RedirectStandardOutput = true,
            //    UseShellExecute = false,
            //    CreateNoWindow = false,
            //    WindowStyle = ProcessWindowStyle.Hidden,
            //};
            //
            //Process.Start(psi);
        }

        public static void update_cm(cmd_params p, List<performance_measure.confusion_matrix> cm_list)
        {
            for (var i = 0; i < cm_list.Count; i++)
            {
                var cm = cm_list[i];

                //cm.ext_duration_grid_search = p.duration_grid_search;
                //cm.ext_duration_training = p.duration_training;
                //cm.ext_duration_testing = p.duration_testing;
                cm.ext_experiment_name = p.experiment_name;
                cm.ext_scaling_function = p.scale_function.ToString();
                //cm.ext_libsvm_cv = p.libsvm_cv;
                //cm.ext_prediction_threshold = p.prediction_threshold;
                //cm.ext_prediction_threshold_class = p.prediction_threshold_class;
                cm.ext_old_feature_count = p.old_feature_count;
                cm.ext_new_feature_count = p.new_feature_count;
                cm.ext_old_group_count = p.old_group_count;
                cm.ext_new_group_count = p.new_group_count;
                //cm.ext_features_included = p.features_included;
                cm.ext_inner_cv_folds = p.inner_cv_folds;
                cm.ext_randomisation_cv_index = p.randomisation_cv_index;
                cm.ext_randomisation_cv_folds = p.randomisation_cv_folds;
                cm.ext_outer_cv_index = p.outer_cv_index;
                cm.ext_outer_cv_folds = p.outer_cv_folds;
                cm.ext_svm_type = p.svm_type.ToString();
                cm.ext_svm_kernel = p.svm_kernel.ToString();
                //cm.ext_cost = p.cost;
                //cm.ext_gamma = p.gamma;
                //cm.ext_epsilon = p.epsilon;
                //cm.ext_coef0 = p.coef0;
                //cm.ext_degree = p.degree;
                cm.ext_class_weight = p.class_weights?.FirstOrDefault(b => cm.class_id == b.class_id).class_weight;
                cm.ext_class_name = p.class_names?.FirstOrDefault(b => cm.class_id == b.class_id).class_name;
                cm.ext_class_size = p.class_sizes?.First(b => b.class_id == cm.class_id).class_size ?? -1;
                cm.ext_class_training_size = p.class_training_sizes?.First(b => b.class_id == cm.class_id).class_training_size ?? -1;
                cm.ext_class_testing_size = p.class_testing_sizes?.First(b => b.class_id == cm.class_id).class_testing_size ?? -1;

                cm.calculate_ppf();

                //join outer-cv-cms together to create 1 cm - roc - pr - etc
            }
        }

        //        public static void save_cm()
        //        {
        //            var cm_lines = new List<string>();
        //
        //            cm_lines.Add(performance_measure.confusion_matrix.csv_header);
        //
        //            for (var i = 0; i < cm_list.Count; i++)
        //            {
        //                var cm = cm_list[i];
        //
        //                cm_lines.Add(cm.ToString());
        //            }
        //
        //            io_proxy.WriteAllLines(cm_output_file, cm_lines);
        //        }

        

        public static TimeSpan calc_eta(Stopwatch sw, int current_amount, int total_amount)
        {
            if (current_amount == 0) return TimeSpan.Zero;
            var elapsed = (double)sw.ElapsedMilliseconds;

            var time_per_item = elapsed / current_amount;
            var items_left = total_amount - current_amount;

            var ms_left = time_per_item * items_left;

            TimeSpan result = TimeSpan.FromMilliseconds(ms_left);

            return result;
        }
        
        //public static Task file_wait_task;
        //public static List<string> file_wait_list = new List<string>();

        //public static object wait_ready_lock = new object();
        //public static int wait_items;
        //public static int wait_items_ready;
        //public static bool wait_ready;

        public static void wait_for_results(List<string> file_wait_list)
        {
            //lock (wait_ready_lock)
            //{
            //    wait_items_ready++;

            //    if (wait_items_ready == wait_items)
            //    {
            //        wait_ready = true;
            //    }
            //}

            //while (!wait_ready)
            //{
                
            //}

            io_proxy.WriteLine($@"Waiting for {file_wait_list.Count} files.", nameof(svm_ctl), nameof(wait_for_results));

            if (file_wait_list == null || file_wait_list.Count == 0) return;

            file_wait_list = file_wait_list.ToList();
            var files_found = new List<(string filename, DateTime time)>();

            var total_files = file_wait_list.Count;

            if (total_files == 0) return;

            var total_found = 0;

            var sw1 = new Stopwatch();
            sw1.Start();

            var itr = 0;

            while (true)
            {
                itr++;

                var new_files_found = file_wait_list.Where(a => io_proxy.is_file_available(a)).ToList();

                if (new_files_found.Count > 0)
                {
                    itr = 0;

                    io_proxy.WriteLine("New files found: ", nameof(svm_ctl), nameof(wait_for_results));
                    for (var j = 0; j < new_files_found.Count; j++)
                    {
                        io_proxy.WriteLine($@"({j}): {new_files_found[j]}", nameof(svm_ctl), nameof(wait_for_results));
                    }



                    files_found.AddRange(new_files_found.Select((a, i) => (a, DateTime.Now)).ToList());

                    file_wait_list = file_wait_list.Except(new_files_found).ToList();
                    total_found += new_files_found.Count;

                    var pct = ((double)total_found / (double)total_files) * (double)100;

                    io_proxy.WriteLine($@"Files ready: {total_found} / {total_files} ( {pct:0.00}% ) [ Time: {sw1.Elapsed.ToString()} ] [ ETA: {calc_eta(sw1, total_found, total_files).ToString()} ]", nameof(svm_ctl), nameof(wait_for_results));
                }
                else if (itr >= 6)
                {
                    itr = 0;

                    var pct = ((double)total_found / (double)total_files) * (double)100;

                    io_proxy.WriteLine($@"Files ready: {total_found} / {total_files} ( {pct:0.00}% ) [ Time: {sw1.Elapsed.ToString()} ] [ ETA: {calc_eta(sw1, total_found, total_files).ToString()} ]", nameof(svm_ctl), nameof(wait_for_results));

                }

                if (file_wait_list.Count == 0)
                {
                    return;
                }

                Task.Delay(new TimeSpan(0, 0, 0, 30)).Wait();
            }
        }
    }
}