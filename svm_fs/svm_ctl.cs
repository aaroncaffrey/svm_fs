using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace svm_fs
{
    public static class svm_ctl
    {
        public static void WriteLine(string text, string module, string func)
        {
            try
            {
                Console.WriteLine($@"{DateTime.Now:G} {module}.{func}: {text}");
            }
            catch (Exception)
            {

            }
        }

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

        public static double PopulationStandardDeviation(List<double> numberSet)
        {
            double mean = numberSet.Average();

            return Math.Sqrt(numberSet.Sum(x => Math.Pow(x - mean, 2)) / (numberSet.Count));
        }

        public static double SampleStandardDeviation(List<double> numberSet)
        {
            double mean = numberSet.Average();

            return Math.Sqrt(numberSet.Sum(x => Math.Pow(x - mean, 2)) / (numberSet.Count - 1));
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
                    return 0;
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

        public static void interactive(cmd_params p)
        {
            //p.feature_count = 0;
            //p.group_count = 0;

            // Load dataset
            //svm_ctl.WriteLine("Start: Loading dataset...");

            var dataset = dataset_loader.read_binary_dataset(p.dataset_dir, p.negative_class_id, p.positive_class_id, p.class_names, use_parallel: true, perform_integrity_checks: false, fix_double: false);


            //svm_ctl.WriteLine("End: Loading dataset...");

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

            svm_ctl.WriteLine("", nameof(svm_ctl), nameof(interactive));
            svm_ctl.WriteLine($@"--------------- Performing greedy feature selection on {groups.Count} groups ---------------", nameof(svm_ctl), nameof(interactive));

            groups = groups.Take(10).ToList();

            //group count -- total groups currently selected/testing
            //feature count -- total features currently selected/testing

            svm_ctl.WriteLine("", nameof(svm_ctl), nameof(interactive));
            svm_ctl.WriteLine($@"--------------- Performing greedy feature selection on {groups.Count} groups ---------------", nameof(svm_ctl), nameof(interactive));

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

            var root_folder = dataset_loader.convert_path(p.results_root_folder);
            var summary_fn = dataset_loader.convert_path(Path.Combine(root_folder, "summary.csv"));

            var summary_lines = new List<string>();

            sw_fs.Start();

            do
            {
                svm_ctl.WriteLine("", nameof(svm_ctl), nameof(interactive));
                svm_ctl.WriteLine($@"--------------- Start of iteration {iteration_index} ---------------", nameof(svm_ctl), nameof(interactive));
                svm_ctl.WriteLine("", nameof(svm_ctl), nameof(interactive));


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

                var sw_iteration = new Stopwatch();
                sw_iteration.Start();

                
                var iteration_folder = dataset_loader.convert_path(Path.Combine(root_folder, $"itr_{iteration_index}"));
                var checkpoint_fn = dataset_loader.convert_path(Path.Combine(iteration_folder, $@"{nameof(currently_selected_group_indexes)}.txt"));
                var previous_tests_fn = dataset_loader.convert_path(Path.Combine(iteration_folder, $@"previous_tests.txt"));

                if (is_file_available(checkpoint_fn))
                {
                    svm_ctl.WriteLine($@"--------------- Checkpoint loaded: {checkpoint_fn} ---------------", nameof(svm_ctl), nameof(interactive));


                    currently_selected_group_indexes.Clear();
                    highest_scoring_group_indexes.Clear();

                    var checkpoint_data1 = File.ReadAllLines(checkpoint_fn);
                    var previous_tests_data = File.ReadAllLines(previous_tests_fn);
                    previous_tests = previous_tests_data.Select(a => a.Split(';').Select(b => int.Parse(b)).ToList()).ToList();

                    foreach (var cpd in checkpoint_data1)
                    {
                        var cpd_key = cpd.Substring(0, cpd.IndexOf('='));
                        var cpd_value = cpd.Substring(cpd_key.Length + 1);

                        switch (cpd_key)
                        {
                            case "iteration_index":
                                iteration_index = int.Parse(cpd_value);
                                break;

                            case "winning_iteration":
                                winning_iteration = int.Parse(cpd_value);
                                break;

                            case "iterations_not_better_than_last":
                                iterations_not_better_than_last = int.Parse(cpd_value);
                                break;

                            case "iterations_not_better_than_all":
                                iterations_not_better_than_all = int.Parse(cpd_value);
                                break;

                            case "currently_selected_group_indexes":
                                currently_selected_group_indexes.AddRange(cpd_value.Split(';').Select(a => int.Parse(a)).ToList());
                                break;

                            case "highest_scoring_group_indexes":
                                highest_scoring_group_indexes.AddRange(cpd_value.Split(';').Select(a => int.Parse(a)).ToList());
                                break;

                            case "highest_score_last_iteration":
                                highest_score_last_iteration = double.Parse(cpd_value);
                                break;

                            case "highest_score_this_iteration":
                                highest_score_this_iteration = double.Parse(cpd_value);
                                break;

                            case "highest_score_all_iteration":
                                highest_score_all_iteration = double.Parse(cpd_value);
                                break;

                            case "score_increase_from_last":
                                score_increase_from_last = double.Parse(cpd_value);
                                break;

                            case "score_increase_from_all":
                                score_increase_from_all = double.Parse(cpd_value);
                                break;

                            case "score_better_than_last":
                                score_better_than_last = bool.Parse(cpd_value);
                                break;

                            case "score_better_than_all":
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

                    
                    //svm_ctl.WriteLine($@"--------------- Start of iteration {iteration_index} ---------------", nameof(svm_ctl), nameof(interactive));

                    svm_ctl.WriteLine("", nameof(svm_ctl), nameof(interactive));
                    svm_ctl.WriteLine($@"Memory usage: {(GC.GetTotalMemory(false) / 1_000_000_000d):F2} GB", nameof(svm_ctl), nameof(interactive));
                    svm_ctl.WriteLine("", nameof(svm_ctl), nameof(interactive));

                    //score_improved = false;
                    //var _iteration_index = iteration_index;
                    var currently_selected_groups = currently_selected_group_indexes.Select(a => groups[a]).ToList();
                    var currently_selected_groups_columns = currently_selected_groups.SelectMany(a => a.columns).OrderBy(a => a).Distinct().ToList();

                    var jobs_group_level = Enumerable.Range(0, groups.Count).AsParallel().AsOrdered().Select(group_index =>
                    {
                        var is_group_index_selected = currently_selected_group_indexes.Contains(group_index);
                        var forward = !is_group_index_selected;

                        var this_test_group_indexes = currently_selected_group_indexes.ToList();


                        if (forward)
                        {
                            this_test_group_indexes.Add(group_index);
                        }
                        else
                        {
                            this_test_group_indexes.Remove(group_index);

                            // do not remove if the only selected feature 
                            if (currently_selected_group_indexes.Count == 1)
                            {
                                svm_ctl.WriteLine($@"{nameof(iteration_index)}={iteration_index}, {nameof(group_index)}={group_index}, currently_selected_group_indexes.Count == 1", nameof(svm_ctl), nameof(interactive));

                                return default;
                            }


                            // do not remove if was just added
                            if (highest_score_last_iteration_group_index == group_index)
                            {
                                svm_ctl.WriteLine($@"{nameof(iteration_index)}={iteration_index}, {nameof(group_index)}={group_index}, highest_score_last_iteration_group_index == group_index", nameof(svm_ctl), nameof(interactive));

                                return default;
                            }
                        }

                        this_test_group_indexes = this_test_group_indexes.OrderBy(a => a).Distinct().ToList();


                        var already_tested = previous_tests.Any(a => a.SequenceEqual(this_test_group_indexes));

                        // do not repeat the same test
                        if (already_tested)
                        {

                            svm_ctl.WriteLine($@"{nameof(iteration_index)}={iteration_index}, {nameof(group_index)}={group_index}, already tested: {string.Join(",", this_test_group_indexes)}", nameof(svm_ctl), nameof(interactive));

                            return default;
                        }

                        var group = group_index > -1 ? groups[group_index] : default;

                        var group_key = group.key;

                        var old_group_count = currently_selected_groups.Count;
                        var new_group_count = this_test_group_indexes.Count; //old_group_count + (is_group_index_selected ? -1 : 1);

                        var query_cols = is_group_index_selected ? currently_selected_groups_columns.Except(group.columns).OrderBy(a => a).Distinct().ToList() : currently_selected_groups_columns.Union(group.columns).OrderBy(a => a).Distinct().ToList();

                        var old_feature_count = currently_selected_groups_columns.Count;
                        var new_feature_count = query_cols.Count;




                        var jobs_randomisation_level = group_cv(
                            0,
                            p,
                            downsampled_training_class_folds,
                            class_folds,
                            dataset_instance_list_grouped,
                            iteration_index,
                            group_index,
                            groups,
                            query_cols,
                            forward,
                            old_feature_count,
                            new_feature_count,
                            old_group_count,
                            new_group_count,
                            group_key,
                            this_test_group_indexes);

                        return jobs_randomisation_level;

                    }).ToList();

                    svm_ctl.WriteLine("", nameof(svm_ctl), nameof(interactive));
                    svm_ctl.WriteLine($@"Memory usage: {(GC.GetTotalMemory(false) / 1_000_000_000d):F2} GB", nameof(svm_ctl), nameof(interactive));
                    svm_ctl.WriteLine("", nameof(svm_ctl), nameof(interactive));

                    jobs_group_level = jobs_group_level.Where(a => a != default).ToList();

                    if (jobs_group_level == null || jobs_group_level.Count == 0)
                    {
                        svm_ctl.WriteLine($@"{nameof(iteration_index)}={iteration_index}: jobs_group_level == null || jobs_group_level.Count == 0", nameof(svm_ctl), nameof(interactive));

                        break;
                    }

                    jobs_group_level.ForEach(a => previous_tests.Add(a.this_test_group_indexes));

                    // get ranks by group
                    var cm_inputs = get_cm_inputs(p, jobs_group_level, groups, iteration_folder, iteration_index);

                    if (cm_inputs == null || cm_inputs.Count <= 0)
                    {
                        svm_ctl.WriteLine($@"{nameof(iteration_index)}={iteration_index}: cm_inputs == null || cm_inputs.Count <= 0", nameof(svm_ctl), nameof(interactive));

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

                    svm_ctl.WriteLine($@"Score {(score_better_than_last ? "improved" : "not improved")} from last iteration; score {(score_better_than_last ? "increased" : "decreased")} by {score_increase_from_last} from {highest_score_last_iteration} to {highest_score_this_iteration}.", nameof(svm_ctl), nameof(interactive));
                    svm_ctl.WriteLine($@"Best score {(score_better_than_last ? "is": "would have been")} to {(forward ? "add" : "remove")} group {winner.cmd_params.group_index} {groups[winner.cmd_params.group_index].key}", nameof(svm_ctl), nameof(interactive));

                    // highest_score_last_iteration


                    // 1. check if grid search results cache are loaded?
                    // 2. rerun file if kernel refuses

                    // 1. per feature: output cm per outer-fold
                    // 2. per feature: combine outer-fold cm into one file
                    // 3. per feature: output cm (all results combined into one prediction file)
                    // 4. all features: output cm with features ordered by predictive ability rank

                    svm_ctl.WriteLine($@"", nameof(svm_ctl), nameof(interactive));
                    svm_ctl.WriteLine($@"Memory usage: {(GC.GetTotalMemory(false) / 1_000_000_000d):F2} GB", nameof(svm_ctl), nameof(interactive));
                    svm_ctl.WriteLine($@"", nameof(svm_ctl), nameof(interactive));
                    //svm_ctl.WriteLine($@"--------------- End of iteration {iteration_index} ---------------", nameof(svm_ctl), nameof(interactive));


                    sw_iteration.Stop();
                    var elapsed_iteration = $@"{sw_iteration.Elapsed:dd\:hh\:mm\:ss}";
                    var elapsed_fs = $@"{sw_fs.Elapsed:dd\:hh\:mm\:ss}";

                    var checkpoint_data = new List<(string key, string value)>();
                    checkpoint_data.Add(($@"{nameof(iteration_index)}", $@"{iteration_index}"));
                    checkpoint_data.Add(($@"{nameof(winning_iteration)}", $@"{winning_iteration}"));
                    checkpoint_data.Add(($@"{nameof(highest_score_last_iteration_group_index)}", $@"{highest_score_last_iteration_group_index}"));
                    checkpoint_data.Add(($@"{nameof(highest_score_this_iteration_group_index)}", $@"{highest_score_this_iteration_group_index}"));
                    checkpoint_data.Add(($@"{nameof(highest_score_last_iteration_group_key)}", $@"{highest_score_last_iteration_group_key}"));
                    checkpoint_data.Add(($@"{nameof(highest_score_this_iteration_group_key)}", $@"{highest_score_this_iteration_group_key}"));
                    checkpoint_data.Add(($@"{nameof(forward)}", $@"{forward}"));
                    checkpoint_data.Add(($@"{nameof(iterations_not_better_than_last)}", $@"{iterations_not_better_than_last}"));
                    checkpoint_data.Add(($@"{nameof(iterations_not_better_than_all)}", $@"{iterations_not_better_than_all}")); 
                  //checkpoint_data.Add(($@"{nameof(selected_group_indexes)}", $@"{selected_group_indexes}"));
                    checkpoint_data.Add(($@"{nameof(highest_score_last_iteration)}", $@"{highest_score_last_iteration}"));
                    checkpoint_data.Add(($@"{nameof(highest_score_this_iteration)}", $@"{highest_score_this_iteration}"));
                    checkpoint_data.Add(($@"{nameof(highest_score_all_iteration)}", $@"{highest_score_all_iteration}"));
                    checkpoint_data.Add(($@"{nameof(score_increase_from_last)}", $@"{score_increase_from_last}"));
                    checkpoint_data.Add(($@"{nameof(score_increase_from_all)}", $@"{score_increase_from_all}"));
                    checkpoint_data.Add(($@"{nameof(score_better_than_last)}", $@"{score_better_than_last}"));
                    checkpoint_data.Add(($@"{nameof(score_better_than_all)}", $@"{score_better_than_all}"));
                    checkpoint_data.Add(($@"{nameof(elapsed_iteration)}", $@"{elapsed_iteration}"));
                    checkpoint_data.Add(($@"{nameof(elapsed_fs)}", $@"{elapsed_fs}"));
                    checkpoint_data.Add(($@"{nameof(currently_selected_group_indexes)}", $@"{string.Join(";", currently_selected_group_indexes)}"));
                    checkpoint_data.Add(($@"{nameof(highest_scoring_group_indexes)}", $@"{string.Join(";", highest_scoring_group_indexes)}"));

                    checkpoint_data.AddRange(winner.cmd_params.get_options().Select(a => ($"p_{a.key}", a.value)).ToList());
                    //checkpoint_data.AddRange(winner.cms.);

                    File.WriteAllLines(checkpoint_fn, checkpoint_data.Select(a => $"{a.key}={a.value}").ToList());

                    //var summary_fn = dataset_loader.convert_path(Path.Combine(iteration_folder, "summary.csv"));
                    
                    

                    if (summary_lines == null || summary_lines.Count == 0)
                    {
                        summary_lines.Add($"{string.Join(",", checkpoint_data.Select(a => a.key).ToList())},{string.Join(",", performance_measure.confusion_matrix.csv_header.Select(h => $"cm_{h}").ToList())}");
                    }
                    //  todo: add winnter cmd_params

                    winner.cms.cm_list.ForEach(c => summary_lines.Add($"{string.Join(",", checkpoint_data.Select(a => a.value).ToList())},{c.ToString()}"));

                    File.WriteAllLines(summary_fn, summary_lines);

                    var previous_tests_data = previous_tests.Select(a => string.Join(";", a)).ToList();
                    File.WriteAllLines(previous_tests_fn, previous_tests_data);

                }

                // previous winning iterations
                // route taken 


                svm_ctl.WriteLine($@"score_better_than_last = {score_better_than_last}", nameof(svm_ctl), nameof(interactive));
                svm_ctl.WriteLine($@"(iterations_not_better_than_last < limit_iteration_not_better_than_last && iterations_not_better_than_all < limit_iteration_not_better_than_all) = {(iterations_not_better_than_last < limit_iteration_not_better_than_last && iterations_not_better_than_all < limit_iteration_not_better_than_all)}", nameof(svm_ctl), nameof(interactive));
                svm_ctl.WriteLine($@"currently_selected_group_indexes.Count < groups.Count = {currently_selected_group_indexes.Count < groups.Count}", nameof(svm_ctl), nameof(interactive));

                svm_ctl.WriteLine($@"--------------- End of iteration {iteration_index} ---------------", nameof(svm_ctl), nameof(interactive));

                 
            }
            while
            (

                (score_better_than_last || (iterations_not_better_than_last < limit_iteration_not_better_than_last && iterations_not_better_than_all < limit_iteration_not_better_than_all))

                &&

                (currently_selected_group_indexes.Count < groups.Count)

            );

            svm_ctl.WriteLine($@"", nameof(svm_ctl), nameof(interactive));
            svm_ctl.WriteLine($@"Finished after {iteration_index} iterations", nameof(svm_ctl), nameof(interactive));

            svm_ctl.WriteLine($@"", nameof(svm_ctl), nameof(interactive));
            svm_ctl.WriteLine($@"Winning iteration: {winning_iteration}", nameof(svm_ctl), nameof(interactive));
            svm_ctl.WriteLine($@"Winning score: {highest_score_all_iteration}", nameof(svm_ctl), nameof(interactive));
            svm_ctl.WriteLine($@"Winning group indexes: {string.Join(", ", highest_scoring_group_indexes)}", nameof(svm_ctl), nameof(interactive));


            // todo: save final list

            var final_list_fn = Path.Combine(root_folder, "final_list.txt");
            var final_list_txt = new List<string>();
            var fl = highest_scoring_group_indexes.Select(a => $"{a},{groups[a].key},{string.Join(";",groups[a].columns)}").ToList();
            File.WriteAllLines(final_list_fn, final_list_txt);

            svm_ctl.WriteLine($@"", nameof(svm_ctl), nameof(interactive));


            // after finding winning solution, check performance with different kernels and scaling methods

            var find_best_params = false;

            if (find_best_params)
            {
                // todo: make it do this for the winning iteration (rather than the last iteration)

                var kernel_types = ((common.libsvm_kernel_type[]) Enum.GetValues(typeof(common.libsvm_kernel_type))).Where(a => a != common.libsvm_kernel_type.precomputed).ToList();
                var scale_functions = ((common.scale_function[]) Enum.GetValues(typeof(common.scale_function))).Where(a => a != common.scale_function.none).ToList();


                svm_ctl.WriteLine($@"Starting to do kernel and scale ranking...", nameof(svm_ctl), nameof(interactive));

                iteration_index++;
                var jobs_group_level1 = Enumerable.Range(0, kernel_types.Count).AsParallel().AsOrdered().Select(kernel_index =>
                {
                    var kernel_type = kernel_types[kernel_index];

                    var jobs_group_level2 = Enumerable.Range(0, scale_functions.Count).AsParallel().AsOrdered().Select(scale_function_index =>
                    {
                        var scale_function = scale_functions[scale_function_index];

                        var p2 = new cmd_params(p);
                        p2.svm_kernel = kernel_type;
                        p2.scale_function = scale_function;
                        p2.iteration = -1;


                        var currently_selected_groups = highest_scoring_group_indexes.Select(a => groups[a]).ToList();
                        var currently_selected_groups_columns = currently_selected_groups.SelectMany(a => a.columns).OrderBy(a => a).Distinct().ToList();
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



                        var group_cv_result = group_cv(1, p2, downsampled_training_class_folds, class_folds, dataset_instance_list_grouped, iteration_index, group_index, groups, query_cols, forward, old_feature_count, new_feature_count, old_group_count, new_group_count, group_key, this_test_group_indexes);

                        return group_cv_result;
                    }).ToList();


                    return jobs_group_level2;
                }).ToList();

                var jobs_group_level0 = jobs_group_level1.Where(a => a != default).SelectMany(a => a.Where(b => b != default).ToList()).ToList();

                svm_ctl.WriteLine($@"Finished to do kernel and scale ranking...", nameof(svm_ctl), nameof(interactive));

                //foreach (common.libsvm_kernel_type kernel_type in Enum.GetValues(typeof(common.libsvm_kernel_type)))
                //{
                //    if (kernel_type == common.libsvm_kernel_type.precomputed) continue;
                //    foreach (common.scale_function scale_function in Enum.GetValues(typeof(common.scale_function)))
                //    {
                //        if (scale_function == common.scale_function.none) continue;
                //    }
                //}

                // get ranks by kernel/scaling


                var iteration_folder1 = dataset_loader.convert_path(Path.Combine(p.results_root_folder, $"itr_{iteration_index}"));

                var cm_inputs1 = get_cm_inputs(p, jobs_group_level0, groups, iteration_folder1, iteration_index);

                svm_ctl.WriteLine($@"Finished ranking the highest scoring group...", nameof(svm_ctl), nameof(interactive));
            }

            // output winner details
        }

        private static List<((string test_file, string test_comments_file, string prediction_file, string cm_file) filenames, (List<performance_measure.prediction> prediction_list, List<performance_measure.confusion_matrix> cm_list) cms, cmd_params cmd_params)>

            get_cm_inputs(
                cmd_params ranking_metric_params,
                List<(List<List<(List<string> wait_file_list, cmd_params cmd_params, cmd_params merge_cmd_params, List<(bool wait_first, bool has_header, string average_out_filename, string merge_out_filename, string merge_in_filename, string average_header)> to_merge)>> jobs_randomisation_level, List<((string test_file, string test_comments_file, string prediction_file, string cm_file) filenames, (List<performance_measure.prediction> prediction_list, List<performance_measure.confusion_matrix> cm_list) cms, cmd_params cmd_params)> merge_cm_inputs, List<int> this_test_group_indexes)> jobs_group_level,
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
            cm_inputs = cm_inputs.OrderByDescending(a => a.cms.cm_list.Where(b => ranking_metric_params.feature_selection_classes == null || ranking_metric_params.feature_selection_classes.Count == 0 || ranking_metric_params.feature_selection_classes.Contains(b.class_id.Value)).Average(b => b.get_perf_value_strings().Where(c => ranking_metric_params.feature_selection_metrics.Any(d => string.Equals(c.name, d, StringComparison.InvariantCultureIgnoreCase))).Average(c => c.value))).ToList();


            var ranked_scores = cm_inputs.Select((a, i) => (group_index: a.cmd_params.group_index, rank_index: i,
                group_key: a.cmd_params.group_index > -1 ? groups[a.cmd_params.group_index].key : ("_", "_", "_", "_", "_", "_", "_"),
                score: a.cms.cm_list.Where(b => ranking_metric_params.feature_selection_classes == null || ranking_metric_params.feature_selection_classes.Count == 0 || ranking_metric_params.feature_selection_classes.Contains(b.class_id.Value))
                    .Average(b => b.get_perf_value_strings().Where(c => ranking_metric_params.feature_selection_metrics.Any(d => string.Equals(c.name, d, StringComparison.InvariantCultureIgnoreCase))).Average(c => c.value)))).ToList();

            svm_ctl.WriteLine("", nameof(svm_ctl), nameof(interactive));
            ranked_scores.ForEach(a => svm_ctl.WriteLine($"Rank index: {a.rank_index}, score: {a.score}, group_index: {a.group_index}, item: {a.group_key}", nameof(svm_ctl), nameof(interactive)));
            svm_ctl.WriteLine("", nameof(svm_ctl), nameof(interactive));



            // save rank ordered confusion matrixes - all groups 
            // will this work for the svm type, svm kernel, scaling type?
            if (string.IsNullOrWhiteSpace(ranks_fn))
            {
                ranks_fn = Path.Combine(iteration_folder, $@"ranks_cm_{iteration_index}.csv");
            }

            if (!is_file_available(ranks_fn))
            {
                var cms_header = $"{string.Join(",", cmd_params.csv_header)},{string.Join(",", performance_measure.confusion_matrix.csv_header)}";
                var cms_csv = cm_inputs.SelectMany(a => a.cms.cm_list.Select(b => $"{string.Join(",", a.cmd_params.get_options().Select(c => c.value).ToList())},{b.ToString()}").ToList()).ToList();
                cms_csv.Insert(0, cms_header);

                File.WriteAllLines(ranks_fn, cms_csv);
                svm_ctl.WriteLine($"Saving: {ranks_fn}", nameof(svm_ctl), nameof(interactive));
            }

            return cm_inputs;
        }

        public static void check_solution(List<int> group_indexes, List<int> column_indexes)
        {
            // check a proposed solution

            // perform outer-cv x10 with given svm_type, kernel_type, scale_function, etc.


        }

        public static

            (List<List<(List<string> wait_file_list, cmd_params cmd_params, cmd_params merge_cmd_params, List<(bool wait_first, bool has_header, string average_out_filename, string merge_out_filename, string merge_in_filename, string average_header)> to_merge)>> jobs_randomisation_level, List<((string test_file, string test_comments_file, string prediction_file, string cm_file) filenames, (List<performance_measure.prediction> prediction_list, List<performance_measure.confusion_matrix> cm_list) cms, cmd_params cmd_params)> merge_cm_inputs, List<int> this_test_group_indexes)
            //List<List<(List<string> wait_file_list, cmd_params cmd_params, cmd_params merge_cmd_params, List<(bool wait_first, bool has_header, string average_out_filename, string merge_out_filename, string merge_in_filename, string average_header)> to_merge)>> 

            group_cv(
                int stage,
                cmd_params p,
                List<(int class_id, int size, List<(int randomisation_cv_index, int outer_cv_index, List<int> indexes)> folds)> downsampled_training_class_folds,
                List<(int class_id, int size, List<(int randomisation_cv_index, int outer_cv_index, List<int> indexes)> folds)> class_folds,
                List<(int class_id, List<(int class_id, int example_id, int class_example_id, List<(string comment_header, string comment_value)> comment_columns, string comment_columns_hash, List<(int fid, double fv)> feature_data, string feature_data_hash)> examples)> dataset_instance_list_grouped,
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
            svm_ctl.WriteLine($@"{nameof(iteration_index)}={iteration_index}, {nameof(group_index)}={group_index}", nameof(svm_ctl), nameof(group_cv));

            var jobs_randomisation_level = Enumerable.Range(0, p.randomisation_cv_folds).AsParallel().AsOrdered().Select(randomisation_cv_index =>
            {
                var jobs_outerfold_level = Enumerable.Range(0, p.outer_cv_folds).AsParallel().AsOrdered().Select(outer_cv_index =>
                {
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
                            examples: a.examples.Select(b => (example: b, columns: query_cols.Select(c => b.feature_data[c].fid == c ? b.feature_data[c] : b.feature_data.First(d => d.fid == c)).ToList(), columns_hash: hash.calc_hash(string.Join(" ", query_cols.Select(c => b.feature_data[c].fid == c ? b.feature_data[c] : b.feature_data.First(d => d.fid == c)).ToList())))).ToList())).ToList();

                    var testing_example_columns = testing_examples.Select(a => (a.class_id, examples: a.examples.Select(b => (example: b, columns: query_cols.Select(c => b.feature_data[c].fid == c ? b.feature_data[c] : b.feature_data.First(d => d.fid == c)).ToList(), columns_hash: hash.calc_hash(string.Join(" ", query_cols.Select(c => b.feature_data[c].fid == c ? b.feature_data[c] : b.feature_data.First(d => d.fid == c)).ToList())))).ToList())).ToList();

                    var training_scaling_params = training_example_columns.SelectMany(a => a.examples.SelectMany(b => b.columns).ToList()).GroupBy(a => a.fid).Select(a =>
                    {
                        var x = a.Select(b => b.fv).ToList();

                        return (fid: a.Key, list: x, non_zero: x.Count(y => y != 0), abs_sum: x.Sum(y => Math.Abs(y)), srsos: sqrt_sumofsqrs(x), average: x.Average(), stdev: SampleStandardDeviation(x), min: x.Min(), max: x.Max());
                    }).ToList();

                    var training_example_columns_scaled = training_example_columns.Select(a => (a.class_id, examples: a.examples.Select(b => (b.example, columns: b.columns.Select(c =>
                    {
                        var x = training_scaling_params.First(y => y.fid == c.fid);

                        return (fid: c.fid, fv: scale(c.fv, x.list, x.non_zero, x.abs_sum, x.srsos, x.min, x.max, x.average, x.stdev, p.scale_function));
                    }).ToList(), b.columns_hash)).ToList())).ToList();

                    var testing_example_columns_scaled = testing_example_columns.Select(a => (a.class_id, examples: a.examples.Select(b => (b.example, columns: b.columns.Select(c =>
                    {
                        var x = training_scaling_params.First(y => y.fid == c.fid);

                        return (fid: c.fid, fv: scale(c.fv, x.list, x.non_zero, x.abs_sum, x.srsos, x.min, x.max, x.average, x.stdev, p.scale_function));
                    }).ToList(), b.columns_hash)).ToList())).ToList();

                    var training_text = training_example_columns_scaled.SelectMany(a => a.examples.Select(b => $"{a.class_id} {String.Join(" ", b.columns.Where(c => c.fv != 0).OrderBy(c => c.fid).Select(c => $"{c.fid}:{c.fv}").ToList())}").ToList()).ToList();

                    var testing_text = testing_example_columns_scaled.SelectMany(a => a.examples.Select(b => $"{a.class_id} {String.Join(" ", b.columns.Where(c => c.fv != 0).OrderBy(c => c.fid).Select(c => $"{c.fid}:{c.fv}").ToList())}").ToList()).ToList();

                    if (string.IsNullOrWhiteSpace(training_id_text_header))
                        training_id_text_header = $"class_id,example_id,class_example_id,feature_data_hash,comment_columns_hash,columns_hash";

                    if (string.IsNullOrWhiteSpace(testing_id_text_header))
                        testing_id_text_header = $"class_id,example_id,class_example_id,feature_data_hash,comment_columns_hash,columns_hash";

                    if (string.IsNullOrWhiteSpace(training_meta_text_header))
                        training_meta_text_header = string.Join(",", training_example_columns.First().examples.First().example.comment_columns.Select(c => "c_" + c.comment_header).ToList());

                    if (string.IsNullOrWhiteSpace(testing_meta_text_header))
                        testing_meta_text_header = string.Join(",", testing_example_columns.First().examples.First().example.comment_columns.Select(c => "c_" + c.comment_header).ToList());

                    var training_id_text = training_example_columns.SelectMany(a => a.examples.Select(b => $"{a.class_id},{b.example.example_id},{b.example.class_example_id},{b.example.feature_data_hash},{b.example.comment_columns_hash},{b.columns_hash}").ToList()).ToList();

                    var testing_id_text = testing_example_columns.SelectMany(a => a.examples.Select(b => $"{a.class_id},{b.example.example_id},{b.example.class_example_id},{b.example.feature_data_hash},{b.example.comment_columns_hash},{b.columns_hash}").ToList()).ToList();

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


                    return ret;
                }).ToList();

                jobs_outerfold_level = jobs_outerfold_level.Where(a => a != default).ToList();

                return jobs_outerfold_level;
            }).ToList();

            jobs_randomisation_level = jobs_randomisation_level.Where(a => a != default).ToList();
            //return jobs_randomisation_level;

            // merge job inputs (training/testing/etc. files, etc.)
            var tm = jobs_randomisation_level.SelectMany(a => a.SelectMany(b => b.to_merge).ToList()).ToList();
            var pre_tm = tm.Where(a => !a.wait_first).ToList();
            var post_tm = tm.Where(a => a.wait_first).ToList();
            merge_pre_results(pre_tm);

            // submit jobs to scheduler
            var cmd_params_list = jobs_randomisation_level.SelectMany(a => a.Select(b => b.cmd_params).ToList()).ToList();


            var iteration_folder = dataset_loader.convert_path(Path.Combine(p.results_root_folder, $"itr_{iteration_index}"));

            // wait for results
            var wait_file_list = jobs_randomisation_level.SelectMany(a => a.SelectMany(b => b.wait_file_list).ToList()).ToList();
            wait_for_results(wait_file_list);


            // merge prediction results... to create a single confusion matrix
            merge_post_results(post_tm);

            var merge_cmd_params_list = jobs_randomisation_level.SelectMany(a => a.Select(b => b.merge_cmd_params).ToList()).ToList();

            var merge_cm_inputs = merge_cmd_params_list.GroupBy(a =>
                (test_file: a.test_filename,
                    test_comments_file: a.test_meta_filename,
                    prediction_file: a.test_predict_filename,
                    cm_file: Path.Combine(Path.GetDirectoryName(a.test_predict_cm_filename), $@"{Path.GetFileNameWithoutExtension(a.test_predict_cm_filename)}_merged_predictions{Path.GetExtension(a.test_predict_cm_filename)}")

                    )).Select(a => (filenames: a.Key, cms: performance_measure.load_prediction_file(a.Key.test_file, a.Key.test_comments_file, a.Key.prediction_file, p.output_threshold_adjustment_performance), cmd_params: new cmd_params(a.ToList()))).ToList();

            merge_cm_inputs.ForEach(a => update_cm(p, a.cms.cm_list));
            merge_cm_inputs.ForEach(a => update_cm(a.cmd_params, a.cms.cm_list));


            var cms_header = $"{string.Join(",", cmd_params.csv_header)},{string.Join(",", performance_measure.confusion_matrix.csv_header)}";

            for (var i = 0; i < merge_cm_inputs.Count; i++)
            {
                var item = merge_cm_inputs[i];

                var fn = item.filenames.cm_file;
                if (!is_file_available(fn))
                {

                    var cm_data = new List<string>();
                    cm_data.Add(cms_header);
                    cm_data.AddRange(item.cms.cm_list.Select(a => $"{string.Join(",", item.cmd_params.get_options().Select(c => c.value).ToList())},{a.ToString()}").ToList());


                    File.WriteAllLines(fn, cm_data);

                    svm_ctl.WriteLine("Saved merged cm: " + fn, nameof(svm_ctl), nameof(interactive));
                }
            }

            //delete_temp_files(cmd_params_list);
            //delete_temp_files(merge_cmd_params_list);
            // return results



            return (jobs_randomisation_level, merge_cm_inputs, this_test_group_indexes);
        }

        public static void delete_temp_files(cmd_params p)
        {
            //try { File.Delete(p.options_filename); } catch (Exception) { }
            //try { File.Delete(p.pbs_stderr_filename); } catch (Exception) { }
            //try { File.Delete(p.pbs_stdout_filename); } catch (Exception) { }
            try { File.Delete(p.test_filename); } catch (Exception) { }
            try { File.Delete(p.test_id_filename); } catch (Exception) { }
            try { File.Delete(p.test_meta_filename); } catch (Exception) { }
            //try { File.Delete(p.test_predict_cm_filename); } catch (Exception) { }
            try { File.Delete(p.test_predict_filename); } catch (Exception) { }
            try { File.Delete(p.train_filename); } catch (Exception) { }
            try { File.Delete(p.train_grid_filename); } catch (Exception) { }
            try { File.Delete(p.train_id_filename); } catch (Exception) { }
            try { File.Delete(p.train_meta_filename); } catch (Exception) { }
            try { File.Delete(p.train_model_filename); } catch (Exception) { }
            //try { File.Delete(p.train_predict_cm_filename); } catch (Exception) { }
            try { File.Delete(p.train_predict_filename); } catch (Exception) { }
        }

        public static void delete_temp_files(List<cmd_params> cmd_params_list)
        {
            foreach (var p in cmd_params_list)
            {
                delete_temp_files(p);
            }
        }

        private static void merge_post_results(List<(bool wait_first, bool has_header, string average_out_filename, string merge_out_filename, string merge_in_filename, string average_header)> to_merge)
        {
            foreach (var t in to_merge.Where(a => a.wait_first).GroupBy(a => (a.merge_out_filename, a.average_out_filename)))
            {
                var key = t.Key;
                var fn = key.merge_out_filename;

                if (is_file_available(fn)) continue;

                var list = t.ToList();

                var read = list.SelectMany((a, i) => File.ReadAllLines(a.merge_in_filename).Skip((i == 0 || !a.has_header) ? 0 : 1).ToList()).ToList();

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
                        is_numeric[i] = read_split_data.Select(a => a[i]).All(a => double.TryParse(a, out var r));
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
                                av_values[i] = values.Average(a => double.Parse(a)).ToString();
                                min_values[i] = values.Min(a => double.Parse(a)).ToString();
                                max_values[i] = values.Max(a => double.Parse(a)).ToString();
                                sum_values[i] = values.Sum(a => double.Parse(a)).ToString();
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
                    //File.WriteAllLines(fn2, lines_data);
                    //svm_ctl.WriteLine("Saved: " + fn2);
                    read.AddRange(lines_data);
                }


                File.WriteAllLines(fn, read);
                svm_ctl.WriteLine("Saved merged results: " + fn, nameof(svm_ctl), nameof(merge_post_results));
            }
        }

        private static void merge_pre_results(List<(bool wait_first, bool has_header, string average_out_filename, string merge_out_filename, string merge_in_filename, string average_header)> to_merge)
        {
            foreach (var t in to_merge.Where(a => !a.wait_first).GroupBy(a => a.merge_out_filename))
            {
                var key = t.Key;
                var fn = key;

                if (is_file_available(fn)) continue;

                var list = t.ToList();

                var read = list.SelectMany((a, i) => File.ReadAllLines(a.merge_in_filename).Skip(i == 0 || !a.has_header ? 0 : 1).ToList()).ToList();


                File.WriteAllLines(fn, read);
                svm_ctl.WriteLine("Saved merged inputs: " + fn, nameof(svm_ctl), nameof(merge_pre_results));
            }
        }

        public static (

            //List<string> cmds,
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
            svm_ctl.WriteLine($@"Memory usage: {(GC.GetTotalMemory(false) / 1_000_000_000d):F2} GB", nameof(svm_ctl), nameof(do_job));

            //var cmds = new List<string>();
            var wait_file_list = new List<string>();

            var merge_param_list = new List<cmd_params>();
            var to_merge = new List<(bool wait_first, bool has_header, string average_out_filename, string merge_out_filename, string merge_in_filename, string average_header)>();
            // each job is 1 outer-cv fold ... 1 grid search ... 1 prediction ... 1 fold cm  

            var group_str = string.Join(".", new string[]
            {
                        job.group_key.alphabet, job.group_key.dimension, job.group_key.category,
                        job.group_key.source, job.group_key.group, job.group_key.member, job.group_key.perspective,
            }.Select(a => string.IsNullOrWhiteSpace(a) ? "_" : a).ToArray());

            var iteration_folder = dataset_loader.convert_path(Path.Combine(p.results_root_folder, $"itr_{job.iteration_index}"));

            var group_folder = Path.Combine(p.results_root_folder, $"itr_{job.iteration_index}", $"grp_{group_str}", $"svm_{p.svm_type}_krnl_{p.svm_kernel}_scale_{p.scale_function}");
            var outer_fold_folder = Path.Combine(group_folder, $"rnd_{job.randomisation_cv_index}_ocvfld_{job.outer_cv_index}");
            var job_output_folder = outer_fold_folder;
            var merge_output_folder = group_folder;

            Directory.CreateDirectory(iteration_folder);
            Directory.CreateDirectory(group_folder);
            Directory.CreateDirectory(outer_fold_folder);

            var filename = $"itr_{job.iteration_index}_grp_{job.group_index}_rnd_{job.randomisation_cv_index}_ocvfld_{job.outer_cv_index}_svm_{p.svm_type}_krnl_{p.svm_kernel}_scale_{p.scale_function}";//"_job_{job.job_id}";

            var pbs_jobname = $@"{nameof(svm_wkr)}_{job.job_id}";
            var pbs_walltime = new TimeSpan(0, 30, 0);

            var cmd_params = new cmd_params(p);
            //{
            cmd_params.cmd = cmd.wkr;
            cmd_params.pbs_jobname = pbs_jobname;
            cmd_params.pbs_walltime = $@"{pbs_walltime:hh\:mm\:ss}";
            cmd_params.pbs_stdout_filename = $"{filename}.stdout.txt";
            cmd_params.pbs_stderr_filename = $"{filename}.stderr.txt";
            //cmd_params.pbs_nodes = 1;
            //cmd_params.pbs_ppn = 4;
            //cmd_params.pbs_mem = $@"{(1024 * 8)}mb";
            cmd_params.job_id = job.job_id;
            cmd_params.feature_id = group.columns != null && group.columns.Count == 1 && group.list != null && group.list.Count > 0 ? group.list.First().fid : -1;
            cmd_params.member = group.columns != null && group.columns.Count == 1 && group.list != null && group.list.Count > 0 ? group.list.First().member : "";
            cmd_params.perspective = group.columns != null && group.columns.Count == 1 && group.list != null && group.list.Count > 0 ? group.list.First().perspective : "";
            cmd_params.forward = job.forward;
            cmd_params.randomisation_cv_index = job.randomisation_cv_index;
            cmd_params.iteration = job.iteration_index;
            cmd_params.outer_cv_index = job.outer_cv_index;
            cmd_params.group_index = job.group_index;
            cmd_params.old_feature_count = job.old_feature_count;
            cmd_params.new_feature_count = job.new_feature_count;
            cmd_params.old_group_count = job.old_group_count;
            cmd_params.new_group_count = job.new_group_count;
            cmd_params.class_training_sizes = job.training_sizes;
            cmd_params.class_testing_sizes = job.testing_sizes;
            cmd_params.alphabet = job.group_key.alphabet;
            cmd_params.category = job.group_key.category;
            cmd_params.dimension = job.group_key.dimension;
            cmd_params.source = job.group_key.source;
            cmd_params.group = job.group_key.group;
            cmd_params.options_filename = Path.Combine(job_output_folder, $"{filename}.options");
            cmd_params.train_filename = Path.Combine(job_output_folder, $"{filename}.train.libsvm");
            cmd_params.train_id_filename = Path.Combine(job_output_folder, $"{filename}.train_id.csv");
            cmd_params.train_meta_filename = Path.Combine(job_output_folder, $"{filename}.train_meta.csv");
            cmd_params.train_grid_filename = Path.Combine(job_output_folder, $"{filename}.train_grid.csv");
            cmd_params.train_model_filename = Path.Combine(job_output_folder, $"{filename}.train_model.libsvm");
            cmd_params.test_filename = Path.Combine(job_output_folder, $"{filename}.test.libsvm");
            cmd_params.test_id_filename = Path.Combine(job_output_folder, $"{filename}.test_id.csv");
            cmd_params.test_meta_filename = Path.Combine(job_output_folder, $"{filename}.test_meta.csv");
            cmd_params.test_predict_cm_filename = Path.Combine(job_output_folder, $"{filename}.test_predict_cm.csv");
            cmd_params.test_predict_filename = Path.Combine(job_output_folder, $"{filename}.test_predict.libsvm");
            //};

            cmd_params.convert_paths();

            var options_text = cmd_params.get_options_ini_text();

            var cached = true;

            if (!is_file_available(cmd_params.options_filename))
            {
                File.WriteAllLines(cmd_params.options_filename, options_text);
                svm_ctl.WriteLine("Saved options: " + cmd_params.options_filename, nameof(svm_ctl), nameof(do_job));
                cached = false;
            }
            else
            {
                svm_ctl.WriteLine("Using cached options: " + cmd_params.options_filename, nameof(svm_ctl), nameof(do_job));
            }

            if (!is_file_available(cmd_params.train_filename))
            {
                File.WriteAllLines(cmd_params.train_filename, job.training_text);
                svm_ctl.WriteLine("Saved training data: " + cmd_params.train_filename, nameof(svm_ctl), nameof(do_job));
                cached = false;
            }
            else
            {
                svm_ctl.WriteLine("Using cached training data: " + cmd_params.train_filename, nameof(svm_ctl), nameof(do_job));


            }

            if (!is_file_available(cmd_params.train_id_filename))
            {
                File.WriteAllLines(cmd_params.train_id_filename, job.training_id_text);
                svm_ctl.WriteLine("Saved training ids: " + cmd_params.train_id_filename, nameof(svm_ctl), nameof(do_job));
                cached = false;
            }
            else
            {
                svm_ctl.WriteLine("Using cached training ids: " + cmd_params.train_id_filename, nameof(svm_ctl), nameof(do_job));


            }

            if (!is_file_available(cmd_params.train_meta_filename))
            {
                File.WriteAllLines(cmd_params.train_meta_filename, job.training_meta_text);
                svm_ctl.WriteLine("Saved training meta: " + cmd_params.train_meta_filename, nameof(svm_ctl), nameof(do_job));
                cached = false;
            }
            else
            {
                svm_ctl.WriteLine("Using cached training meta: " + cmd_params.train_meta_filename, nameof(svm_ctl), nameof(do_job));

            }

            if (!is_file_available(cmd_params.test_filename))
            {
                File.WriteAllLines(cmd_params.test_filename, job.testing_text);
                svm_ctl.WriteLine("Saved testing data: " + cmd_params.test_filename, nameof(svm_ctl), nameof(do_job));
                cached = false;
            }
            else
            {
                svm_ctl.WriteLine("Using cached testing data: " + cmd_params.test_filename, nameof(svm_ctl), nameof(do_job));

            }

            if (!is_file_available(cmd_params.test_id_filename))
            {
                File.WriteAllLines(cmd_params.test_id_filename, job.testing_id_text);
                svm_ctl.WriteLine("Saved testing ids: " + cmd_params.test_id_filename, nameof(svm_ctl), nameof(do_job));
                cached = false;
            }
            else
            {
                svm_ctl.WriteLine("Using cached testing ids: " + cmd_params.test_id_filename, nameof(svm_ctl), nameof(do_job));

            }

            if (!is_file_available(cmd_params.test_meta_filename))
            {
                File.WriteAllLines(cmd_params.test_meta_filename, job.testing_meta_text);
                svm_ctl.WriteLine("Saved testing meta: " + cmd_params.test_meta_filename, nameof(svm_ctl), nameof(do_job));
                cached = false;
            }
            else
            {
                svm_ctl.WriteLine("Using cached testing meta: " + cmd_params.test_meta_filename, nameof(svm_ctl), nameof(do_job));


            }
            //cmds.Add($@"start cmd /c {cmd_params.program_runtime} -j {cmd_params.options_filename}");

            wait_file_list.Add(cmd_params.train_grid_filename);
            wait_file_list.Add(cmd_params.train_model_filename);
            wait_file_list.Add(cmd_params.test_predict_filename);
            wait_file_list.Add(cmd_params.test_predict_cm_filename);

            if (cached)
            {
                foreach (var f in wait_file_list)
                {
                    if (!is_file_available(f))
                    {
                        cached = false;
                        break;
                    }
                }
            }

            //var merge_filename = $"itr_{job.iteration_index}_grp_{job.group_index}_rnd_{job.randomisation_cv_index}_ocvfld_{job.outer_cv_index}_svm_{p.svm_type}_krnl_{p.svm_kernel}_scale_{p.scale_function}";//"_job_{job.job_id}";

            var merge_filename = $"itr_{job.iteration_index}_grp_{job.group_index}_svm_{p.svm_type}_krnl_{p.svm_kernel}_scale_{p.scale_function}"; // note: randomsiations are included as part of the outer-cv, not separate results ... if wanted seperate use: _{job.randomisation}"; // note: no job id

            var merge_cmd_params = new cmd_params(cmd_params)
            {
                pbs_stdout_filename = Path.Combine(merge_output_folder, $"{merge_filename}.stdout.txt"),
                pbs_stderr_filename = Path.Combine(merge_output_folder, $"{merge_filename}.stderr.txt"),

                train_filename = Path.Combine(merge_output_folder, $"{merge_filename}.train.libsvm"),
                train_id_filename = Path.Combine(merge_output_folder, $"{merge_filename}.train_id.csv"),
                train_meta_filename = Path.Combine(merge_output_folder, $"{merge_filename}.train_meta.csv"),
                train_grid_filename = Path.Combine(merge_output_folder, $"{merge_filename}.train_grid.csv"),
                train_model_filename = Path.Combine(merge_output_folder, $"{merge_filename}.train_model.libsvm"),
                test_filename = Path.Combine(merge_output_folder, $"{merge_filename}.test.libsvm"),
                test_id_filename = Path.Combine(merge_output_folder, $"{merge_filename}.test_id.csv"),
                test_meta_filename = Path.Combine(merge_output_folder, $"{merge_filename}.test_meta.csv"),
                test_predict_cm_filename = Path.Combine(merge_output_folder, $"{merge_filename}.test_predict_cm.csv"),
                test_predict_filename = Path.Combine(merge_output_folder, $"{merge_filename}.test_predict.libsvm"),
            };

            merge_cmd_params.convert_paths();

            to_merge.Add((true, true, null, merge_cmd_params.train_grid_filename, cmd_params.train_grid_filename, null));
            to_merge.Add((false, false, null, merge_cmd_params.test_filename, cmd_params.test_filename, null));
            to_merge.Add((false, true, null, merge_cmd_params.test_id_filename, cmd_params.test_id_filename, null));
            to_merge.Add((false, true, null, merge_cmd_params.test_meta_filename, cmd_params.test_meta_filename, null));
            to_merge.Add((true, p.libsvm_train_probability_estimates, null, merge_cmd_params.test_predict_filename, cmd_params.test_predict_filename, null));
            to_merge.Add((true, true, null, merge_cmd_params.test_predict_cm_filename, cmd_params.test_predict_cm_filename, "class_id"));

            if (!cached)
            {
                submit_pbs_job(cmd_params);
            }

            var ret = (
                       //cmds,
                       wait_file_list,
                       cmd_params,
                       merge_cmd_params,
                       to_merge
                       );

            return ret;
        }

        public static void submit_pbs_job(cmd_params cmd_params) //(string jobs_fn)
        {
            var source = dataset_loader.convert_path(cmd_params.options_filename);
            var dest = dataset_loader.convert_path(Path.Combine(Path.GetDirectoryName(cmd_params.pbs_submission_directory), Path.GetFileName(cmd_params.options_filename)));

            if (source != dest)
            {
                File.Copy(source, dest);
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
        //            File.WriteAllLines(cm_output_file, cm_lines);
        //        }

        public static bool is_file_available(string filename)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(filename)) return false;

                if (!File.Exists(filename)) return false;

                if (new FileInfo(filename).Length <= 0) return false;

                using (var fs = File.Open(filename, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite))
                {

                }

                return true;
            }
            catch (IOException)
            {
                return false;
            }
            catch (Exception)
            {
                return false;
            }
        }

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

        public static void wait_for_results(List<string> file_wait_list)
        {
            svm_ctl.WriteLine($@"Waiting for {file_wait_list.Count} files.", nameof(svm_ctl), nameof(wait_for_results));

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

                var new_files_found = file_wait_list.Where(a => is_file_available(a)).ToList();

                if (new_files_found.Count > 0)
                {
                    itr = 0;

                    svm_ctl.WriteLine("New files found: ", nameof(svm_ctl), nameof(wait_for_results));
                    for (var j = 0; j < new_files_found.Count; j++)
                    {
                        svm_ctl.WriteLine($@"({j}): {new_files_found[j]}", nameof(svm_ctl), nameof(wait_for_results));
                    }



                    files_found.AddRange(new_files_found.Select((a, i) => (a, DateTime.Now)).ToList());

                    file_wait_list = file_wait_list.Except(new_files_found).ToList();
                    total_found += new_files_found.Count;

                    var pct = ((double)total_found / (double)total_files) * (double)100;

                    svm_ctl.WriteLine($@"Files ready: {total_found} / {total_files} ( {pct:0.00}% ) [ Time: {sw1.Elapsed.ToString()} ] [ ETA: {calc_eta(sw1, total_found, total_files).ToString()} ]", nameof(svm_ctl), nameof(wait_for_results));
                }
                else if (itr >= 6)
                {
                    itr = 0;

                    var pct = ((double)total_found / (double)total_files) * (double)100;

                    svm_ctl.WriteLine($@"Files ready: {total_found} / {total_files} ( {pct:0.00}% ) [ Time: {sw1.Elapsed.ToString()} ] [ ETA: {calc_eta(sw1, total_found, total_files).ToString()} ]", nameof(svm_ctl), nameof(wait_for_results));

                }

                if (file_wait_list.Count == 0)
                {
                    return;
                }

                Task.Delay(new TimeSpan(0, 0, 0, 10)).Wait();
            }
        }
    }
}