using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Diagnostics;
using System.Globalization;

namespace svm_fs
{
    internal static class svm_wkr
    {
        internal static void cross_validation(cmd_params p)
        {
            var use_cache = true;
            var cached = true;

            if (!use_cache || !io_proxy.is_file_available(p.test_predict_cm_filename, nameof(svm_wkr), nameof(cross_validation))) { cached = false; }
            //if (!use_cache || !io_proxy.is_file_available(p.test_predict_filename, nameof(svm_wkr), nameof(cross_validation))) { cached = false; }
            //if (!use_cache || !io_proxy.is_file_available(p.test_filename, nameof(svm_wkr), nameof(cross_validation))) { cached = false; }
            //if (!use_cache || !io_proxy.is_file_available(p.train_model_filename, nameof(svm_wkr), nameof(cross_validation))) { cached = false; }
            //if (!use_cache || (p.save_test_meta && !io_proxy.is_file_available(p.test_meta_filename, nameof(svm_wkr), nameof(cross_validation))) { cached = false; }
            //if (!use_cache || (p.inner_cv_folds > 1 && !io_proxy.is_file_available(p.train_grid_filename, nameof(svm_wkr), nameof(cross_validation))) { cached = false; }

            if (use_cache && cached)
            {
                io_proxy.WriteLine($@"{nameof(svm_wkr)} Cache found. Exiting.");
                //delete_temp_wkr_files(p);
                return;
            }


            var train_stdout_filename = "";
            var train_stderr_filename = "";

            var predict_stdout_filename = "";
            var predict_stderr_filename = "";

            //grid.best_rate_container train_grid_search_result = null;
            //grid.best_rate_container test_svm_params = null;

            ((double? cost, double? gamma, double? epsilon, double? coef0, double? degree) point, double? cv_rate) train_grid_search_result = ((null, null, null, null, null), null);


            //TimeSpan? point_max_time = new TimeSpan(0, 0, 10, 0);

            var sw_grid = new Stopwatch();

            // perform inner-cv
            if (p.inner_cv_folds > 1)
            {
                sw_grid.Start();
                if (!string.IsNullOrWhiteSpace(p.train_grid_filename))
                {
                    var train_grid_stdout_file = "";
                    var train_grid_stderr_file = "";

                    train_grid_search_result = grid.grid_parameter_search(
                        p.libsvm_train_runtime,
                        p.train_grid_filename,
                        p.train_filename,
                        train_grid_stdout_file,
                        train_grid_stderr_file,
                        p.class_weights,
                        p.svm_type,
                        p.svm_kernel,
                        p.randomisation_cv_folds,
                        p.randomisation_cv_index,
                        p.outer_cv_folds,
                        p.outer_cv_index,
                        p.inner_cv_folds,
                        p.libsvm_grid_probability_estimates,
                        p.libsvm_grid_shrinking_heuristics,
                        p.libsvm_grid_quiet_mode,
                        p.libsvm_grid_memory_limit_mb,
                        p.libsvm_grid_max_time,
                        p.grid_cost_exp_begin, p.grid_cost_exp_end, p.grid_cost_exp_step,
                        p.grid_gamma_exp_begin, p.grid_gamma_exp_end, p.grid_gamma_exp_step,
                        p.grid_epsilon_exp_begin, p.grid_epsilon_exp_end, p.grid_epsilon_exp_step,
                        p.grid_coef0_exp_begin, p.grid_coef0_exp_end, p.grid_coef0_exp_step,
                        p.grid_degree_exp_begin, p.grid_degree_exp_end, p.grid_degree_exp_step
                        );
                }
                sw_grid.Stop();

            }
            var sw_grid_dur = sw_grid.ElapsedMilliseconds;

            // train
            var sw_train = new Stopwatch();
            sw_train.Start();
            var train_result = libsvm.train(
                p.libsvm_train_runtime,
                p.train_filename,
                p.train_model_filename,
                train_stdout_filename,
                train_stderr_filename,
                train_grid_search_result.point.cost,
                train_grid_search_result.point.gamma,
                train_grid_search_result.point.epsilon,
                train_grid_search_result.point.coef0,
                train_grid_search_result.point.degree,
                null,
                p.svm_type,
                p.svm_kernel,
                null,
                p.libsvm_train_probability_estimates,
                p.libsvm_train_shrinking_heuristics,
                p.libsvm_train_max_time,
                p.libsvm_train_quiet_mode,
                p.libsvm_train_memory_limit_mb
                );
            sw_train.Stop();
            var sw_train_dur = sw_train.ElapsedMilliseconds;

            if (!string.IsNullOrWhiteSpace(train_result.cmd_line)) io_proxy.WriteLine(train_result.cmd_line, nameof(svm_wkr), nameof(cross_validation));
            if (!string.IsNullOrWhiteSpace(train_result.stdout)) train_result.stdout.Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries).ToList().ForEach(a => io_proxy.WriteLine($@"{nameof(train_result)}.{nameof(train_result.stdout)}: {a}", nameof(svm_wkr), nameof(cross_validation)));
            if (!string.IsNullOrWhiteSpace(train_result.stderr)) train_result.stderr.Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries).ToList().ForEach(a => io_proxy.WriteLine($@"{nameof(train_result)}.{nameof(train_result.stderr)}: {a}", nameof(svm_wkr), nameof(cross_validation)));


            // predict
            var sw_predict = new Stopwatch();
            sw_predict.Start();
            var predict_result = libsvm.predict(
                p.libsvm_predict_runtime,
                p.test_filename,
                p.train_model_filename,
                p.test_predict_filename,
                p.libsvm_train_probability_estimates,
                predict_stdout_filename,
                predict_stderr_filename
                );

            sw_predict.Stop();
            var sw_predict_dur = sw_train.ElapsedMilliseconds;

            if (!string.IsNullOrWhiteSpace(predict_result.cmd_line)) io_proxy.WriteLine(predict_result.cmd_line, nameof(svm_wkr), nameof(cross_validation));
            if (!string.IsNullOrWhiteSpace(predict_result.stdout)) predict_result.stdout.Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries).ToList().ForEach(a => io_proxy.WriteLine($@"{nameof(predict_result)}.{nameof(predict_result.stdout)}: {a}", nameof(svm_wkr), nameof(cross_validation)));
            if (!string.IsNullOrWhiteSpace(predict_result.stderr)) predict_result.stderr.Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries).ToList().ForEach(a => io_proxy.WriteLine($@"{nameof(predict_result)}.{nameof(predict_result.stderr)}: {a}", nameof(svm_wkr), nameof(cross_validation)));


            var prediction_file_data = performance_measure.load_prediction_file(p.test_filename, p.save_test_meta ? p.test_meta_filename : null, p.test_predict_filename, p.output_threshold_adjustment_performance);


            svm_ctl.update_cm(p, prediction_file_data.cm_list);

            for (var i = 0; i < prediction_file_data.cm_list.Count; i++)
            {
                var cm = prediction_file_data.cm_list[i];

                cm.ext_cost = train_grid_search_result.point.cost;
                cm.ext_gamma = train_grid_search_result.point.gamma;
                cm.ext_coef0 = train_grid_search_result.point.coef0;
                cm.ext_epsilon = train_grid_search_result.point.epsilon;
                cm.ext_degree = train_grid_search_result.point.degree;
                cm.ext_libsvm_cv = train_grid_search_result.cv_rate ?? -1;

                cm.ext_duration_grid_search = sw_grid_dur.ToString(CultureInfo.InvariantCulture);
                cm.ext_duration_training = sw_train_dur.ToString(CultureInfo.InvariantCulture);
                cm.ext_duration_testing = sw_predict_dur.ToString(CultureInfo.InvariantCulture);

                cm.calculate_ppf();
                
                //join outer-cv-cms together to create 1 cm - roc - pr - etc
            }

            var cm_lines = new List<string>();
            var cms_header = $"{string.Join(",", cmd_params.csv_header)},{string.Join(",", performance_measure.confusion_matrix.csv_header)}";
            cm_lines.Add(cms_header);
            //var cms_csv = cms.SelectMany(a => a.pm.cm_list.Select(b => string.Join(",", p.get_options().Select(c=>c.value).ToList()) + "," + b.ToString()).ToList()).ToList();
            for (var i = 0; i < prediction_file_data.cm_list.Count; i++)
            {
                var cm = prediction_file_data.cm_list[i];
                cm_lines.Add($"{string.Join(",", p.get_options().Select(c => c.value).ToList())},{cm}");
            }
            io_proxy.WriteAllLines(p.test_predict_cm_filename, cm_lines);

            //delete_temp_wkr_files(p);
            // report finished
            // rename output files
        }

        /*
        internal static void delete_temp_wkr_files(cmd_params p, bool delete_logs = false)
        {
            if (delete_logs)
            {
                var pbs_wkr_stderr_filename = Path.Combine(p.pbs_wkr_execution_directory, Path.GetFileName(p.pbs_wkr_stderr_filename));
                if (io_proxy.is_file_empty(pbs_wkr_stderr_filename)) io_proxy.Delete(pbs_wkr_stderr_filename, nameof(svm_wkr), nameof(delete_temp_wkr_files));
                io_proxy.Delete(Path.Combine(p.pbs_wkr_execution_directory, Path.GetFileName(p.pbs_wkr_stdout_filename)), nameof(svm_wkr), nameof(delete_temp_wkr_files));

                var program_wkr_stderr_filename = Path.Combine(p.pbs_wkr_execution_directory, Path.GetFileName(p.program_wkr_stderr_filename));
                if (io_proxy.is_file_empty(program_wkr_stderr_filename)) io_proxy.Delete(program_wkr_stderr_filename, nameof(svm_wkr), nameof(delete_temp_wkr_files));
                io_proxy.Delete(Path.Combine(p.pbs_wkr_execution_directory, Path.GetFileName(p.program_wkr_stdout_filename)), nameof(svm_wkr), nameof(delete_temp_wkr_files));

                //try { io_proxy.Delete(p.program_wkr_stderr_filename, nameof(svm_ctl), nameof(delete_temp_wkr_files));
                //try { io_proxy.Delete(p.program_wkr_stdout_filename, nameof(svm_ctl), nameof(delete_temp_wkr_files));
            }

            io_proxy.Delete(p.options_filename, nameof(svm_wkr), nameof(delete_temp_wkr_files));
            io_proxy.Delete(p.train_filename, nameof(svm_wkr), nameof(delete_temp_wkr_files)); 
            io_proxy.Delete(p.train_model_filename, nameof(svm_wkr), nameof(delete_temp_wkr_files));
            //io_proxy.Delete(p.test_filename, nameof(svm_wkr), nameof(delete_temp_wkr_files)); 
            //io_proxy.Delete(p.test_labels_filename, nameof(svm_wkr), nameof(delete_temp_wkr_files)); 

            if (p.save_test_id) io_proxy.Delete(p.test_id_filename, nameof(svm_wkr), nameof(delete_temp_wkr_files)); 
            if (p.save_test_meta) io_proxy.Delete(p.test_meta_filename, nameof(svm_wkr), nameof(delete_temp_wkr_files)); 
            if (p.save_train_id) io_proxy.Delete(p.train_id_filename, nameof(svm_wkr), nameof(delete_temp_wkr_files)); 
            if (p.save_train_meta) io_proxy.Delete(p.train_meta_filename, nameof(svm_wkr), nameof(delete_temp_wkr_files));

            //io_proxy.Delete(p.train_predict_cm_filename, nameof(svm_wkr), nameof(delete_temp_wkr_files));
            //io_proxy.Delete(p.train_predict_filename, nameof(svm_wkr), nameof(delete_temp_wkr_files));
            //io_proxy.Delete(p.train_grid_filename, nameof(svm_wkr), nameof(delete_temp_wkr_files));
            //io_proxy.Delete(p.test_predict_cm_filename, nameof(svm_wkr), nameof(delete_temp_wkr_files));
            //io_proxy.Delete(p.test_predict_filename, nameof(svm_wkr), nameof(delete_temp_wkr_files)); 
        }*/

    }
}
