using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;

namespace svm_fs
{
    internal enum cmd
    {
        none,
        ctl,
        wkr
    }

    internal class cmd_params
    {
        //internal bool probability_estimates = true;
        //internal bool shrinking_heuristics = true;
        //internal List<(int class_id, string class_name)> class_training_sizes = null;
        //internal string pbs_jobid_filename;
        internal bool forward;
        internal bool group_features = true;
        internal bool libsvm_grid_probability_estimates = true;
        internal bool libsvm_grid_quiet_mode = true;
        internal bool libsvm_grid_shrinking_heuristics = true;
        internal bool libsvm_train_probability_estimates = true;
        internal bool libsvm_train_quiet_mode = true;
        internal bool libsvm_train_shrinking_heuristics = true;
        internal bool output_threshold_adjustment_performance = false;
        internal bool save_test_id = false;
        internal bool save_test_meta = false;
        internal bool save_train_id = false;
        internal bool save_train_meta = false;
        internal cmd cmd = cmd.none;
        internal common.libsvm_kernel_type svm_kernel = common.libsvm_kernel_type.rbf;
        internal common.libsvm_svm_type svm_type = common.libsvm_svm_type.c_svc;
        internal common.scale_function scale_function = common.scale_function.rescale;
        internal const string svm_fs_home = "/home/k1040015/svm_fs/svm_fs";
        internal const string user_home = "/home/k1040015";
        internal double? grid_coef0_exp_begin = null;
        internal double? grid_coef0_exp_end = null;
        internal double? grid_coef0_exp_step = null;
        internal double? grid_cost_exp_begin = -5;
        internal double? grid_cost_exp_end = 15;
        internal double? grid_cost_exp_step = 2;
        internal double? grid_degree_exp_begin = null;
        internal double? grid_degree_exp_end = null;
        internal double? grid_degree_exp_step = null;
        internal double? grid_epsilon_exp_begin = null;
        internal double? grid_epsilon_exp_end = null;
        internal double? grid_epsilon_exp_step = null;
        internal double? grid_gamma_exp_begin = 3;
        internal double? grid_gamma_exp_end = -15;
        internal double? grid_gamma_exp_step = -2;
        internal int feature_id = -1;
        internal int group_index = -1;
        internal int inner_cv_folds = 0;//10; // set to 0 to skip inner-cv
        internal int iteration = -1;
        internal int job_id = -1;
        internal int libsvm_grid_memory_limit_mb = 1024;
        internal int libsvm_train_memory_limit_mb = 1024;
        internal int negative_class_id = -1;
        internal int new_feature_count = 0;
        internal int new_group_count = 0;
        internal int old_feature_count = 0;
        internal int old_group_count = 0;
        internal int outer_cv_folds = 10;
        internal int outer_cv_index = -1;
        internal int pbs_ctl_nodes = 1;
        internal int pbs_ctl_ppn = 64;
        internal int pbs_wkr_nodes = 1;
        internal int pbs_wkr_ppn = 16;
        internal int positive_class_id = +1;
        internal int randomisation_cv_folds = 1;
        internal int randomisation_cv_index = -1;
        internal List<(int class_id, double class_weight)> class_weights = null;
        internal List<(int class_id, int class_size)> class_sizes = null;
        internal List<(int class_id, int class_testing_size)> class_testing_sizes = null;
        internal List<(int class_id, int class_training_size)> class_training_sizes = null;
        internal List<(int class_id, string class_name)> class_names = new List<(int class_id, string class_name)>() { (-1, "standard_coil"), (+1, "dimorphic_coil") };
        internal List<int> feature_selection_classes = new List<int>() { +1 };
        internal List<string> feature_selection_metrics = new List<string>() { nameof(performance_measure.confusion_matrix.F1S) };
        internal string alphabet;
        internal string category;
        internal string dataset_dir = $@"{user_home}/dataset/";
        internal string dimension;
        internal string experiment_name = null;
        internal string group;
        internal string libsvm_predict_runtime = $@"{user_home}/libsvm/svm-predict";
        internal string libsvm_train_runtime = $@"{user_home}/libsvm/svm-train";
        internal string member;
        internal string options_filename = null;
        internal string pbs_ctl_execution_directory = $@"{svm_fs_home}/pbs_ctl_sub/";
        internal string pbs_ctl_jobname;
        internal string pbs_ctl_mail_addr; // 
        internal string pbs_ctl_mail_opt = "n"; // abe|n
        internal string pbs_ctl_mem = "192GB";
        internal string pbs_ctl_stderr_filename = $@"{nameof(svm_ctl)}.pbs.stderr";
        internal string pbs_ctl_stdout_filename = $@"{nameof(svm_ctl)}.pbs.stdout";
        internal string pbs_ctl_submission_directory = $@"{svm_fs_home}/pbs_ctl_sub/";
        internal string pbs_ctl_walltime = "240:00:00";
        internal string pbs_wkr_execution_directory = $@"{svm_fs_home}/pbs_wkr_sub/";
        internal string pbs_wkr_jobname;
        internal string pbs_wkr_mail_addr; // 
        internal string pbs_wkr_mail_opt = "n"; // abe|n
        internal string pbs_wkr_mem = null;
        internal string pbs_wkr_stderr_filename = $@"{nameof(svm_wkr)}.pbs.stderr";
        internal string pbs_wkr_stdout_filename = $@"{nameof(svm_wkr)}.pbs.stdout";
        internal string pbs_wkr_submission_directory = $@"{svm_fs_home}/pbs_wkr_sub/";
        internal string pbs_wkr_walltime = "1:00:00";
        internal string perspective;
        internal string program_ctl_stderr_filename = $@"{nameof(svm_ctl)}.{nameof(svm_fs)}.stderr";
        internal string program_ctl_stdout_filename = $@"{nameof(svm_ctl)}.{nameof(svm_fs)}.stdout";
        internal string program_runtime = System.Diagnostics.Process.GetCurrentProcess().MainModule.FileName;
        internal string program_wkr_stderr_filename = $@"{nameof(svm_wkr)}.{nameof(svm_fs)}.stderr";
        internal string program_wkr_stdout_filename = $@"{nameof(svm_wkr)}.{nameof(svm_fs)}.stdout";
        internal string results_root_folder = $@"{svm_fs_home}/results/";
        internal string source;
        internal string test_filename;
        internal string test_id_filename;
        internal string test_labels_filename;
        internal string test_meta_filename;
        internal string test_predict_cm_filename;
        internal string test_predict_filename;
        internal string train_filename;
        internal string train_grid_filename;
        internal string train_id_filename;
        internal string train_meta_filename;
        internal string train_model_filename;
        internal string train_predict_cm_filename;
        internal string train_predict_filename;
        internal TimeSpan libsvm_grid_max_time = new TimeSpan(0, 0, 10, 0);
        internal TimeSpan libsvm_train_max_time = new TimeSpan(0, 1, 0, 0);

        internal void convert_paths()
        {
            this.options_filename = io_proxy.convert_path(this.options_filename);
            this.pbs_ctl_stderr_filename = io_proxy.convert_path(this.pbs_ctl_stderr_filename);
            this.pbs_ctl_stdout_filename = io_proxy.convert_path(this.pbs_ctl_stdout_filename);
            this.pbs_wkr_stderr_filename = io_proxy.convert_path(this.pbs_wkr_stderr_filename);
            this.pbs_wkr_stdout_filename = io_proxy.convert_path(this.pbs_wkr_stdout_filename);

            this.program_ctl_stderr_filename = io_proxy.convert_path(this.program_ctl_stderr_filename);
            this.program_ctl_stdout_filename = io_proxy.convert_path(this.program_ctl_stdout_filename);
            this.program_wkr_stderr_filename = io_proxy.convert_path(this.program_wkr_stderr_filename);
            this.program_wkr_stdout_filename = io_proxy.convert_path(this.program_wkr_stdout_filename);

            this.test_filename = io_proxy.convert_path(this.test_filename);
            this.test_labels_filename = io_proxy.convert_path(this.test_labels_filename);
            this.test_id_filename = io_proxy.convert_path(this.test_id_filename);
            this.test_meta_filename = io_proxy.convert_path(this.test_meta_filename);
            this.test_predict_cm_filename = io_proxy.convert_path(this.test_predict_cm_filename);
            this.test_predict_filename = io_proxy.convert_path(this.test_predict_filename);
            this.train_filename = io_proxy.convert_path(this.train_filename);
            this.train_grid_filename = io_proxy.convert_path(this.train_grid_filename);
            this.train_id_filename = io_proxy.convert_path(this.train_id_filename);
            this.train_meta_filename = io_proxy.convert_path(this.train_meta_filename);
            this.train_model_filename = io_proxy.convert_path(this.train_model_filename);
            this.train_predict_cm_filename = io_proxy.convert_path(this.train_predict_cm_filename);
            this.train_predict_filename = io_proxy.convert_path(this.train_predict_filename);

            var files = new List<string>()
            {
                this.options_filename          ,
                this.pbs_ctl_stderr_filename       ,
                this.pbs_ctl_stdout_filename       ,
                this.pbs_wkr_stderr_filename       ,
                this.pbs_wkr_stdout_filename       ,
                this.program_ctl_stderr_filename       ,
                this.program_ctl_stdout_filename       ,
                this.program_wkr_stderr_filename       ,
                this.program_wkr_stdout_filename       ,
                this.test_filename             ,
                this.test_labels_filename             ,
                this.test_id_filename          ,
                this.test_meta_filename        ,
                this.test_predict_cm_filename  ,
                this.test_predict_filename     ,
                this.train_filename            ,
                this.train_grid_filename       ,
                this.train_id_filename         ,
                this.train_meta_filename       ,
                this.train_model_filename      ,
                this.train_predict_cm_filename ,
                this.train_predict_filename    ,
            };

            var dirs = files.Where(a => !string.IsNullOrWhiteSpace(a)).Select(a => Path.GetDirectoryName(a) ?? null).Where(a => !string.IsNullOrWhiteSpace(a)).Distinct().ToList();

            dirs.ForEach(a =>
            {
                try
                {
                    
                    
                        io_proxy.CreateDirectory(a);
                    
                }
                catch (Exception)
                {
                }
            });
        }

        internal cmd_params()
        {
            convert_paths();
        }

        internal cmd_params(List<cmd_params> p)
        {
            //if (p.Select(a => a.probability_estimates).Distinct().Count() == 1) this.probability_estimates = p.FirstOrDefault().probability_estimates;
            //if (p.Select(a => a.shrinking_heuristics).Distinct().Count() == 1) this.shrinking_heuristics = p.FirstOrDefault().shrinking_heuristics;
            if (p.Select(a => a.alphabet).Distinct().Count() == 1) this.alphabet = p.FirstOrDefault().alphabet;
            if (p.Select(a => a.category).Distinct().Count() == 1) this.category = p.FirstOrDefault().category;
            if (p.Select(a => a.class_names).Distinct().Count() == 1) this.class_names = p.FirstOrDefault().class_names;
            if (p.Select(a => a.class_sizes).Distinct().Count() == 1) this.class_sizes = p.FirstOrDefault().class_sizes;
            if (p.Select(a => a.class_testing_sizes).Distinct().Count() == 1) this.class_testing_sizes = p.FirstOrDefault().class_testing_sizes;
            if (p.Select(a => a.class_training_sizes).Distinct().Count() == 1) this.class_training_sizes = p.FirstOrDefault().class_training_sizes;
            if (p.Select(a => a.class_weights).Distinct().Count() == 1) this.class_weights = p.FirstOrDefault().class_weights;
            if (p.Select(a => a.cmd).Distinct().Count() == 1) this.cmd = p.FirstOrDefault().cmd;
            if (p.Select(a => a.dataset_dir).Distinct().Count() == 1) this.dataset_dir = p.FirstOrDefault().dataset_dir;
            if (p.Select(a => a.dimension).Distinct().Count() == 1) this.dimension = p.FirstOrDefault().dimension;
            if (p.Select(a => a.experiment_name).Distinct().Count() == 1) this.experiment_name = p.FirstOrDefault().experiment_name;
            if (p.Select(a => a.feature_id).Distinct().Count() == 1) this.feature_id = p.FirstOrDefault().feature_id;
            if (p.Select(a => a.feature_selection_classes).Distinct().Count() == 1) this.feature_selection_classes = p.FirstOrDefault().feature_selection_classes;
            if (p.Select(a => a.feature_selection_metrics).Distinct().Count() == 1) this.feature_selection_metrics = p.FirstOrDefault().feature_selection_metrics;
            if (p.Select(a => a.forward).Distinct().Count() == 1) this.forward = p.FirstOrDefault().forward;
            if (p.Select(a => a.grid_coef0_exp_begin).Distinct().Count() == 1) this.grid_coef0_exp_begin = p.FirstOrDefault().grid_coef0_exp_begin;
            if (p.Select(a => a.grid_coef0_exp_end).Distinct().Count() == 1) this.grid_coef0_exp_end = p.FirstOrDefault().grid_coef0_exp_end;
            if (p.Select(a => a.grid_coef0_exp_step).Distinct().Count() == 1) this.grid_coef0_exp_step = p.FirstOrDefault().grid_coef0_exp_step;
            if (p.Select(a => a.grid_cost_exp_begin).Distinct().Count() == 1) this.grid_cost_exp_begin = p.FirstOrDefault().grid_cost_exp_begin;
            if (p.Select(a => a.grid_cost_exp_end).Distinct().Count() == 1) this.grid_cost_exp_end = p.FirstOrDefault().grid_cost_exp_end;
            if (p.Select(a => a.grid_cost_exp_step).Distinct().Count() == 1) this.grid_cost_exp_step = p.FirstOrDefault().grid_cost_exp_step;
            if (p.Select(a => a.grid_degree_exp_begin).Distinct().Count() == 1) this.grid_degree_exp_begin = p.FirstOrDefault().grid_degree_exp_begin;
            if (p.Select(a => a.grid_degree_exp_end).Distinct().Count() == 1) this.grid_degree_exp_end = p.FirstOrDefault().grid_degree_exp_end;
            if (p.Select(a => a.grid_degree_exp_step).Distinct().Count() == 1) this.grid_degree_exp_step = p.FirstOrDefault().grid_degree_exp_step;
            if (p.Select(a => a.grid_epsilon_exp_begin).Distinct().Count() == 1) this.grid_epsilon_exp_begin = p.FirstOrDefault().grid_epsilon_exp_begin;
            if (p.Select(a => a.grid_epsilon_exp_end).Distinct().Count() == 1) this.grid_epsilon_exp_end = p.FirstOrDefault().grid_epsilon_exp_end;
            if (p.Select(a => a.grid_epsilon_exp_step).Distinct().Count() == 1) this.grid_epsilon_exp_step = p.FirstOrDefault().grid_epsilon_exp_step;
            if (p.Select(a => a.grid_gamma_exp_begin).Distinct().Count() == 1) this.grid_gamma_exp_begin = p.FirstOrDefault().grid_gamma_exp_begin;
            if (p.Select(a => a.grid_gamma_exp_end).Distinct().Count() == 1) this.grid_gamma_exp_end = p.FirstOrDefault().grid_gamma_exp_end;
            if (p.Select(a => a.grid_gamma_exp_step).Distinct().Count() == 1) this.grid_gamma_exp_step = p.FirstOrDefault().grid_gamma_exp_step;
            if (p.Select(a => a.group).Distinct().Count() == 1) this.group = p.FirstOrDefault().group;
            if (p.Select(a => a.group_features).Distinct().Count() == 1) this.group_features = p.FirstOrDefault().group_features;
            if (p.Select(a => a.group_index).Distinct().Count() == 1) this.group_index = p.FirstOrDefault().group_index;
            if (p.Select(a => a.inner_cv_folds).Distinct().Count() == 1) this.inner_cv_folds = p.FirstOrDefault().inner_cv_folds;
            if (p.Select(a => a.iteration).Distinct().Count() == 1) this.iteration = p.FirstOrDefault().iteration;
            if (p.Select(a => a.job_id).Distinct().Count() == 1) this.job_id = p.FirstOrDefault().job_id;
            if (p.Select(a => a.libsvm_grid_max_time).Distinct().Count() == 1) this.libsvm_grid_max_time = p.FirstOrDefault().libsvm_grid_max_time;
            if (p.Select(a => a.libsvm_grid_memory_limit_mb).Distinct().Count() == 1) this.libsvm_grid_memory_limit_mb = p.FirstOrDefault().libsvm_grid_memory_limit_mb;
            if (p.Select(a => a.libsvm_grid_probability_estimates).Distinct().Count() == 1) this.libsvm_grid_probability_estimates = p.FirstOrDefault().libsvm_grid_probability_estimates;
            if (p.Select(a => a.libsvm_grid_quiet_mode).Distinct().Count() == 1) this.libsvm_grid_quiet_mode = p.FirstOrDefault().libsvm_grid_quiet_mode;
            if (p.Select(a => a.libsvm_grid_shrinking_heuristics).Distinct().Count() == 1) this.libsvm_grid_shrinking_heuristics = p.FirstOrDefault().libsvm_grid_shrinking_heuristics;
            if (p.Select(a => a.libsvm_predict_runtime).Distinct().Count() == 1) this.libsvm_predict_runtime = p.FirstOrDefault().libsvm_predict_runtime;
            if (p.Select(a => a.libsvm_train_max_time).Distinct().Count() == 1) this.libsvm_train_max_time = p.FirstOrDefault().libsvm_train_max_time;
            if (p.Select(a => a.libsvm_train_memory_limit_mb).Distinct().Count() == 1) this.libsvm_train_memory_limit_mb = p.FirstOrDefault().libsvm_train_memory_limit_mb;
            if (p.Select(a => a.libsvm_train_probability_estimates).Distinct().Count() == 1) this.libsvm_train_probability_estimates = p.FirstOrDefault().libsvm_train_probability_estimates;
            if (p.Select(a => a.libsvm_train_quiet_mode).Distinct().Count() == 1) this.libsvm_train_quiet_mode = p.FirstOrDefault().libsvm_train_quiet_mode;
            if (p.Select(a => a.libsvm_train_runtime).Distinct().Count() == 1) this.libsvm_train_runtime = p.FirstOrDefault().libsvm_train_runtime;
            if (p.Select(a => a.libsvm_train_shrinking_heuristics).Distinct().Count() == 1) this.libsvm_train_shrinking_heuristics = p.FirstOrDefault().libsvm_train_shrinking_heuristics;
            if (p.Select(a => a.member).Distinct().Count() == 1) this.member = p.FirstOrDefault().member;
            if (p.Select(a => a.negative_class_id).Distinct().Count() == 1) this.negative_class_id = p.FirstOrDefault().negative_class_id;
            if (p.Select(a => a.new_feature_count).Distinct().Count() == 1) this.new_feature_count = p.FirstOrDefault().new_feature_count;
            if (p.Select(a => a.new_group_count).Distinct().Count() == 1) this.new_group_count = p.FirstOrDefault().new_group_count;
            if (p.Select(a => a.old_feature_count).Distinct().Count() == 1) this.old_feature_count = p.FirstOrDefault().old_feature_count;
            if (p.Select(a => a.old_group_count).Distinct().Count() == 1) this.old_group_count = p.FirstOrDefault().old_group_count;
            if (p.Select(a => a.options_filename).Distinct().Count() == 1) this.options_filename = p.FirstOrDefault().options_filename;
            if (p.Select(a => a.outer_cv_folds).Distinct().Count() == 1) this.outer_cv_folds = p.FirstOrDefault().outer_cv_folds;
            if (p.Select(a => a.outer_cv_index).Distinct().Count() == 1) this.outer_cv_index = p.FirstOrDefault().outer_cv_index;
            if (p.Select(a => a.output_threshold_adjustment_performance).Distinct().Count() == 1) this.output_threshold_adjustment_performance = p.FirstOrDefault().output_threshold_adjustment_performance;
            if (p.Select(a => a.pbs_ctl_execution_directory).Distinct().Count() == 1) this.pbs_ctl_execution_directory = p.FirstOrDefault().pbs_ctl_execution_directory;
            if (p.Select(a => a.pbs_ctl_jobname).Distinct().Count() == 1) this.pbs_ctl_jobname = p.FirstOrDefault().pbs_ctl_jobname;
            if (p.Select(a => a.pbs_ctl_mail_addr).Distinct().Count() == 1) this.pbs_ctl_mail_addr = p.FirstOrDefault().pbs_ctl_mail_addr;
            if (p.Select(a => a.pbs_ctl_mail_opt).Distinct().Count() == 1) this.pbs_ctl_mail_opt = p.FirstOrDefault().pbs_ctl_mail_opt;
            if (p.Select(a => a.pbs_ctl_mem).Distinct().Count() == 1) this.pbs_ctl_mem = p.FirstOrDefault().pbs_ctl_mem;
            if (p.Select(a => a.pbs_ctl_nodes).Distinct().Count() == 1) this.pbs_ctl_nodes = p.FirstOrDefault().pbs_ctl_nodes;
            if (p.Select(a => a.pbs_ctl_ppn).Distinct().Count() == 1) this.pbs_ctl_ppn = p.FirstOrDefault().pbs_ctl_ppn;
            if (p.Select(a => a.pbs_ctl_stderr_filename).Distinct().Count() == 1) this.pbs_ctl_stderr_filename = p.FirstOrDefault().pbs_ctl_stderr_filename;
            if (p.Select(a => a.pbs_ctl_stdout_filename).Distinct().Count() == 1) this.pbs_ctl_stdout_filename = p.FirstOrDefault().pbs_ctl_stdout_filename;
            if (p.Select(a => a.pbs_ctl_submission_directory).Distinct().Count() == 1) this.pbs_ctl_submission_directory = p.FirstOrDefault().pbs_ctl_submission_directory;
            if (p.Select(a => a.pbs_ctl_walltime).Distinct().Count() == 1) this.pbs_ctl_walltime = p.FirstOrDefault().pbs_ctl_walltime;
            if (p.Select(a => a.pbs_wkr_execution_directory).Distinct().Count() == 1) this.pbs_wkr_execution_directory = p.FirstOrDefault().pbs_wkr_execution_directory;
            if (p.Select(a => a.pbs_wkr_jobname).Distinct().Count() == 1) this.pbs_wkr_jobname = p.FirstOrDefault().pbs_wkr_jobname;
            if (p.Select(a => a.pbs_wkr_mail_addr).Distinct().Count() == 1) this.pbs_wkr_mail_addr = p.FirstOrDefault().pbs_wkr_mail_addr;
            if (p.Select(a => a.pbs_wkr_mail_opt).Distinct().Count() == 1) this.pbs_wkr_mail_opt = p.FirstOrDefault().pbs_wkr_mail_opt;
            if (p.Select(a => a.pbs_wkr_mem).Distinct().Count() == 1) this.pbs_wkr_mem = p.FirstOrDefault().pbs_wkr_mem;
            if (p.Select(a => a.pbs_wkr_nodes).Distinct().Count() == 1) this.pbs_wkr_nodes = p.FirstOrDefault().pbs_wkr_nodes;
            if (p.Select(a => a.pbs_wkr_ppn).Distinct().Count() == 1) this.pbs_wkr_ppn = p.FirstOrDefault().pbs_wkr_ppn;
            if (p.Select(a => a.pbs_wkr_stderr_filename).Distinct().Count() == 1) this.pbs_wkr_stderr_filename = p.FirstOrDefault().pbs_wkr_stderr_filename;
            if (p.Select(a => a.pbs_wkr_stdout_filename).Distinct().Count() == 1) this.pbs_wkr_stdout_filename = p.FirstOrDefault().pbs_wkr_stdout_filename;
            if (p.Select(a => a.pbs_wkr_submission_directory).Distinct().Count() == 1) this.pbs_wkr_submission_directory = p.FirstOrDefault().pbs_wkr_submission_directory;
            if (p.Select(a => a.pbs_wkr_walltime).Distinct().Count() == 1) this.pbs_wkr_walltime = p.FirstOrDefault().pbs_wkr_walltime;
            if (p.Select(a => a.perspective).Distinct().Count() == 1) this.perspective = p.FirstOrDefault().perspective;
            if (p.Select(a => a.positive_class_id).Distinct().Count() == 1) this.positive_class_id = p.FirstOrDefault().positive_class_id;
            if (p.Select(a => a.program_ctl_stderr_filename).Distinct().Count() == 1) this.program_ctl_stderr_filename = p.FirstOrDefault().program_ctl_stderr_filename;
            if (p.Select(a => a.program_ctl_stdout_filename).Distinct().Count() == 1) this.program_ctl_stdout_filename = p.FirstOrDefault().program_ctl_stdout_filename;
            if (p.Select(a => a.program_runtime).Distinct().Count() == 1) this.program_runtime = p.FirstOrDefault().program_runtime;
            if (p.Select(a => a.program_wkr_stderr_filename).Distinct().Count() == 1) this.program_wkr_stderr_filename = p.FirstOrDefault().program_wkr_stderr_filename;
            if (p.Select(a => a.program_wkr_stdout_filename).Distinct().Count() == 1) this.program_wkr_stdout_filename = p.FirstOrDefault().program_wkr_stdout_filename;
            if (p.Select(a => a.randomisation_cv_folds).Distinct().Count() == 1) this.randomisation_cv_folds = p.FirstOrDefault().randomisation_cv_folds;
            if (p.Select(a => a.randomisation_cv_index).Distinct().Count() == 1) this.randomisation_cv_index = p.FirstOrDefault().randomisation_cv_index;
            if (p.Select(a => a.results_root_folder).Distinct().Count() == 1) this.results_root_folder = p.FirstOrDefault().results_root_folder;
            if (p.Select(a => a.save_test_id).Distinct().Count() == 1) this.save_test_id = p.FirstOrDefault().save_test_id;
            if (p.Select(a => a.save_test_meta).Distinct().Count() == 1) this.save_test_meta = p.FirstOrDefault().save_test_meta;
            if (p.Select(a => a.save_train_id).Distinct().Count() == 1) this.save_train_id = p.FirstOrDefault().save_train_id;
            if (p.Select(a => a.save_train_meta).Distinct().Count() == 1) this.save_train_meta = p.FirstOrDefault().save_train_meta;
            if (p.Select(a => a.scale_function).Distinct().Count() == 1) this.scale_function = p.FirstOrDefault().scale_function;
            if (p.Select(a => a.source).Distinct().Count() == 1) this.source = p.FirstOrDefault().source;
            if (p.Select(a => a.svm_kernel).Distinct().Count() == 1) this.svm_kernel = p.FirstOrDefault().svm_kernel;
            if (p.Select(a => a.svm_type).Distinct().Count() == 1) this.svm_type = p.FirstOrDefault().svm_type;
            if (p.Select(a => a.test_filename).Distinct().Count() == 1) this.test_filename = p.FirstOrDefault().test_filename;
            if (p.Select(a => a.test_labels_filename).Distinct().Count() == 1) this.test_labels_filename = p.FirstOrDefault().test_labels_filename;
            if (p.Select(a => a.test_id_filename).Distinct().Count() == 1) this.test_id_filename = p.FirstOrDefault().test_id_filename;
            if (p.Select(a => a.test_meta_filename).Distinct().Count() == 1) this.test_meta_filename = p.FirstOrDefault().test_meta_filename;
            if (p.Select(a => a.test_predict_cm_filename).Distinct().Count() == 1) this.test_predict_cm_filename = p.FirstOrDefault().test_predict_cm_filename;
            if (p.Select(a => a.test_predict_filename).Distinct().Count() == 1) this.test_predict_filename = p.FirstOrDefault().test_predict_filename;
            if (p.Select(a => a.train_filename).Distinct().Count() == 1) this.train_filename = p.FirstOrDefault().train_filename;
            if (p.Select(a => a.train_grid_filename).Distinct().Count() == 1) this.train_grid_filename = p.FirstOrDefault().train_grid_filename;
            if (p.Select(a => a.train_id_filename).Distinct().Count() == 1) this.train_id_filename = p.FirstOrDefault().train_id_filename;
            if (p.Select(a => a.train_meta_filename).Distinct().Count() == 1) this.train_meta_filename = p.FirstOrDefault().train_meta_filename;
            if (p.Select(a => a.train_model_filename).Distinct().Count() == 1) this.train_model_filename = p.FirstOrDefault().train_model_filename;
            if (p.Select(a => a.train_predict_cm_filename).Distinct().Count() == 1) this.train_predict_cm_filename = p.FirstOrDefault().train_predict_cm_filename;
            if (p.Select(a => a.train_predict_filename).Distinct().Count() == 1) this.train_predict_filename = p.FirstOrDefault().train_predict_filename;


            convert_paths();
        }

        internal cmd_params(cmd_params p)
        {
            //this.probability_estimates = p.probability_estimates;
            //this.shrinking_heuristics = p.shrinking_heuristics;
            this.alphabet = p.alphabet;
            this.category = p.category;
            this.class_names = p.class_names;
            this.class_sizes = p.class_sizes;
            this.class_testing_sizes = p.class_testing_sizes;
            this.class_training_sizes = p.class_training_sizes;
            this.class_weights = p.class_weights;
            this.cmd = p.cmd;
            this.dataset_dir = p.dataset_dir;
            this.dimension = p.dimension;
            this.experiment_name = p.experiment_name;
            this.feature_id = p.feature_id;
            this.feature_selection_classes = p.feature_selection_classes;
            this.feature_selection_metrics = p.feature_selection_metrics;
            this.forward = p.forward;
            this.grid_coef0_exp_begin = p.grid_coef0_exp_begin;
            this.grid_coef0_exp_end = p.grid_coef0_exp_end;
            this.grid_coef0_exp_step = p.grid_coef0_exp_step;
            this.grid_cost_exp_begin = p.grid_cost_exp_begin;
            this.grid_cost_exp_end = p.grid_cost_exp_end;
            this.grid_cost_exp_step = p.grid_cost_exp_step;
            this.grid_degree_exp_begin = p.grid_degree_exp_begin;
            this.grid_degree_exp_end = p.grid_degree_exp_end;
            this.grid_degree_exp_step = p.grid_degree_exp_step;
            this.grid_epsilon_exp_begin = p.grid_epsilon_exp_begin;
            this.grid_epsilon_exp_end = p.grid_epsilon_exp_end;
            this.grid_epsilon_exp_step = p.grid_epsilon_exp_step;
            this.grid_gamma_exp_begin = p.grid_gamma_exp_begin;
            this.grid_gamma_exp_end = p.grid_gamma_exp_end;
            this.grid_gamma_exp_step = p.grid_gamma_exp_step;
            this.group = p.group;
            this.group_features = p.group_features;
            this.group_index = p.group_index;
            this.inner_cv_folds = p.inner_cv_folds;
            this.iteration = p.iteration;
            this.job_id = p.job_id;
            this.libsvm_grid_max_time = p.libsvm_grid_max_time;
            this.libsvm_grid_memory_limit_mb = p.libsvm_grid_memory_limit_mb;
            this.libsvm_grid_probability_estimates = p.libsvm_grid_probability_estimates;
            this.libsvm_grid_quiet_mode = p.libsvm_grid_quiet_mode;
            this.libsvm_grid_shrinking_heuristics = p.libsvm_grid_shrinking_heuristics;
            this.libsvm_predict_runtime = p.libsvm_predict_runtime;
            this.libsvm_train_max_time = p.libsvm_train_max_time;
            this.libsvm_train_memory_limit_mb = p.libsvm_train_memory_limit_mb;
            this.libsvm_train_probability_estimates = p.libsvm_train_probability_estimates;
            this.libsvm_train_quiet_mode = p.libsvm_train_quiet_mode;
            this.libsvm_train_runtime = p.libsvm_train_runtime;
            this.libsvm_train_shrinking_heuristics = p.libsvm_train_shrinking_heuristics;
            this.member = p.member;
            this.negative_class_id = p.negative_class_id;
            this.new_feature_count = p.new_feature_count;
            this.new_group_count = p.new_group_count;
            this.old_feature_count = p.old_feature_count;
            this.old_group_count = p.old_group_count;
            this.options_filename = p.options_filename;
            this.outer_cv_folds = p.outer_cv_folds;
            this.outer_cv_index = p.outer_cv_index;
            this.output_threshold_adjustment_performance = p.output_threshold_adjustment_performance;
            this.pbs_ctl_execution_directory = p.pbs_ctl_execution_directory;
            this.pbs_ctl_jobname = p.pbs_ctl_jobname;
            this.pbs_ctl_mail_addr = p.pbs_ctl_mail_addr;
            this.pbs_ctl_mail_opt = p.pbs_ctl_mail_opt;
            this.pbs_ctl_mem = p.pbs_ctl_mem;
            this.pbs_ctl_nodes = p.pbs_ctl_nodes;
            this.pbs_ctl_ppn = p.pbs_ctl_ppn;
            this.pbs_ctl_stderr_filename = p.pbs_ctl_stderr_filename;
            this.pbs_ctl_stdout_filename = p.pbs_ctl_stdout_filename;
            this.pbs_ctl_submission_directory = p.pbs_ctl_submission_directory;
            this.pbs_ctl_walltime = p.pbs_ctl_walltime;
            this.pbs_wkr_execution_directory = p.pbs_wkr_execution_directory;
            this.pbs_wkr_jobname = p.pbs_wkr_jobname;
            this.pbs_wkr_mail_addr = p.pbs_wkr_mail_addr;
            this.pbs_wkr_mail_opt = p.pbs_wkr_mail_opt;
            this.pbs_wkr_mem = p.pbs_wkr_mem;
            this.pbs_wkr_nodes = p.pbs_wkr_nodes;
            this.pbs_wkr_ppn = p.pbs_wkr_ppn;
            this.pbs_wkr_stderr_filename = p.pbs_wkr_stderr_filename;
            this.pbs_wkr_stdout_filename = p.pbs_wkr_stdout_filename;
            this.pbs_wkr_submission_directory = p.pbs_wkr_submission_directory;
            this.pbs_wkr_walltime = p.pbs_wkr_walltime;
            this.perspective = p.perspective;
            this.positive_class_id = p.positive_class_id;
            this.program_ctl_stderr_filename = p.program_ctl_stderr_filename;
            this.program_ctl_stdout_filename = p.program_ctl_stdout_filename;
            this.program_runtime = p.program_runtime;
            this.program_wkr_stderr_filename = p.program_wkr_stderr_filename;
            this.program_wkr_stdout_filename = p.program_wkr_stdout_filename;
            this.randomisation_cv_folds = p.randomisation_cv_folds;
            this.randomisation_cv_index = p.randomisation_cv_index;
            this.results_root_folder = p.results_root_folder;
            this.save_test_id = p.save_test_id;
            this.save_test_meta = p.save_test_meta;
            this.save_train_id = p.save_train_id;
            this.save_train_meta = p.save_train_meta;
            this.scale_function = p.scale_function;
            this.source = p.source;
            this.svm_kernel = p.svm_kernel;
            this.svm_type = p.svm_type;
            this.test_filename = p.test_filename;
            this.test_labels_filename = p.test_labels_filename;
            this.test_id_filename = p.test_id_filename;
            this.test_meta_filename = p.test_meta_filename;
            this.test_predict_cm_filename = p.test_predict_cm_filename;
            this.test_predict_filename = p.test_predict_filename;
            this.train_filename = p.train_filename;
            this.train_grid_filename = p.train_grid_filename;
            this.train_id_filename = p.train_id_filename;
            this.train_meta_filename = p.train_meta_filename;
            this.train_model_filename = p.train_model_filename;
            this.train_predict_cm_filename = p.train_predict_cm_filename;
            this.train_predict_filename = p.train_predict_filename;


            convert_paths();
        }

        internal cmd_params(string params_filename) : this(io_proxy.ReadAllLines(io_proxy.convert_path(params_filename), nameof(cmd_params), nameof(cmd_params)))
        {

        }

        internal cmd_params(string[] params_file_data = null)//string[] args)
        {
            if (params_file_data == null || params_file_data.Length == 0) return;




            //if (args == null || args.Length == 0) return;

            //var args2 = new List<(string key, string value)>();

            //for (var i = 0; i < args.Length; i++)
            //{
            //    var x = args[i];

            //    if (!string.IsNullOrWhiteSpace(x) && x.StartsWith('-') && x.Any(a => char.IsLetter(a)))
            //    {
            //        x = x.Substring(1);

            //        var y = (i < args.Length - 1 && args[i + 1].Length > 0 && (args[i + 1][0] != '-' || args[i + 1].Skip(1).All(b => char.IsDigit(b)))) ? args[i + 1] : "";

            //        args2.Add((x, y));
            //    }
            //}

            // 165,1777 --> 132,132 + 33,177 -->

            var args2 = params_file_data.Where(a => !string.IsNullOrWhiteSpace(a)).Select(a => (key: a.Substring(0, a.IndexOf('=', StringComparison.InvariantCulture) > -1 ? a.IndexOf('=', StringComparison.InvariantCulture) : 0), value: a.Substring(a.IndexOf('=', StringComparison.InvariantCulture) > -1 ? a.IndexOf('=', StringComparison.InvariantCulture) + 1 : 0))).ToList();
            //if (args2.Any(a => a.key == nameof(probability_estimates) && !string.IsNullOrWhiteSpace(a.value))) probability_estimates = bool.Parse(args2.FirstOrDefault(a => a.key == nameof(probability_estimates)).value);
            //if (args2.Any(a => a.key == nameof(shrinking_heuristics) && !string.IsNullOrWhiteSpace(a.value))) shrinking_heuristics = bool.Parse(args2.FirstOrDefault(a => a.key == nameof(shrinking_heuristics)).value);
            //if (args2.Any(a => a.key == nameof(test_grid_filename))) test_grid_filename = args2.FirstOrDefault(a => a.key == nameof(test_grid_filename)).value;
            //if (args2.Any(a => a.key == nameof(test_predict_pr_filename) && !string.IsNullOrWhiteSpace(a.value))) test_predict_pr_filename = args2.FirstOrDefault(a => a.key == nameof(test_predict_pr_filename)).value;
            //if (args2.Any(a => a.key == nameof(test_predict_roc_filename) && !string.IsNullOrWhiteSpace(a.value))) test_predict_roc_filename = args2.FirstOrDefault(a => a.key == nameof(test_predict_roc_filename)).value;
            //if (args2.Any(a => a.key == nameof(train_predict_pr_filename) && !string.IsNullOrWhiteSpace(a.value))) train_predict_pr_filename = args2.FirstOrDefault(a => a.key == nameof(train_predict_pr_filename)).value;
            //if (args2.Any(a => a.key == nameof(train_predict_roc_filename) && !string.IsNullOrWhiteSpace(a.value))) train_predict_roc_filename = args2.FirstOrDefault(a => a.key == nameof(train_predict_roc_filename)).value;
            if (args2.Any(a => a.key == nameof(alphabet) && !string.IsNullOrWhiteSpace(a.value))) alphabet = args2.FirstOrDefault(a => a.key == nameof(alphabet)).value;
            if (args2.Any(a => a.key == nameof(category) && !string.IsNullOrWhiteSpace(a.value))) category = args2.FirstOrDefault(a => a.key == nameof(category)).value;
            if (args2.Any(a => a.key == nameof(class_names) && !string.IsNullOrWhiteSpace(a.value))) class_names = args2.FirstOrDefault(a => a.key == nameof(class_names)).value.Split(';').Select((a, i) => (int.Parse(a.Split(':')[0], CultureInfo.InvariantCulture), a.Split(':')[1])).ToList();
            if (args2.Any(a => a.key == nameof(class_sizes) && !string.IsNullOrWhiteSpace(a.value))) class_sizes = args2.FirstOrDefault(a => a.key == nameof(class_sizes)).value.Split(';').Select((a, i) => (int.Parse(a.Split(':')[0], CultureInfo.InvariantCulture), int.Parse(a.Split(':')[1], CultureInfo.InvariantCulture))).ToList();
            if (args2.Any(a => a.key == nameof(class_testing_sizes) && !string.IsNullOrWhiteSpace(a.value))) class_testing_sizes = args2.FirstOrDefault(a => a.key == nameof(class_testing_sizes)).value.Split(';').Select((a, i) => (int.Parse(a.Split(':')[0], CultureInfo.InvariantCulture), int.Parse(a.Split(':')[1], CultureInfo.InvariantCulture))).ToList();
            if (args2.Any(a => a.key == nameof(class_training_sizes) && !string.IsNullOrWhiteSpace(a.value))) class_training_sizes = args2.FirstOrDefault(a => a.key == nameof(class_training_sizes)).value.Split(';').Select((a, i) => (int.Parse(a.Split(':')[0], CultureInfo.InvariantCulture), int.Parse(a.Split(':')[1], CultureInfo.InvariantCulture))).ToList();
            if (args2.Any(a => a.key == nameof(class_weights) && !string.IsNullOrWhiteSpace(a.value))) class_weights = args2.FirstOrDefault(a => a.key == nameof(class_weights)).value.Split(';').Select((a, i) => (int.Parse(a.Split(':')[0], CultureInfo.InvariantCulture), double.Parse(a.Split(':')[1], NumberStyles.Float, CultureInfo.InvariantCulture))).ToList();
            if (args2.Any(a => a.key == nameof(cmd) && !string.IsNullOrWhiteSpace(a.value))) cmd = (cmd)Enum.Parse(typeof(cmd), args2.FirstOrDefault(a => a.key == nameof(cmd)).value);
            if (args2.Any(a => a.key == nameof(dataset_dir) && !string.IsNullOrWhiteSpace(a.value))) dataset_dir = args2.FirstOrDefault(a => a.key == nameof(dataset_dir)).value;
            if (args2.Any(a => a.key == nameof(dimension) && !string.IsNullOrWhiteSpace(a.value))) dimension = args2.FirstOrDefault(a => a.key == nameof(dimension)).value;
            if (args2.Any(a => a.key == nameof(experiment_name) && !string.IsNullOrWhiteSpace(a.value))) experiment_name = args2.FirstOrDefault(a => a.key == nameof(experiment_name)).value;
            if (args2.Any(a => a.key == nameof(feature_id) && !string.IsNullOrWhiteSpace(a.value))) feature_id = int.Parse(args2.FirstOrDefault(a => a.key == nameof(feature_id)).value, CultureInfo.InvariantCulture);
            if (args2.Any(a => a.key == nameof(feature_selection_classes) && !string.IsNullOrWhiteSpace(a.value))) feature_selection_classes = args2.FirstOrDefault(a => a.key == nameof(feature_selection_classes)).value.Split(new char[] { ';' }, StringSplitOptions.RemoveEmptyEntries).Select(a => int.Parse(a, CultureInfo.InvariantCulture)).ToList();
            if (args2.Any(a => a.key == nameof(feature_selection_metrics) && !string.IsNullOrWhiteSpace(a.value))) feature_selection_metrics = args2.FirstOrDefault(a => a.key == nameof(feature_selection_metrics)).value.Split(new char[] { ';' }, StringSplitOptions.RemoveEmptyEntries).ToList();
            if (args2.Any(a => a.key == nameof(forward) && !string.IsNullOrWhiteSpace(a.value))) forward = bool.Parse(args2.FirstOrDefault(a => a.key == nameof(forward)).value);
            if (args2.Any(a => a.key == nameof(grid_coef0_exp_begin) && !string.IsNullOrWhiteSpace(a.value))) grid_coef0_exp_begin = double.Parse(args2.FirstOrDefault(a => a.key == nameof(grid_coef0_exp_begin)).value, NumberStyles.Float, CultureInfo.InvariantCulture);
            if (args2.Any(a => a.key == nameof(grid_coef0_exp_end) && !string.IsNullOrWhiteSpace(a.value))) grid_coef0_exp_end = double.Parse(args2.FirstOrDefault(a => a.key == nameof(grid_coef0_exp_end)).value, NumberStyles.Float, CultureInfo.InvariantCulture);
            if (args2.Any(a => a.key == nameof(grid_coef0_exp_step) && !string.IsNullOrWhiteSpace(a.value))) grid_coef0_exp_step = double.Parse(args2.FirstOrDefault(a => a.key == nameof(grid_coef0_exp_step)).value, NumberStyles.Float, CultureInfo.InvariantCulture);
            if (args2.Any(a => a.key == nameof(grid_cost_exp_begin) && !string.IsNullOrWhiteSpace(a.value))) grid_cost_exp_begin = double.Parse(args2.FirstOrDefault(a => a.key == nameof(grid_cost_exp_begin)).value, NumberStyles.Float, CultureInfo.InvariantCulture);
            if (args2.Any(a => a.key == nameof(grid_cost_exp_end) && !string.IsNullOrWhiteSpace(a.value))) grid_cost_exp_end = double.Parse(args2.FirstOrDefault(a => a.key == nameof(grid_cost_exp_end)).value, NumberStyles.Float, CultureInfo.InvariantCulture);
            if (args2.Any(a => a.key == nameof(grid_cost_exp_step) && !string.IsNullOrWhiteSpace(a.value))) grid_cost_exp_step = double.Parse(args2.FirstOrDefault(a => a.key == nameof(grid_cost_exp_step)).value, NumberStyles.Float, CultureInfo.InvariantCulture);
            if (args2.Any(a => a.key == nameof(grid_degree_exp_begin) && !string.IsNullOrWhiteSpace(a.value))) grid_degree_exp_begin = double.Parse(args2.FirstOrDefault(a => a.key == nameof(grid_degree_exp_begin)).value, NumberStyles.Float, CultureInfo.InvariantCulture);
            if (args2.Any(a => a.key == nameof(grid_degree_exp_end) && !string.IsNullOrWhiteSpace(a.value))) grid_degree_exp_end = double.Parse(args2.FirstOrDefault(a => a.key == nameof(grid_degree_exp_end)).value, NumberStyles.Float, CultureInfo.InvariantCulture);
            if (args2.Any(a => a.key == nameof(grid_degree_exp_step) && !string.IsNullOrWhiteSpace(a.value))) grid_degree_exp_step = double.Parse(args2.FirstOrDefault(a => a.key == nameof(grid_degree_exp_step)).value, NumberStyles.Float, CultureInfo.InvariantCulture);
            if (args2.Any(a => a.key == nameof(grid_epsilon_exp_begin) && !string.IsNullOrWhiteSpace(a.value))) grid_epsilon_exp_begin = double.Parse(args2.FirstOrDefault(a => a.key == nameof(grid_epsilon_exp_begin)).value, NumberStyles.Float, CultureInfo.InvariantCulture);
            if (args2.Any(a => a.key == nameof(grid_epsilon_exp_end) && !string.IsNullOrWhiteSpace(a.value))) grid_epsilon_exp_end = double.Parse(args2.FirstOrDefault(a => a.key == nameof(grid_epsilon_exp_end)).value, NumberStyles.Float, CultureInfo.InvariantCulture);
            if (args2.Any(a => a.key == nameof(grid_epsilon_exp_step) && !string.IsNullOrWhiteSpace(a.value))) grid_epsilon_exp_step = double.Parse(args2.FirstOrDefault(a => a.key == nameof(grid_epsilon_exp_step)).value, NumberStyles.Float, CultureInfo.InvariantCulture);
            if (args2.Any(a => a.key == nameof(grid_gamma_exp_begin) && !string.IsNullOrWhiteSpace(a.value))) grid_gamma_exp_begin = double.Parse(args2.FirstOrDefault(a => a.key == nameof(grid_gamma_exp_begin)).value, NumberStyles.Float, CultureInfo.InvariantCulture);
            if (args2.Any(a => a.key == nameof(grid_gamma_exp_end) && !string.IsNullOrWhiteSpace(a.value))) grid_gamma_exp_end = double.Parse(args2.FirstOrDefault(a => a.key == nameof(grid_gamma_exp_end)).value, NumberStyles.Float, CultureInfo.InvariantCulture);
            if (args2.Any(a => a.key == nameof(grid_gamma_exp_step) && !string.IsNullOrWhiteSpace(a.value))) grid_gamma_exp_step = double.Parse(args2.FirstOrDefault(a => a.key == nameof(grid_gamma_exp_step)).value, NumberStyles.Float, CultureInfo.InvariantCulture);
            if (args2.Any(a => a.key == nameof(group) && !string.IsNullOrWhiteSpace(a.value))) group = args2.FirstOrDefault(a => a.key == nameof(group)).value;
            if (args2.Any(a => a.key == nameof(group_features) && !string.IsNullOrWhiteSpace(a.value))) group_features = bool.Parse(args2.FirstOrDefault(a => a.key == nameof(group_features)).value);
            if (args2.Any(a => a.key == nameof(group_index) && !string.IsNullOrWhiteSpace(a.value))) group_index = int.Parse(args2.FirstOrDefault(a => a.key == nameof(group_index)).value, CultureInfo.InvariantCulture);
            if (args2.Any(a => a.key == nameof(inner_cv_folds) && !string.IsNullOrWhiteSpace(a.value))) inner_cv_folds = int.Parse(args2.FirstOrDefault(a => a.key == nameof(inner_cv_folds)).value, CultureInfo.InvariantCulture);
            if (args2.Any(a => a.key == nameof(iteration) && !string.IsNullOrWhiteSpace(a.value))) iteration = int.Parse(args2.FirstOrDefault(a => a.key == nameof(iteration)).value, CultureInfo.InvariantCulture);
            if (args2.Any(a => a.key == nameof(job_id) && !string.IsNullOrWhiteSpace(a.value))) job_id = int.Parse(args2.FirstOrDefault(a => a.key == nameof(job_id)).value, CultureInfo.InvariantCulture);
            if (args2.Any(a => a.key == nameof(libsvm_grid_max_time) && !string.IsNullOrWhiteSpace(a.value))) libsvm_grid_max_time = TimeSpan.Parse(args2.FirstOrDefault(a => a.key == nameof(libsvm_grid_max_time)).value, DateTimeFormatInfo.InvariantInfo);
            if (args2.Any(a => a.key == nameof(libsvm_grid_memory_limit_mb) && !string.IsNullOrWhiteSpace(a.value))) libsvm_grid_memory_limit_mb = int.Parse(args2.FirstOrDefault(a => a.key == nameof(libsvm_grid_memory_limit_mb)).value, CultureInfo.InvariantCulture);
            if (args2.Any(a => a.key == nameof(libsvm_grid_probability_estimates) && !string.IsNullOrWhiteSpace(a.value))) libsvm_grid_probability_estimates = bool.Parse(args2.FirstOrDefault(a => a.key == nameof(libsvm_grid_probability_estimates)).value);
            if (args2.Any(a => a.key == nameof(libsvm_grid_quiet_mode) && !string.IsNullOrWhiteSpace(a.value))) libsvm_grid_quiet_mode = bool.Parse(args2.FirstOrDefault(a => a.key == nameof(libsvm_grid_quiet_mode)).value);
            if (args2.Any(a => a.key == nameof(libsvm_grid_shrinking_heuristics) && !string.IsNullOrWhiteSpace(a.value))) libsvm_grid_shrinking_heuristics = bool.Parse(args2.FirstOrDefault(a => a.key == nameof(libsvm_grid_shrinking_heuristics)).value);
            if (args2.Any(a => a.key == nameof(libsvm_predict_runtime) && !string.IsNullOrWhiteSpace(a.value))) libsvm_predict_runtime = args2.FirstOrDefault(a => a.key == nameof(libsvm_predict_runtime)).value;
            if (args2.Any(a => a.key == nameof(libsvm_train_max_time) && !string.IsNullOrWhiteSpace(a.value))) libsvm_train_max_time = TimeSpan.Parse(args2.FirstOrDefault(a => a.key == nameof(libsvm_train_max_time)).value, DateTimeFormatInfo.InvariantInfo);
            if (args2.Any(a => a.key == nameof(libsvm_train_memory_limit_mb) && !string.IsNullOrWhiteSpace(a.value))) libsvm_train_memory_limit_mb = int.Parse(args2.FirstOrDefault(a => a.key == nameof(libsvm_train_memory_limit_mb)).value, CultureInfo.InvariantCulture);
            if (args2.Any(a => a.key == nameof(libsvm_train_probability_estimates) && !string.IsNullOrWhiteSpace(a.value))) libsvm_train_probability_estimates = bool.Parse(args2.FirstOrDefault(a => a.key == nameof(libsvm_train_probability_estimates)).value);
            if (args2.Any(a => a.key == nameof(libsvm_train_quiet_mode) && !string.IsNullOrWhiteSpace(a.value))) libsvm_train_quiet_mode = bool.Parse(args2.FirstOrDefault(a => a.key == nameof(libsvm_train_quiet_mode)).value);
            if (args2.Any(a => a.key == nameof(libsvm_train_runtime) && !string.IsNullOrWhiteSpace(a.value))) libsvm_train_runtime = args2.FirstOrDefault(a => a.key == nameof(libsvm_train_runtime)).value;
            if (args2.Any(a => a.key == nameof(libsvm_train_shrinking_heuristics) && !string.IsNullOrWhiteSpace(a.value))) libsvm_train_shrinking_heuristics = bool.Parse(args2.FirstOrDefault(a => a.key == nameof(libsvm_train_shrinking_heuristics)).value);
            if (args2.Any(a => a.key == nameof(member) && !string.IsNullOrWhiteSpace(a.value))) member = args2.FirstOrDefault(a => a.key == nameof(member)).value;
            if (args2.Any(a => a.key == nameof(negative_class_id) && !string.IsNullOrWhiteSpace(a.value))) negative_class_id = int.Parse(args2.FirstOrDefault(a => a.key == nameof(negative_class_id)).value, CultureInfo.InvariantCulture);
            if (args2.Any(a => a.key == nameof(new_feature_count) && !string.IsNullOrWhiteSpace(a.value))) new_feature_count = int.Parse(args2.FirstOrDefault(a => a.key == nameof(new_feature_count)).value, CultureInfo.InvariantCulture);
            if (args2.Any(a => a.key == nameof(new_group_count) && !string.IsNullOrWhiteSpace(a.value))) new_group_count = int.Parse(args2.FirstOrDefault(a => a.key == nameof(new_group_count)).value, CultureInfo.InvariantCulture);
            if (args2.Any(a => a.key == nameof(old_feature_count) && !string.IsNullOrWhiteSpace(a.value))) old_feature_count = int.Parse(args2.FirstOrDefault(a => a.key == nameof(old_feature_count)).value, CultureInfo.InvariantCulture);
            if (args2.Any(a => a.key == nameof(old_group_count) && !string.IsNullOrWhiteSpace(a.value))) old_group_count = int.Parse(args2.FirstOrDefault(a => a.key == nameof(old_group_count)).value, CultureInfo.InvariantCulture);
            if (args2.Any(a => a.key == nameof(options_filename) && !string.IsNullOrWhiteSpace(a.value))) options_filename = args2.FirstOrDefault(a => a.key == nameof(options_filename)).value;
            if (args2.Any(a => a.key == nameof(outer_cv_folds) && !string.IsNullOrWhiteSpace(a.value))) outer_cv_folds = int.Parse(args2.FirstOrDefault(a => a.key == nameof(outer_cv_folds)).value, CultureInfo.InvariantCulture);
            if (args2.Any(a => a.key == nameof(outer_cv_index) && !string.IsNullOrWhiteSpace(a.value))) outer_cv_index = int.Parse(args2.FirstOrDefault(a => a.key == nameof(outer_cv_index)).value, CultureInfo.InvariantCulture);
            if (args2.Any(a => a.key == nameof(output_threshold_adjustment_performance) && !string.IsNullOrWhiteSpace(a.value))) output_threshold_adjustment_performance = bool.Parse(args2.FirstOrDefault(a => a.key == nameof(output_threshold_adjustment_performance)).value);
            if (args2.Any(a => a.key == nameof(pbs_ctl_execution_directory) && !string.IsNullOrWhiteSpace(a.value))) pbs_ctl_execution_directory = args2.FirstOrDefault(a => a.key == nameof(pbs_ctl_execution_directory)).value;
            if (args2.Any(a => a.key == nameof(pbs_ctl_jobname) && !string.IsNullOrWhiteSpace(a.value))) pbs_ctl_jobname = args2.FirstOrDefault(a => a.key == nameof(pbs_ctl_jobname)).value;
            if (args2.Any(a => a.key == nameof(pbs_ctl_mail_addr) && !string.IsNullOrWhiteSpace(a.value))) pbs_ctl_mail_addr = args2.FirstOrDefault(a => a.key == nameof(pbs_ctl_mail_addr)).value;
            if (args2.Any(a => a.key == nameof(pbs_ctl_mail_opt) && !string.IsNullOrWhiteSpace(a.value))) pbs_ctl_mail_opt = args2.FirstOrDefault(a => a.key == nameof(pbs_ctl_mail_opt)).value;
            if (args2.Any(a => a.key == nameof(pbs_ctl_mem) && !string.IsNullOrWhiteSpace(a.value))) pbs_ctl_mem = args2.FirstOrDefault(a => a.key == nameof(pbs_ctl_mem)).value;
            if (args2.Any(a => a.key == nameof(pbs_ctl_nodes) && !string.IsNullOrWhiteSpace(a.value))) pbs_ctl_nodes = int.Parse(args2.FirstOrDefault(a => a.key == nameof(pbs_ctl_nodes)).value, CultureInfo.InvariantCulture);
            if (args2.Any(a => a.key == nameof(pbs_ctl_ppn) && !string.IsNullOrWhiteSpace(a.value))) pbs_ctl_ppn = int.Parse(args2.FirstOrDefault(a => a.key == nameof(pbs_ctl_ppn)).value, CultureInfo.InvariantCulture);
            if (args2.Any(a => a.key == nameof(pbs_ctl_stderr_filename) && !string.IsNullOrWhiteSpace(a.value))) pbs_ctl_stderr_filename = args2.FirstOrDefault(a => a.key == nameof(pbs_ctl_stderr_filename)).value;
            if (args2.Any(a => a.key == nameof(pbs_ctl_stdout_filename) && !string.IsNullOrWhiteSpace(a.value))) pbs_ctl_stdout_filename = args2.FirstOrDefault(a => a.key == nameof(pbs_ctl_stdout_filename)).value;
            if (args2.Any(a => a.key == nameof(pbs_ctl_submission_directory) && !string.IsNullOrWhiteSpace(a.value))) pbs_ctl_submission_directory = args2.FirstOrDefault(a => a.key == nameof(pbs_ctl_submission_directory)).value;
            if (args2.Any(a => a.key == nameof(pbs_ctl_walltime) && !string.IsNullOrWhiteSpace(a.value))) pbs_ctl_walltime = args2.FirstOrDefault(a => a.key == nameof(pbs_ctl_walltime)).value;
            if (args2.Any(a => a.key == nameof(pbs_wkr_execution_directory) && !string.IsNullOrWhiteSpace(a.value))) pbs_wkr_execution_directory = args2.FirstOrDefault(a => a.key == nameof(pbs_wkr_execution_directory)).value;
            if (args2.Any(a => a.key == nameof(pbs_wkr_jobname) && !string.IsNullOrWhiteSpace(a.value))) pbs_wkr_jobname = args2.FirstOrDefault(a => a.key == nameof(pbs_wkr_jobname)).value;
            if (args2.Any(a => a.key == nameof(pbs_wkr_mail_addr) && !string.IsNullOrWhiteSpace(a.value))) pbs_wkr_mail_addr = args2.FirstOrDefault(a => a.key == nameof(pbs_wkr_mail_addr)).value;
            if (args2.Any(a => a.key == nameof(pbs_wkr_mail_opt) && !string.IsNullOrWhiteSpace(a.value))) pbs_wkr_mail_opt = args2.FirstOrDefault(a => a.key == nameof(pbs_wkr_mail_opt)).value;
            if (args2.Any(a => a.key == nameof(pbs_wkr_mem) && !string.IsNullOrWhiteSpace(a.value))) pbs_wkr_mem = args2.FirstOrDefault(a => a.key == nameof(pbs_wkr_mem)).value;
            if (args2.Any(a => a.key == nameof(pbs_wkr_nodes) && !string.IsNullOrWhiteSpace(a.value))) pbs_wkr_nodes = int.Parse(args2.FirstOrDefault(a => a.key == nameof(pbs_wkr_nodes)).value, CultureInfo.InvariantCulture);
            if (args2.Any(a => a.key == nameof(pbs_wkr_ppn) && !string.IsNullOrWhiteSpace(a.value))) pbs_wkr_ppn = int.Parse(args2.FirstOrDefault(a => a.key == nameof(pbs_wkr_ppn)).value, CultureInfo.InvariantCulture);
            if (args2.Any(a => a.key == nameof(pbs_wkr_stderr_filename) && !string.IsNullOrWhiteSpace(a.value))) pbs_wkr_stderr_filename = args2.FirstOrDefault(a => a.key == nameof(pbs_wkr_stderr_filename)).value;
            if (args2.Any(a => a.key == nameof(pbs_wkr_stdout_filename) && !string.IsNullOrWhiteSpace(a.value))) pbs_wkr_stdout_filename = args2.FirstOrDefault(a => a.key == nameof(pbs_wkr_stdout_filename)).value;
            if (args2.Any(a => a.key == nameof(pbs_wkr_submission_directory) && !string.IsNullOrWhiteSpace(a.value))) pbs_wkr_submission_directory = args2.FirstOrDefault(a => a.key == nameof(pbs_wkr_submission_directory)).value;
            if (args2.Any(a => a.key == nameof(pbs_wkr_walltime) && !string.IsNullOrWhiteSpace(a.value))) pbs_wkr_walltime = args2.FirstOrDefault(a => a.key == nameof(pbs_wkr_walltime)).value;
            if (args2.Any(a => a.key == nameof(perspective) && !string.IsNullOrWhiteSpace(a.value))) perspective = args2.FirstOrDefault(a => a.key == nameof(perspective)).value;
            if (args2.Any(a => a.key == nameof(positive_class_id) && !string.IsNullOrWhiteSpace(a.value))) positive_class_id = int.Parse(args2.FirstOrDefault(a => a.key == nameof(positive_class_id)).value, CultureInfo.InvariantCulture);
            if (args2.Any(a => a.key == nameof(program_ctl_stderr_filename) && !string.IsNullOrWhiteSpace(a.value))) program_ctl_stderr_filename = args2.FirstOrDefault(a => a.key == nameof(program_ctl_stderr_filename)).value;
            if (args2.Any(a => a.key == nameof(program_ctl_stdout_filename) && !string.IsNullOrWhiteSpace(a.value))) program_ctl_stdout_filename = args2.FirstOrDefault(a => a.key == nameof(program_ctl_stdout_filename)).value;
            if (args2.Any(a => a.key == nameof(program_runtime) && !string.IsNullOrWhiteSpace(a.value))) program_runtime = args2.FirstOrDefault(a => a.key == nameof(program_runtime)).value;
            if (args2.Any(a => a.key == nameof(program_wkr_stderr_filename) && !string.IsNullOrWhiteSpace(a.value))) program_wkr_stderr_filename = args2.FirstOrDefault(a => a.key == nameof(program_wkr_stderr_filename)).value;
            if (args2.Any(a => a.key == nameof(program_wkr_stdout_filename) && !string.IsNullOrWhiteSpace(a.value))) program_wkr_stdout_filename = args2.FirstOrDefault(a => a.key == nameof(program_wkr_stdout_filename)).value;
            if (args2.Any(a => a.key == nameof(randomisation_cv_folds) && !string.IsNullOrWhiteSpace(a.value))) randomisation_cv_folds = int.Parse(args2.FirstOrDefault(a => a.key == nameof(randomisation_cv_folds)).value, CultureInfo.InvariantCulture);
            if (args2.Any(a => a.key == nameof(randomisation_cv_index) && !string.IsNullOrWhiteSpace(a.value))) randomisation_cv_index = int.Parse(args2.FirstOrDefault(a => a.key == nameof(randomisation_cv_index)).value, CultureInfo.InvariantCulture);
            if (args2.Any(a => a.key == nameof(results_root_folder) && !string.IsNullOrWhiteSpace(a.value))) results_root_folder = args2.FirstOrDefault(a => a.key == nameof(results_root_folder)).value;
            if (args2.Any(a => a.key == nameof(save_test_id) && !string.IsNullOrWhiteSpace(a.value))) save_test_id = bool.Parse(args2.FirstOrDefault(a => a.key == nameof(save_test_id)).value);
            if (args2.Any(a => a.key == nameof(save_test_meta) && !string.IsNullOrWhiteSpace(a.value))) save_test_meta = bool.Parse(args2.FirstOrDefault(a => a.key == nameof(save_test_meta)).value);
            if (args2.Any(a => a.key == nameof(save_train_id) && !string.IsNullOrWhiteSpace(a.value))) save_train_id = bool.Parse(args2.FirstOrDefault(a => a.key == nameof(save_train_id)).value);
            if (args2.Any(a => a.key == nameof(save_train_meta) && !string.IsNullOrWhiteSpace(a.value))) save_train_meta = bool.Parse(args2.FirstOrDefault(a => a.key == nameof(save_train_meta)).value);
            if (args2.Any(a => a.key == nameof(scale_function) && !string.IsNullOrWhiteSpace(a.value))) scale_function = (common.scale_function)Enum.Parse(typeof(common.scale_function), args2.FirstOrDefault(a => a.key == nameof(scale_function)).value);
            if (args2.Any(a => a.key == nameof(source) && !string.IsNullOrWhiteSpace(a.value))) source = args2.FirstOrDefault(a => a.key == nameof(source)).value;
            if (args2.Any(a => a.key == nameof(svm_kernel) && !string.IsNullOrWhiteSpace(a.value))) svm_kernel = (common.libsvm_kernel_type)Enum.Parse(typeof(common.libsvm_kernel_type), args2.FirstOrDefault(a => a.key == nameof(svm_kernel)).value);
            if (args2.Any(a => a.key == nameof(svm_type) && !string.IsNullOrWhiteSpace(a.value))) svm_type = (common.libsvm_svm_type)Enum.Parse(typeof(common.libsvm_svm_type), args2.FirstOrDefault(a => a.key == nameof(svm_type)).value);
            if (args2.Any(a => a.key == nameof(test_filename) && !string.IsNullOrWhiteSpace(a.value))) test_filename = args2.FirstOrDefault(a => a.key == nameof(test_filename)).value;
            if (args2.Any(a => a.key == nameof(test_labels_filename) && !string.IsNullOrWhiteSpace(a.value))) test_labels_filename = args2.FirstOrDefault(a => a.key == nameof(test_labels_filename)).value;
            if (args2.Any(a => a.key == nameof(test_id_filename) && !string.IsNullOrWhiteSpace(a.value))) test_id_filename = args2.FirstOrDefault(a => a.key == nameof(test_id_filename)).value;
            if (args2.Any(a => a.key == nameof(test_meta_filename) && !string.IsNullOrWhiteSpace(a.value))) test_meta_filename = args2.FirstOrDefault(a => a.key == nameof(test_meta_filename)).value;
            if (args2.Any(a => a.key == nameof(test_predict_cm_filename) && !string.IsNullOrWhiteSpace(a.value))) test_predict_cm_filename = args2.FirstOrDefault(a => a.key == nameof(test_predict_cm_filename)).value;
            if (args2.Any(a => a.key == nameof(test_predict_filename) && !string.IsNullOrWhiteSpace(a.value))) test_predict_filename = args2.FirstOrDefault(a => a.key == nameof(test_predict_filename)).value;
            if (args2.Any(a => a.key == nameof(train_filename) && !string.IsNullOrWhiteSpace(a.value))) train_filename = args2.FirstOrDefault(a => a.key == nameof(train_filename)).value;
            if (args2.Any(a => a.key == nameof(train_grid_filename) && !string.IsNullOrWhiteSpace(a.value))) train_grid_filename = args2.FirstOrDefault(a => a.key == nameof(train_grid_filename)).value;
            if (args2.Any(a => a.key == nameof(train_id_filename) && !string.IsNullOrWhiteSpace(a.value))) train_id_filename = args2.FirstOrDefault(a => a.key == nameof(train_id_filename)).value;
            if (args2.Any(a => a.key == nameof(train_meta_filename) && !string.IsNullOrWhiteSpace(a.value))) train_meta_filename = args2.FirstOrDefault(a => a.key == nameof(train_meta_filename)).value;
            if (args2.Any(a => a.key == nameof(train_model_filename) && !string.IsNullOrWhiteSpace(a.value))) train_model_filename = args2.FirstOrDefault(a => a.key == nameof(train_model_filename)).value;
            if (args2.Any(a => a.key == nameof(train_predict_cm_filename) && !string.IsNullOrWhiteSpace(a.value))) train_predict_cm_filename = args2.FirstOrDefault(a => a.key == nameof(train_predict_cm_filename)).value;
            if (args2.Any(a => a.key == nameof(train_predict_filename) && !string.IsNullOrWhiteSpace(a.value))) train_predict_filename = args2.FirstOrDefault(a => a.key == nameof(train_predict_filename)).value;


            // fix any missing params
            if (!string.IsNullOrWhiteSpace(train_filename) && string.IsNullOrWhiteSpace(train_model_filename))
            {
                train_model_filename = $@"{train_filename}.model";
            }

            if (string.IsNullOrWhiteSpace(experiment_name))
            {
                throw new ArgumentNullException(nameof(experiment_name));
            }

            convert_paths();
        }

        internal string[] get_options_ini_text()
        {
            var x = get_options();

            var result = x.Select(a => $@"{a.key}={a.value}").ToArray();

            return result;
        }

        internal static List<string> csv_header = new List<string>()
            {

//nameof(probability_estimates),
//nameof(shrinking_heuristics),
nameof(alphabet),
nameof(category),
nameof(class_names),
nameof(class_sizes),
nameof(class_testing_sizes),
nameof(class_training_sizes),
nameof(class_weights),
nameof(cmd),
nameof(dataset_dir),
nameof(dimension),
nameof(experiment_name),
nameof(feature_id),
nameof(feature_selection_classes),
nameof(feature_selection_metrics),
nameof(forward),
nameof(grid_coef0_exp_begin),
nameof(grid_coef0_exp_end),
nameof(grid_coef0_exp_step),
nameof(grid_cost_exp_begin),
nameof(grid_cost_exp_end),
nameof(grid_cost_exp_step),
nameof(grid_degree_exp_begin),
nameof(grid_degree_exp_end),
nameof(grid_degree_exp_step),
nameof(grid_epsilon_exp_begin),
nameof(grid_epsilon_exp_end),
nameof(grid_epsilon_exp_step),
nameof(grid_gamma_exp_begin),
nameof(grid_gamma_exp_end),
nameof(grid_gamma_exp_step),
nameof(group),
nameof(group_features),
nameof(group_index),
nameof(inner_cv_folds),
nameof(iteration),
nameof(job_id),
nameof(libsvm_grid_max_time),
nameof(libsvm_grid_memory_limit_mb),
nameof(libsvm_grid_probability_estimates),
nameof(libsvm_grid_quiet_mode),
nameof(libsvm_grid_shrinking_heuristics),
nameof(libsvm_predict_runtime),
nameof(libsvm_train_max_time),
nameof(libsvm_train_memory_limit_mb),
nameof(libsvm_train_probability_estimates),
nameof(libsvm_train_quiet_mode),
nameof(libsvm_train_runtime),
nameof(libsvm_train_shrinking_heuristics),
nameof(member),
nameof(negative_class_id),
nameof(new_feature_count),
nameof(new_group_count),
nameof(old_feature_count),
nameof(old_group_count),
nameof(options_filename),
nameof(outer_cv_folds),
nameof(outer_cv_index),
nameof(output_threshold_adjustment_performance),
nameof(pbs_ctl_execution_directory),
nameof(pbs_ctl_jobname),
nameof(pbs_ctl_mail_addr),
nameof(pbs_ctl_mail_opt),
nameof(pbs_ctl_mem),
nameof(pbs_ctl_nodes),
nameof(pbs_ctl_ppn),
nameof(pbs_ctl_stderr_filename),
nameof(pbs_ctl_stdout_filename),
nameof(pbs_ctl_submission_directory),
nameof(pbs_ctl_walltime),
nameof(pbs_wkr_execution_directory),
nameof(pbs_wkr_jobname),
nameof(pbs_wkr_mail_addr),
nameof(pbs_wkr_mail_opt),
nameof(pbs_wkr_mem),
nameof(pbs_wkr_nodes),
nameof(pbs_wkr_ppn),
nameof(pbs_wkr_stderr_filename),
nameof(pbs_wkr_stdout_filename),
nameof(pbs_wkr_submission_directory),
nameof(pbs_wkr_walltime),
nameof(perspective),
nameof(positive_class_id),
nameof(program_ctl_stderr_filename),
nameof(program_ctl_stdout_filename),
nameof(program_runtime),
nameof(program_wkr_stderr_filename),
nameof(program_wkr_stdout_filename),
nameof(randomisation_cv_folds),
nameof(randomisation_cv_index),
nameof(results_root_folder),
nameof(save_test_id),
nameof(save_test_meta),
nameof(save_train_id),
nameof(save_train_meta),
nameof(scale_function),
nameof(source),
nameof(svm_kernel),
nameof(svm_type),
nameof(test_filename),
nameof(test_labels_filename),
nameof(test_id_filename),
nameof(test_meta_filename),
nameof(test_predict_cm_filename),
nameof(test_predict_filename),
nameof(train_filename),
nameof(train_grid_filename),
nameof(train_id_filename),
nameof(train_meta_filename),
nameof(train_model_filename),
nameof(train_predict_cm_filename),
nameof(train_predict_filename),

            };



        internal List<(string key, string value)> get_options()
        {
            var result = new List<(string key, string value)>()
            {
(nameof(alphabet), alphabet),
(nameof(category), category),
(nameof(class_names), string.Join(';', class_names == null ? new List<string>() : class_names?.Select(a => $"{a.class_id}:{a.class_name}").ToList())),
(nameof(class_sizes), string.Join(';', class_sizes == null ? new List<string>() : class_sizes?.Select(a => $"{a.class_id}:{a.class_size}").ToList())),
(nameof(class_testing_sizes), string.Join(';', class_testing_sizes == null ? new List<string>() : class_testing_sizes?.Select(a => $"{a.class_id}:{a.class_testing_size}").ToList())),
(nameof(class_training_sizes), string.Join(';', class_training_sizes == null ? new List<string>() : class_training_sizes?.Select(a => $"{a.class_id}:{a.class_training_size}").ToList())),
(nameof(class_weights), string.Join(';', class_weights == null ? new List<string>() : class_weights?.Select(a => $"{a.class_id}:{a.class_weight}").ToList())),
(nameof(cmd), cmd.ToString()),
(nameof(dataset_dir), dataset_dir),
(nameof(dimension), dimension),
(nameof(experiment_name), experiment_name),
(nameof(feature_id), feature_id.ToString(CultureInfo.InvariantCulture)),
(nameof(feature_selection_classes), string.Join(";", feature_selection_classes == null ? new List<int>() : feature_selection_classes)),
(nameof(feature_selection_metrics), string.Join(";", feature_selection_metrics == null ? new List<string>() : feature_selection_metrics)),
(nameof(forward), forward.ToString(CultureInfo.InvariantCulture)),
(nameof(grid_coef0_exp_begin), grid_coef0_exp_begin?.ToString("G17",CultureInfo.InvariantCulture)),
(nameof(grid_coef0_exp_end), grid_coef0_exp_end?.ToString("G17",CultureInfo.InvariantCulture)),
(nameof(grid_coef0_exp_step), grid_coef0_exp_step?.ToString("G17",CultureInfo.InvariantCulture)),
(nameof(grid_cost_exp_begin), grid_cost_exp_begin?.ToString("G17",CultureInfo.InvariantCulture)),
(nameof(grid_cost_exp_end), grid_cost_exp_end?.ToString("G17",CultureInfo.InvariantCulture)),
(nameof(grid_cost_exp_step), grid_cost_exp_step?.ToString("G17",CultureInfo.InvariantCulture)),
(nameof(grid_degree_exp_begin), grid_degree_exp_begin?.ToString("G17",CultureInfo.InvariantCulture)),
(nameof(grid_degree_exp_end), grid_degree_exp_end?.ToString("G17",CultureInfo.InvariantCulture)),
(nameof(grid_degree_exp_step), grid_degree_exp_step?.ToString("G17",CultureInfo.InvariantCulture)),
(nameof(grid_epsilon_exp_begin), grid_epsilon_exp_begin?.ToString("G17",CultureInfo.InvariantCulture)),
(nameof(grid_epsilon_exp_end), grid_epsilon_exp_end?.ToString("G17",CultureInfo.InvariantCulture)),
(nameof(grid_epsilon_exp_step), grid_epsilon_exp_step?.ToString("G17",CultureInfo.InvariantCulture)),
(nameof(grid_gamma_exp_begin), grid_gamma_exp_begin?.ToString("G17",CultureInfo.InvariantCulture)),
(nameof(grid_gamma_exp_end), grid_gamma_exp_end?.ToString("G17",CultureInfo.InvariantCulture)),
(nameof(grid_gamma_exp_step), grid_gamma_exp_step?.ToString("G17",CultureInfo.InvariantCulture)),
(nameof(group), group),
(nameof(group_features), group_features.ToString(CultureInfo.InvariantCulture)),
(nameof(group_index), group_index.ToString(CultureInfo.InvariantCulture)),
(nameof(inner_cv_folds), inner_cv_folds.ToString(CultureInfo.InvariantCulture)),
(nameof(iteration), iteration.ToString(CultureInfo.InvariantCulture)),
(nameof(job_id), job_id.ToString(CultureInfo.InvariantCulture)),
(nameof(libsvm_grid_max_time), libsvm_grid_max_time.ToString()),
(nameof(libsvm_grid_memory_limit_mb), libsvm_grid_memory_limit_mb.ToString(CultureInfo.InvariantCulture)),
(nameof(libsvm_grid_probability_estimates), libsvm_grid_probability_estimates.ToString(CultureInfo.InvariantCulture)),
(nameof(libsvm_grid_quiet_mode), libsvm_grid_quiet_mode.ToString(CultureInfo.InvariantCulture)),
(nameof(libsvm_grid_shrinking_heuristics), libsvm_grid_shrinking_heuristics.ToString(CultureInfo.InvariantCulture)),
(nameof(libsvm_predict_runtime), libsvm_predict_runtime),
(nameof(libsvm_train_max_time), libsvm_train_max_time.ToString()),
(nameof(libsvm_train_memory_limit_mb), libsvm_train_memory_limit_mb.ToString(CultureInfo.InvariantCulture)),
(nameof(libsvm_train_probability_estimates), libsvm_train_probability_estimates.ToString(CultureInfo.InvariantCulture)),
(nameof(libsvm_train_quiet_mode), libsvm_train_quiet_mode.ToString(CultureInfo.InvariantCulture)),
(nameof(libsvm_train_runtime), libsvm_train_runtime),
(nameof(libsvm_train_shrinking_heuristics), libsvm_train_shrinking_heuristics.ToString(CultureInfo.InvariantCulture)),
(nameof(member), member),
(nameof(negative_class_id), negative_class_id.ToString(CultureInfo.InvariantCulture)),
(nameof(new_feature_count), new_feature_count.ToString(CultureInfo.InvariantCulture)),
(nameof(new_group_count), new_group_count.ToString(CultureInfo.InvariantCulture)),
(nameof(old_feature_count), old_feature_count.ToString(CultureInfo.InvariantCulture)),
(nameof(old_group_count), old_group_count.ToString(CultureInfo.InvariantCulture)),
(nameof(options_filename), options_filename),
(nameof(outer_cv_folds), outer_cv_folds.ToString(CultureInfo.InvariantCulture)),
(nameof(outer_cv_index), outer_cv_index.ToString(CultureInfo.InvariantCulture)),
(nameof(output_threshold_adjustment_performance), output_threshold_adjustment_performance.ToString(CultureInfo.InvariantCulture)),
(nameof(pbs_ctl_execution_directory),      pbs_ctl_execution_directory  ),
(nameof(pbs_ctl_jobname),      pbs_ctl_jobname  ),
(nameof(pbs_ctl_mail_addr),     pbs_ctl_mail_addr   ),
(nameof(pbs_ctl_mail_opt),     pbs_ctl_mail_opt   ),
(nameof(pbs_ctl_mem),          pbs_ctl_mem   ),
(nameof(pbs_ctl_nodes),        pbs_ctl_nodes.ToString(CultureInfo.InvariantCulture)   ),
(nameof(pbs_ctl_ppn),          pbs_ctl_ppn.ToString(CultureInfo.InvariantCulture)   ),
(nameof(pbs_ctl_stderr_filename),          pbs_ctl_stderr_filename   ),
(nameof(pbs_ctl_stdout_filename),          pbs_ctl_stdout_filename  ),
(nameof(pbs_ctl_submission_directory),     pbs_ctl_submission_directory   ),
(nameof(pbs_ctl_walltime),     pbs_ctl_walltime   ),
(nameof(pbs_wkr_execution_directory),      pbs_wkr_execution_directory  ),
(nameof(pbs_wkr_jobname),      pbs_wkr_jobname  ),
(nameof(pbs_wkr_mail_addr),     pbs_wkr_mail_addr   ),
(nameof(pbs_wkr_mail_opt),     pbs_wkr_mail_opt   ),
(nameof(pbs_wkr_mem),          pbs_wkr_mem   ),
(nameof(pbs_wkr_nodes),        pbs_wkr_nodes.ToString(CultureInfo.InvariantCulture)   ),
(nameof(pbs_wkr_ppn),          pbs_wkr_ppn.ToString(CultureInfo.InvariantCulture)   ),
(nameof(pbs_wkr_stderr_filename),          pbs_wkr_stderr_filename   ),
(nameof(pbs_wkr_stdout_filename),          pbs_wkr_stdout_filename  ),
(nameof(pbs_wkr_submission_directory),     pbs_wkr_submission_directory   ),
(nameof(pbs_wkr_walltime),     pbs_wkr_walltime   ),
(nameof(perspective), perspective),
(nameof(positive_class_id), positive_class_id.ToString(CultureInfo.InvariantCulture)),
(nameof(program_ctl_stderr_filename),          program_ctl_stderr_filename   ),
(nameof(program_ctl_stdout_filename),          program_ctl_stdout_filename  ),
(nameof(program_runtime), program_runtime),
(nameof(program_wkr_stderr_filename),          program_wkr_stderr_filename   ),
(nameof(program_wkr_stdout_filename),          program_wkr_stdout_filename  ),
(nameof(randomisation_cv_folds), randomisation_cv_folds.ToString(CultureInfo.InvariantCulture)),
(nameof(randomisation_cv_index), randomisation_cv_index.ToString(CultureInfo.InvariantCulture)),
(nameof(results_root_folder), results_root_folder),
(nameof(save_test_id), save_test_id.ToString(CultureInfo.InvariantCulture)),
(nameof(save_test_meta), save_test_meta.ToString(CultureInfo.InvariantCulture)),
(nameof(save_train_id), save_train_id.ToString(CultureInfo.InvariantCulture)),
(nameof(save_train_meta), save_train_meta.ToString(CultureInfo.InvariantCulture)),
(nameof(scale_function), scale_function.ToString()),
(nameof(source), source),
(nameof(svm_kernel), svm_kernel.ToString()),
(nameof(svm_type), svm_type.ToString()),
(nameof(test_filename), test_filename),
(nameof(test_labels_filename), test_labels_filename),
(nameof(test_id_filename), test_id_filename),
(nameof(test_meta_filename), test_meta_filename),
(nameof(test_predict_cm_filename), test_predict_cm_filename),
(nameof(test_predict_filename), test_predict_filename),
(nameof(train_filename), train_filename),
(nameof(train_grid_filename), train_grid_filename),
(nameof(train_id_filename), train_id_filename),
(nameof(train_meta_filename), train_meta_filename),
(nameof(train_model_filename), train_model_filename),
(nameof(train_predict_cm_filename), train_predict_cm_filename),
(nameof(train_predict_filename), train_predict_filename),
//(nameof(probability_estimates), probability_estimates.ToString()),
//(nameof(shrinking_heuristics), shrinking_heuristics.ToString()),


            };

            //return string.Join(" ", list.Select(a => $"-{a.key} {a.value}").ToList());

            //result = result.OrderBy(a => a.key).ThenBy(a => a.value).ToList(); // do not reorder these items because it breaks the order of the header

            return result;
        }
    }
}
