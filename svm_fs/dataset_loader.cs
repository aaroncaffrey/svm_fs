using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;

namespace svm_fs
{
    public static class dataset_loader
    {
//        [Serializable]
        public class dataset
        {
            public List<(int fid, string alphabet, string dimension, string category, string source, string group, string member, string perspective, int alphabet_id, int dimension_id, int category_id, int source_id, int group_id, int member_id, int perspective_id)> dataset_headers = null;
            public List<(int filename_index, int line_index, List<(string comment_header, string comment_value)> comment_columns, string comment_columns_hash)> dataset_comment_row_values = null;
            public List<(int class_id, int example_id, int class_example_id, List<(string comment_header, string comment_value)> comment_columns, string comment_columns_hash, List<(int fid, double fv)> feature_data, string feature_data_hash)> dataset_instance_list = null;

            //public static void serialise(datax datax, string filename)
            //{
            //    IFormatter formatter = new BinaryFormatter();
            //    Stream stream = new FileStream(filename, FileMode.Create, FileAccess.Write, FileShare.None);
            //    formatter.Serialize(stream, datax);
            //    stream.Close();
            //}

            //public void serialise(string filename)
            //{
            //    datax.serialise(this, filename);
            //}

            //public static datax deserialise(string filename)
            //{
            //    IFormatter formatter = new BinaryFormatter();
            //    Stream stream = new FileStream(filename, FileMode.Open, FileAccess.Read, FileShare.Read);
            //    datax datax = (datax)formatter.Deserialize(stream);
            //    stream.Close();
            //    return datax;
            //}
        }

        public static bool matches(string text, string search_pattern)
        {
            if (string.IsNullOrWhiteSpace(search_pattern) || search_pattern == "*")
            {
                return true;
            }
            else if (search_pattern.StartsWith("*") && search_pattern.EndsWith("*"))
            {
                search_pattern = search_pattern.Substring(1, search_pattern.Length - 2);

                return text.Contains(search_pattern, StringComparison.InvariantCultureIgnoreCase);
            }
            else if (search_pattern.StartsWith("*"))
            {
                search_pattern = search_pattern.Substring(1);

                return text.EndsWith(search_pattern, StringComparison.InvariantCultureIgnoreCase);
            }
            else if (search_pattern.EndsWith("*"))
            {
                search_pattern = search_pattern.Substring(0, search_pattern.Length - 1);

                return text.StartsWith(search_pattern, StringComparison.InvariantCultureIgnoreCase);
            }
            else
            {
                return string.Equals(text, search_pattern, StringComparison.InvariantCultureIgnoreCase);
            }
        }

        public static
            dataset
           read_binary_dataset(

           string dataset_folder,

           int negative_class_id,
           int positive_class_id,
           List<(int class_id, string class_name)> class_names,
           bool use_parallel = true,
           bool perform_integrity_checks = false,
           bool fix_double = true,
           List<(bool required, string alphabet, string dimension, string category, string source, string group, string member, string perspective)> required_matches = null
           )
        {

            var lock_table = new object();

            var table_alphabet = new List<string>();
            var table_dimension = new List<string>();
            var table_category = new List<string>();
            var table_source = new List<string>();
            var table_group = new List<string>();
            var table_member = new List<string>();
            var table_perspective = new List<string>();

            dataset_folder = convert_path(dataset_folder);

            var dataset_csv_files = new List<string>()
            {
                convert_path(Path.Combine(dataset_folder, $@"f__[{class_names.First(a => a.class_id == positive_class_id).class_name}].csv")),
                convert_path(Path.Combine(dataset_folder, $@"f__[{class_names.First(a => a.class_id == negative_class_id).class_name}].csv")),
            };

            var dataset_header_csv_files = new List<string>()
            {
                convert_path(Path.Combine(dataset_folder, $@"h__[{class_names.First(a => a.class_id == positive_class_id).class_name}].csv")),
                convert_path(Path.Combine(dataset_folder, $@"h__[{class_names.First(a => a.class_id == negative_class_id).class_name}].csv")),
            };

            var dataset_comment_csv_files = new List<string>
            {
                convert_path(Path.Combine(dataset_folder, $@"c__[{class_names.First(a => a.class_id == positive_class_id).class_name}].csv")),
                convert_path(Path.Combine(dataset_folder, $@"c__[{class_names.First(a => a.class_id == negative_class_id).class_name}].csv")),
            };


            svm_ctl.WriteLine($@"{nameof(negative_class_id)} = {negative_class_id}", nameof(dataset_loader), nameof(read_binary_dataset));
            svm_ctl.WriteLine($@"{nameof(positive_class_id)} = {positive_class_id}", nameof(dataset_loader), nameof(read_binary_dataset));
            svm_ctl.WriteLine($@"{nameof(class_names)} = {string.Join(", ", class_names)}", nameof(dataset_loader), nameof(read_binary_dataset));

            svm_ctl.WriteLine($@"{nameof(dataset_csv_files)}: {string.Join(", ", dataset_csv_files)}", nameof(dataset_loader), nameof(read_binary_dataset));
            svm_ctl.WriteLine($@"{nameof(dataset_header_csv_files)}: {string.Join(", ", dataset_header_csv_files)}", nameof(dataset_loader), nameof(read_binary_dataset));
            svm_ctl.WriteLine($@"{nameof(dataset_comment_csv_files)}: {string.Join(", ", dataset_comment_csv_files)}", nameof(dataset_loader), nameof(read_binary_dataset));

            svm_ctl.WriteLine($@"Reading non-novel dataset headers...", nameof(dataset_loader), nameof(read_binary_dataset));

            // READ HEADER CSV FILE - ALL CLASSES HAVE THE SAME HEADERS/FEATURES

            //List<(int fid, string alphabet, string dimension, string category, string source, string group, string member, string perspective, int alphabet_id, int dimension_id, int category_id, int source_id, int group_id, int member_id, int perspective_id)> header_data = File.ReadAllLines(dataset_header_csv_files.First()).Skip(1).Select((a, i) =>



            var header_data = File.ReadAllLines(dataset_header_csv_files.First()).Skip(1)/*.AsParallel().AsOrdered()*/.Select((a, i) =>
            {
                var b = a.Split(',');
                var fid = int.Parse(b[0]);
                //if (fid!=i) throw new Exception();

                var alphabet = b[1];
                var dimension = b[2];
                var category = b[3];
                var source = b[4];
                var group = b[5];
                var member = b[6];
                var perspective = b[7];

                const string def = "default";

                if (string.IsNullOrWhiteSpace(alphabet)) alphabet = def;
                if (string.IsNullOrWhiteSpace(dimension)) dimension = def;
                if (string.IsNullOrWhiteSpace(category)) category = def;
                if (string.IsNullOrWhiteSpace(source)) source = def;
                if (string.IsNullOrWhiteSpace(group)) group = def;
                if (string.IsNullOrWhiteSpace(member)) member = def;
                if (string.IsNullOrWhiteSpace(perspective)) perspective = def;

                lock (lock_table)
                {
                    var duplicate = true;
                    if (!table_alphabet.Contains(alphabet)) { table_alphabet.Add(alphabet); duplicate = false; }
                    if (!table_dimension.Contains(dimension)) { table_dimension.Add(dimension); duplicate = false; }
                    if (!table_category.Contains(category)) { table_category.Add(category); duplicate = false; }
                    if (!table_source.Contains(source)) { table_source.Add(source); duplicate = false; }
                    if (!table_group.Contains(group)) { table_group.Add(group); duplicate = false; }
                    if (!table_member.Contains(member)) { table_member.Add(member); duplicate = false; }
                    if (!table_perspective.Contains(perspective)) { table_perspective.Add(perspective); duplicate = false; }

                    if (duplicate)
                    {
                        //svm_ctl.WriteLine("Duplicate: " + a);
                        //Console.ReadLine();
                    }
                }


                var alphabet_id = table_alphabet.LastIndexOf(alphabet);
                var dimension_id = table_dimension.LastIndexOf(dimension);
                var category_id = table_category.LastIndexOf(category);
                var source_id = table_source.LastIndexOf(source);
                var group_id = table_group.LastIndexOf(group);
                var member_id = table_member.LastIndexOf(member);
                var perspective_id = table_perspective.LastIndexOf(perspective);

                alphabet = table_alphabet[alphabet_id];
                dimension = table_dimension[dimension_id];
                category = table_category[category_id];
                source = table_source[source_id];
                group = table_group[group_id];
                member = table_member[member_id];
                perspective = table_perspective[perspective_id];

                return (fid: fid, alphabet: alphabet, dimension: dimension, category: category, source: source, group: group, member: member, perspective: perspective,
                                    alphabet_id: alphabet_id, dimension_id: dimension_id, category_id: category_id, source_id: source_id, group_id: group_id, member_id: member_id, perspective_id: perspective_id);
            }).ToList();

            bool[] required = null;

            if (required_matches != null && required_matches.Count > 0)
            {
                required = new bool[header_data.Count];
                Array.Fill(required, true);

                for (var index = 0; index < required_matches.Count; index++)
                {
                    var rm = required_matches[index];

                    var matching_fids = header_data.Where(a => matches(a.alphabet, rm.alphabet) && matches(a.category, rm.category) && matches(a.dimension, rm.dimension) && matches(a.source, rm.source) && matches(a.@group, rm.@group) && matches(a.member, rm.member) && matches(a.perspective, rm.perspective)).Select(a => a.fid).ToList();

                    
                    matching_fids.ForEach(a => required[a] = rm.required);
                    
                }

                required[0] = true;

                header_data = header_data.Where((a, i) => required[a.fid]).ToList();
            }

            

            // READ (DATA) COMMENTS CSV FILE - THESE ARE CLASS AND INSTANCE SPECIFIC
            var data_comments_header = File.ReadLines(dataset_comment_csv_files.First()).First().Split(',');

            //var total_features = header_data.Count;
            //Program.WriteLine($@"{nameof(total_features)}: {total_features}");
            //Program.WriteLine($@"{nameof(table_alphabet)}: {table_alphabet.Count}: {string.Join(", ", table_alphabet)}");
            //Program.WriteLine($@"{nameof(table_dimension)}: {table_dimension.Count}: {string.Join(", ", table_dimension)}");
            //Program.WriteLine($@"{nameof(table_category)}: {table_category.Count}: {string.Join(", ", table_category)}");
            //Program.WriteLine($@"{nameof(table_source)}: {table_source.Count}: {string.Join(", ", table_source)}");
            //Program.WriteLine($@"{nameof(table_group)}: {table_group.Count}: {string.Join(", ", table_group)}");
            //Program.WriteLine($@"{nameof(table_member)}: {table_member.Count}: {string.Join(", ", table_member)}");
            //Program.WriteLine($@"{nameof(table_perspective)}: {table_perspective.Count}: {string.Join(", ", table_perspective)}");
            //Program.WriteLine($@"");

            List<(int filename_index, int line_index, List<(string comment_header, string comment_value)> comment_columns, string comment_columns_hash)> dataset_comment_row_values = null;
            List<(int class_id, int example_id, int class_example_id, List<(string comment_header, string comment_value)> comment_columns, string comment_columns_hash, List<(int fid, double fv)> feature_data, string feature_data_hash)> dataset_instance_list = null;




            // data comments: load comment lines as key-value pairs.  note: these are variables associated with each example instance rather than specific features.
            svm_ctl.WriteLine($@"Reading data comments...", nameof(dataset_loader), nameof(read_binary_dataset));

            if (use_parallel)
            {
                dataset_comment_row_values = dataset_comment_csv_files.AsParallel().AsOrdered().SelectMany((filename, filename_index) => File.ReadAllLines(filename).Skip(1/*header line*/).AsParallel().AsOrdered().Select((line, line_index) => {

                    var comment_columns = line.Split(',').Select((col, col_index) => (comment_header: data_comments_header[col_index], comment_value: col)).ToList();
                    comment_columns = comment_columns.Where(d => d.comment_header.FirstOrDefault() != '#').ToList();

                    var comment_columns_hash = hash.calc_hash(string.Join(" ", comment_columns.Select(c => c.comment_header + ":" + c.comment_value).ToList()));
                    return (filename_index: filename_index, line_index: line_index, comment_columns: comment_columns, comment_columns_hash: comment_columns_hash);

                }).ToList()).ToList();
            }
            else
            {
                dataset_comment_row_values = dataset_comment_csv_files.SelectMany((filename, filename_index) => File.ReadAllLines(filename).Skip(1/*header line*/).Select((line, line_index) => {

                    var comment_columns = line.Split(',').Select((col, col_index) => (comment_header: data_comments_header[col_index], comment_value: col)).ToList();
                    comment_columns = comment_columns.Where(d => d.comment_header.FirstOrDefault() != '#').ToList();

                    var comment_columns_hash = hash.calc_hash(string.Join(" ", comment_columns.Select(c => c.comment_header + ":" + c.comment_value).ToList()));
                    return (filename_index: filename_index, line_index: line_index, comment_columns: comment_columns, comment_columns_hash: comment_columns_hash);

                }).ToList()).ToList();

            }

            //// data comments: filter out any '#' commented out key-value pairs 
            //svm_manager.WriteLine($@"Removing data comments which are commented out...");
            //if (use_parallel)
            //{
            //    dataset_comment_row_values = dataset_comment_row_values.AsParallel().AsOrdered().Select(a => { a.comment_columns = a.comment_columns.Where(b => b.comment_header.FirstOrDefault() != '#').ToList(); return a; }).ToList();
            //}
            //else
            //{
            //    dataset_comment_row_values = dataset_comment_row_values.Select(a => { a.comment_columns = a.comment_columns.Where(b => b.comment_header.FirstOrDefault() != '#').ToList(); return a; }).ToList();
            //}


            // data set: load data
            svm_ctl.WriteLine($@"Reading data...", nameof(dataset_loader), nameof(read_binary_dataset));
            if (use_parallel)
            {
                dataset_instance_list = dataset_csv_files.AsParallel().AsOrdered().SelectMany((filename, filename_index) => File.ReadAllLines(filename).Skip(1/*skip header*/).AsParallel().AsOrdered().Select((line, line_index) =>
                {
                    //var feature_data = line.Split(',').Select((column_value, fid) => (fid, fv: fix_double ? dataset_loader.fix_double(column_value) : double.Parse(column_value) )).ToList()

                    var class_id = int.Parse(line.Substring(0, line.IndexOf(',')));
                    var feature_data = parse_csv_line_doubles(line, required);
                    var feature_data_hash = hash.calc_hash(string.Join(" ", feature_data.Select(d => $"{d.fid}:{d.value}").ToList()));

                    var comment_row = dataset_comment_row_values.First(b => b.filename_index == filename_index && b.line_index == line_index);
                    var comment_columns = comment_row.comment_columns;
                    var comment_columns_hash = comment_row.comment_columns_hash;

                    return (
                        class_id: class_id,
                        example_id: 0,
                        class_example_id: 0,
                        comment_columns: comment_columns,
                        comment_columns_hash: comment_columns_hash,
                        feature_data: feature_data,
                        feature_data_hash: feature_data_hash
                    );

                }
                ).ToList()).ToList();
            }
            else
            {
                dataset_instance_list = dataset_csv_files.SelectMany((filename, filename_index) => File.ReadAllLines(filename).Skip(1/*skip header*/)./*Take(20).*/Select((line, line_index) =>
                {
                    //var feature_data = line.Split(',').Select((column_value, fid) => (fid, fv: fix_double ? dataset_loader.fix_double(column_value) : double.Parse(column_value) )).ToList()



                    var class_id = int.Parse(line.Substring(0, line.IndexOf(',')));
                    var feature_data = parse_csv_line_doubles(line, required);
                    var feature_data_hash = hash.calc_hash(string.Join(" ", feature_data.Select(d => $"{d.fid}:{d.value}").ToList()));

                    var comment_row = dataset_comment_row_values.First(b => b.filename_index == filename_index && b.line_index == line_index);
                    var comment_columns = comment_row.comment_columns;
                    var comment_columns_hash = comment_row.comment_columns_hash;

                    return (
                        class_id: class_id,
                        example_id: 0,
                        class_example_id: 0,
                        comment_columns: comment_columns,
                        comment_columns_hash: comment_columns_hash,
                        feature_data: feature_data,
                        feature_data_hash: feature_data_hash
                    );

                }
                ).ToList()).ToList();
            }

            if (dataset_comment_row_values.Count != dataset_instance_list.Count)
            {
                throw new Exception();
            }


            if (perform_integrity_checks)
            {
                svm_ctl.WriteLine($@"Checking all dataset columns are the same length...", nameof(dataset_loader), nameof(read_binary_dataset));
                var dataset_num_diferent_column_length = dataset_instance_list.Select(a => a.feature_data.Count).Distinct().Count();
                if (dataset_num_diferent_column_length != 1) throw new Exception();

                svm_ctl.WriteLine($@"Checking dataset headers and dataset columns are the same length...", nameof(dataset_loader), nameof(read_binary_dataset));
                var header_length = header_data.Count;
                var dataset_column_length = dataset_instance_list.First().feature_data.Count;
                if (dataset_column_length != header_length) throw new Exception();

                svm_ctl.WriteLine($@"Checking all dataset comment columns are the same length...", nameof(dataset_loader), nameof(read_binary_dataset));
                var comments_num_different_column_length = dataset_instance_list.Select(a => a.comment_columns.Count).Distinct().Count();
                if (comments_num_different_column_length != 1) throw new Exception();
            }


            dataset_instance_list = dataset_instance_list.Select((a, i) => (a.class_id, i, a.class_example_id, a.comment_columns, a.comment_columns_hash, a.feature_data, a.feature_data_hash)).ToList();
            dataset_instance_list = dataset_instance_list.GroupBy(a => a.class_id).SelectMany(x => x.Select((a, i) => (a.class_id, a.example_id, i, a.comment_columns, a.comment_columns_hash, a.feature_data, a.feature_data_hash)).ToList()).ToList();


            //var dataset_instance_list2 = dataset_instance_list.Select((a, i) => (class_id: a.class_id, comment_columns: a.comment_columns, feature_data: a.feature_data,
            //feature_data_hash: svm_manager.calc_hash(string.Join(" ", a.feature_data.SelectMany(b => new string[] { ""+b.fid, ""+b.fv }).ToList())), example_id: i)).ToList();
            //
            //var dataset_instance_list3 = dataset_instance_list2.GroupBy(a => a.class_id).Select(x => x.Select((a, i) => (class_id: a.class_id, comment_columns: a.comment_columns, feature_data: a.feature_data, 
            //comment_columns_hash: a.comment_columns_hash, feature_data_hash: a.feature_data_hash, example_id: a.example_id, class_example_id: i))).SelectMany(a => a).ToList();


            var datax = new dataset();

            datax.dataset_headers = header_data;
            datax.dataset_comment_row_values = dataset_comment_row_values;
            datax.dataset_instance_list = dataset_instance_list;

            return datax;
        }

        public static List<(int fid, double value)> parse_csv_line_doubles(string line, bool[] required = null)
        {
            var result = new List<(int fid, double value)>();

            if (string.IsNullOrWhiteSpace(line)) return result;

            var fid = 0;

            var start = 0;
            var len = 0;

            for (var i = 0; i <= line.Length; i++)
            {
                if (i == line.Length || line[i] == ',')
                {
                    if ((required == null || required.Length == 0 || fid == 0) || (required != null && required.Length > fid && required[fid]))
                    {
                        result.Add((fid, len == 0 ? 0d : double.Parse(line.Substring(start, len))));
                    }

                    fid++;

                    start = i + 1;
                    len = 0;
                    continue;

                }

                len++;
            }

            return result;
        }

        public static List<string> parse_csv_line_strings(string line)
        {
            var result = new List<string>();

            if (string.IsNullOrWhiteSpace(line)) return result;

            var id = 0;

            var start = 0;
            var len = 0;

            for (var i = 0; i <= line.Length; i++)
            {
                if (i == line.Length || line[i] == ',')
                {

                    result.Add(len == 0 ? "" : line.Substring(start, len));

                    id++;

                    start = i + 1;
                    len = 0;
                    continue;

                }

                len++;
            }

            return result;
        }

        public static string convert_path(string path, bool temp_file = false)
        {
            if (string.IsNullOrWhiteSpace(path))
            {
                return path;
            }

            if (Environment.OSVersion.Platform == PlatformID.Unix || Environment.OSVersion.Platform == PlatformID.MacOSX)
            {
                // convert windows path to linux
                
                if (path.Length >= 2 && char.IsLetter(path[0]) && path[1] == ':')
                {
                    path = '~' + path.Substring(2);
                }
            }
            else if (Environment.OSVersion.Platform == PlatformID.Win32NT)
            {
                // convert linux path to windows

                if ((path.Length == 1 && path[0] == '~') || (path.Length > 1 && path[0] == '~' && (path[1] == '\\' || path[1] == '/')))
                {
                    //var ad = Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData);
                    //var up = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
                    var md = Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments);

                    var p = path.Substring(path.Length > 1 && (path[1] == '\\' || path[1] == '/') ? 2 : 1);

                    if (!string.IsNullOrWhiteSpace(p))
                    {
                        path = Path.Combine(md, p);
                    }
                    else
                    {
                        path = md;
                    }
                }
                else if (path.Length > 0 && (path[0] == '\\' || path[0] == '/') && (path.Length == 1 || (path[1] != '\\' && path[1] != '/')))
                {
                    if (path.StartsWith("/home") || path.StartsWith("\\home")) path = path.Substring("/home".Length);

                    if (path.FirstOrDefault() == '/' || path.FirstOrDefault() == '\\') path = path.Substring(1);

                    var md = Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments);

                    path = Path.Combine(md, path);
                }
                //else if (path.StartsWith("/home"))
                
            }

            if (Path.DirectorySeparatorChar != '\\' && path.Contains('\\'))
            {
                path = path.Replace('\\', Path.DirectorySeparatorChar);
            }

            if (Path.DirectorySeparatorChar != '/' && path.Contains('/'))
            {
                path = path.Replace('/', Path.DirectorySeparatorChar);
            }

            return path;
        }


        public static double fix_double(string double_value)
        {
            const char infinity = '∞';
            const string neg_infinity = "-∞";
            const string pos_infinity = "+∞";
            const string NaN = "NaN";

            if (double.TryParse(double_value, NumberStyles.Float, CultureInfo.InvariantCulture, out var value1)) return fix_double(value1);

            if (double_value.Length == 1 && double_value[0] == infinity) return fix_double(double.PositiveInfinity);
            else if (double_value.Contains(pos_infinity)) return fix_double(double.PositiveInfinity);
            else if (double_value.Contains(neg_infinity)) return fix_double(double.NegativeInfinity);
            else if (double_value.Contains(infinity)) return fix_double(double.PositiveInfinity);
            else if (double_value.Contains(NaN)) return fix_double(double.NaN);
            else return 0d;
        }

        public static double fix_double(double value)
        {
            // the doubles must be compatible with libsvm which is written in C (C and CSharp have different min/max values for double)
            const double c_double_max = (double)1.79769e+308;
            const double c_double_min = (double)-c_double_max;
            const double double_zero = (double)0;

            if (value >= c_double_min && value <= c_double_max)
            {
                return value;
            }
            else if (double.IsPositiveInfinity(value) || value >= c_double_max || value >= double.MaxValue)
            {
                value = c_double_max;
            }
            else if (double.IsNegativeInfinity(value) || value <= c_double_min || value <= double.MinValue)
            {
                value = c_double_min;
            }
            else if (double.IsNaN(value))
            {
                value = double_zero;
            }

            return value;
        }

    }
}
