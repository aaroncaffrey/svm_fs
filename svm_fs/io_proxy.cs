using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;

namespace svm_fs
{
    public static class io_proxy
    {
        public static string fix_path(string filename)
        {
            return convert_path(fix_filename(filename));
        }

        public static string fix_filename(string filename)
        {
            //var path = Path.GetDirectoryName(filename);
            //var file = Path.GetFileName(filename);

            var invalid = $"?%*:|<>\"" + string.Join("", Enumerable.Range(0, 32).Select(a => (char)a).ToList()); // includes \0 \b \t \r \n, leaves /\\ as it is full paths input

            var filename2 = string.Join("", filename.Select(a => invalid.Any(b => a == b) ? '_' : a).ToList());

            return filename2;
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

                if (path.Length > 0 && path[0] == '~')
                {
                    // convert ~ to home directory
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

        private static readonly object _console_lock = new object();

        public static void WriteLine(string text = "", string module_name = "", string function_name = "", bool use_lock = false)
        {
            //if (!program.verbose) return;

            try
            {
                var pid = Process.GetCurrentProcess().Id;
                var thread_id = Thread.CurrentThread.ManagedThreadId;
                var task_id = Task.CurrentId ?? 0;

                if (use_lock)
                {
                    lock (_console_lock)
                    {
                        Console.WriteLine($@"{DateTime.Now:G} {pid:000000}.{thread_id:000000}.{task_id:000000} {module_name}.{function_name} -> {text}");
                    }
                }
                else
                {
                    Console.WriteLine($@"{DateTime.Now:G} {pid:000000}.{thread_id:000000}.{task_id:000000} {module_name}.{function_name} -> {text}");
                }
            }
            catch (Exception)
            {

            }
        }

        public static bool Exists(string filename, string module_name = "", string function_name = "")
        {
            filename=fix_path(filename);
            io_proxy.WriteLine($"{module_name}.{function_name} -> ( {filename} )", nameof(io_proxy), nameof(Exists));

            return File.Exists(filename);
        }
            
        public static void Delete(string filename, string module_name = "", string function_name = "")
        {
            filename = fix_path(filename);

            io_proxy.WriteLine($"{module_name}.{function_name} -> ( {filename} )", nameof(io_proxy), nameof(Delete));

            File.Delete(filename);
        }

        public static void Copy(string source, string dest, bool overwrite = false, string module_name = "", string function_name = "")
        {
            source = fix_path(source);
            dest = fix_path(dest);

            io_proxy.WriteLine($"{module_name}.{function_name} -> ( {source} , {dest} , {overwrite} )", nameof(io_proxy), nameof(Copy));

            File.Copy(source, dest, overwrite);
        }

        public static void CreateDirectory(string filename, string module_name = "", string function_name = "")
        {
            filename = fix_path(filename);

            io_proxy.WriteLine($"{module_name}.{function_name} -> ( {filename} )", nameof(io_proxy), nameof(CreateDirectory));

            var dir = Path.GetDirectoryName(filename);

            if (!String.IsNullOrWhiteSpace(dir))
            {
                Directory.CreateDirectory(dir);
            }
            else
            {
                throw new Exception();
            }
        }

        public static string[] ReadAllLines(string filename, string module_name = "", string function_name = "")
        {
            filename = fix_path(filename);
            io_proxy.WriteLine($"{module_name}.{function_name} -> ( {filename} )", nameof(io_proxy), nameof(ReadAllLines));

            while (true)
            {
                try
                {
                    var ret = File.ReadAllLines(filename);

                    return ret;
                }
                catch (Exception e)
                {
                    WriteLine(e.ToString(), nameof(io_proxy), nameof(ReadAllLines));
                    Task.Delay(new TimeSpan(0, 0, 0, 10)).Wait();
                }
            }
        }



        public static string ReadAllText(string filename, string module_name = "", string function_name = "")
        {
            filename = fix_path(filename);
            io_proxy.WriteLine($"{module_name}.{function_name} -> ( {filename} )", nameof(io_proxy), nameof(ReadAllText));

            while (true)
            {
                try
                {
                    var ret = File.ReadAllText(filename);

                    return ret;
                }
                catch (Exception e)
                {
                    WriteLine(e.ToString(), nameof(io_proxy), nameof(ReadAllText));
                    Task.Delay(new TimeSpan(0, 0, 0, 10)).Wait();
                }
            }
        }


        public static void WriteAllLines(string filename, IEnumerable<string> lines, string module_name = "", string function_name = "")
        {
            filename = fix_path(filename);
            io_proxy.WriteLine($"{module_name}.{function_name} -> ( {filename} )", nameof(io_proxy), nameof(WriteAllLines));

            CreateDirectory(filename, module_name, function_name);

            while (true)
            {
                try
                {
                    File.WriteAllLines(filename, lines);
                    return;
                }
                catch (Exception e)
                {
                    WriteLine(e.ToString(), nameof(io_proxy), nameof(WriteAllLines));
                    Task.Delay(new TimeSpan(0, 0, 0, 10)).Wait();
                }
            }
        }

        public static void AppendAllLines(string filename, IEnumerable<string> lines, string module_name = "", string function_name = "")
        {
            filename = fix_path(filename);
            io_proxy.WriteLine($"{module_name}.{function_name} -> ( {filename} )", nameof(io_proxy), nameof(AppendAllLines));

            CreateDirectory(filename, module_name, function_name);

            while (true)
            {
                try
                {
                    File.AppendAllLines(filename, lines);
                    return;
                }
                catch (Exception e)
                {
                    WriteLine(e.ToString(), nameof(io_proxy), nameof(AppendAllLines));
                    Task.Delay(new TimeSpan(0, 0, 0, 10)).Wait();
                }
            }
        }

        public static void AppendAllText(string filename, string text, string module_name = "", string function_name = "")
        {
            filename = fix_path(filename);
            io_proxy.WriteLine($"{module_name}.{function_name} -> ( {filename} )", nameof(io_proxy), nameof(AppendAllText));

            CreateDirectory(filename, module_name, function_name);

            while (true)
            {
                try
                {
                    File.AppendAllText(filename, text);
                    return;
                }
                catch (Exception e)
                {
                    WriteLine(e.ToString(), nameof(io_proxy), nameof(AppendAllText));
                    Task.Delay(new TimeSpan(0, 0, 0, 10)).Wait();
                }
            }
        }

        public static void WriteAllText(string filename, string text, string module_name = "", string function_name = "")
        {
            filename = fix_path(filename);
            io_proxy.WriteLine($"{module_name}.{function_name} -> ( {filename} )", nameof(io_proxy), nameof(WriteAllText));

            CreateDirectory(filename, module_name, function_name);

            while (true)
            {
                try
                {
                    File.WriteAllText(filename, text);
                    return;
                }
                catch (Exception e)
                {
                    WriteLine(e.ToString(), nameof(io_proxy), nameof(WriteAllText));
                    Task.Delay(new TimeSpan(0, 0, 0, 10)).Wait();
                }
            }
        }
    }
}
