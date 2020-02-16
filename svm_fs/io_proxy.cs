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
        public static bool is_file_available(string filename)
        {
            try
            {
                filename = io_proxy.convert_path(filename);

                if (string.IsNullOrWhiteSpace(filename)) return false;

                if (!io_proxy.Exists(filename, nameof(svm_ctl), nameof(is_file_available))) return false;

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

        public static string convert_path(string path)//, bool temp_file = false)
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

            // remove invalid chars
            //var invalid = $"?%*|¦<>\"" + string.Join("", Enumerable.Range(0, 32).Select(a => (char)a).ToList()); // includes \0 \b \t \r \n, leaves /\\: as it is full paths input
            const string valid = ":\\/.qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM0123456789_-+()[]";
            path = string.Join("", path.Select(a => !valid.Any(b => a == b) ? '_' : a).ToList());

            // make sure no part is more than 255 length

            var path_split = path.Split(new char[] { '\\', '/' });
            if (path_split.Any(a=> a.Length > 255))
            {
                var end_slash = path.Last() == '\\' || path.Last() == '/' ? "" + path.Last() : "";
                
                for (var i = 0; i < path_split.Length; i++)
                {
                    if (path_split[i].Length > 255)
                    {
                        path_split[i] = path_split[i].Substring(0, 255);
                    }
                }

                path = end_slash.Length == 0 ? Path.Combine(path_split) : Path.Combine(path_split) + end_slash;
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
            filename = convert_path(filename);
            io_proxy.WriteLine($"{module_name}.{function_name} -> ( {filename} )", nameof(io_proxy), nameof(Exists));

            return File.Exists(filename);
        }
            
        public static void Delete(string filename, string module_name = "", string function_name = "")
        {
            filename = convert_path(filename);

            io_proxy.WriteLine($"{module_name}.{function_name} -> ( {filename} )", nameof(io_proxy), nameof(Delete));

            try
            {
                File.Delete(filename);
            }
            catch (Exception e)
            { 
                    WriteLine(e.ToString(), nameof(io_proxy), nameof(Delete));

            }
        }

        public static void Copy(string source, string dest, bool overwrite = false, string module_name = "", string function_name = "", int max_tries = 1_000_000)
        {
            source = convert_path(source);
            dest = convert_path(dest);

            io_proxy.WriteLine($"{module_name}.{function_name} -> ( {source} , {dest} , {overwrite} )", nameof(io_proxy), nameof(Copy));

            var tries = 0;

            while (true)
            {
                try
                {
                    tries++;
                    
                    File.Copy(source, dest, overwrite);
                }
                catch (Exception e)
                {
                    WriteLine(e.ToString(), nameof(io_proxy), nameof(Copy));

                    if (tries >= max_tries) throw;

                    Task.Delay(new TimeSpan(0, 0, 10)).Wait();
                }
            }
        }

        public static void CreateDirectory(string filename, string module_name = "", string function_name = "")
        {
            filename = convert_path(filename);

            //io_proxy.WriteLine($"{module_name}.{function_name} -> ( {filename} )", nameof(io_proxy), nameof(CreateDirectory));

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

        public static string[] ReadAllLines(string filename, string module_name = "", string function_name = "", int max_tries = 1_000_000)
        {
            filename = convert_path(filename);
            io_proxy.WriteLine($"{module_name}.{function_name} -> ( {filename} )", nameof(io_proxy), nameof(ReadAllLines));

            int tries = 0;

            while (true)
            {
                try
                {
                    tries++;

                    var ret = File.ReadAllLines(filename);

                    return ret;
                }
                catch (Exception e)
                {
                    WriteLine(e.ToString(), nameof(io_proxy), nameof(ReadAllLines));

                    if (tries >= max_tries) throw;

                    Task.Delay(new TimeSpan(0, 0, 0, 10)).Wait();
                }
            }
        }



        public static string ReadAllText(string filename, string module_name = "", string function_name = "", int max_tries = 1_000_000)
        {
            filename = convert_path(filename);
            io_proxy.WriteLine($"{module_name}.{function_name} -> ( {filename} )", nameof(io_proxy), nameof(ReadAllText));

            int tries = 0;

            while (true)
            {
                try
                {
                    tries++;

                    var ret = File.ReadAllText(filename);

                    return ret;
                }
                catch (Exception e)
                {
                    WriteLine(e.ToString(), nameof(io_proxy), nameof(ReadAllText));

                    if (tries >= max_tries) throw;

                    Task.Delay(new TimeSpan(0, 0, 0, 10)).Wait();
                }
            }
        }


        public static void WriteAllLines(string filename, IEnumerable<string> lines, string module_name = "", string function_name = "", int max_tries = 1_000_000)
        {
            filename = convert_path(filename);
            io_proxy.WriteLine($"{module_name}.{function_name} -> ( {filename} )", nameof(io_proxy), nameof(WriteAllLines));

            CreateDirectory(filename, module_name, function_name);

            var tries = 0;

            while (true)
            {
                try
                {
                    tries++;

                    File.WriteAllLines(filename, lines);
                    return;
                }
                catch (Exception e)
                {
                    WriteLine(e.ToString(), nameof(io_proxy), nameof(WriteAllLines));

                    if (tries >= max_tries) throw;

                    Task.Delay(new TimeSpan(0, 0, 0, 10)).Wait();
                }
            }
        }

        public static void AppendAllLines(string filename, IEnumerable<string> lines, string module_name = "", string function_name = "", int max_tries = 1_000_000)
        {
            filename = convert_path(filename);
            io_proxy.WriteLine($"{module_name}.{function_name} -> ( {filename} )", nameof(io_proxy), nameof(AppendAllLines));

            CreateDirectory(filename, module_name, function_name);

            var tries = 0;
            while (true)
            {
                try
                {
                    tries++;
                    File.AppendAllLines(filename, lines);
                    return;
                }
                catch (Exception e)
                {
                    WriteLine(e.ToString(), nameof(io_proxy), nameof(AppendAllLines));

                    if (tries >= max_tries) throw;

                    Task.Delay(new TimeSpan(0, 0, 0, 10)).Wait();
                }
            }
        }

        public static void AppendAllText(string filename, string text, string module_name = "", string function_name = "", int max_tries = 1_000_000)
        {
            filename = convert_path(filename);
            io_proxy.WriteLine($"{module_name}.{function_name} -> ( {filename} )", nameof(io_proxy), nameof(AppendAllText));

            CreateDirectory(filename, module_name, function_name);

            var tries = 0;
            while (true)
            {
                try
                {
                    tries++;
                    File.AppendAllText(filename, text);
                    return;
                }
                catch (Exception e)
                {
                    WriteLine(e.ToString(), nameof(io_proxy), nameof(AppendAllText));

                    if (tries >= max_tries) throw;

                    Task.Delay(new TimeSpan(0, 0, 0, 10)).Wait();
                }
            }
        }

        public static void WriteAllText(string filename, string text, string module_name = "", string function_name = "", int max_tries = 1_000_000)
        {
            filename = convert_path(filename);
            io_proxy.WriteLine($"{module_name}.{function_name} -> ( {filename} )", nameof(io_proxy), nameof(WriteAllText));

            CreateDirectory(filename, module_name, function_name);

            var tries = 0;
            while (true)
            {
                try
                {
                    tries++;
                    File.WriteAllText(filename, text);
                    return;
                }
                catch (Exception e)
                {
                    WriteLine(e.ToString(), nameof(io_proxy), nameof(WriteAllText));

                    if (tries >= max_tries) throw;

                    Task.Delay(new TimeSpan(0, 0, 0, 10)).Wait();
                }
            }
        }
    }
}
