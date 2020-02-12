using System;
using System.Collections.Generic;

namespace svm_fs
{
    public static class common
    {
        public enum scale_function : int
        {
            none,
            rescale,
            normalisation,
            standardisation,
            L0_norm,
            L1_norm,
            L2_norm,
        }

        public enum libsvm_kernel_type : int
        {
            //@default = rbf,
            linear = 0,
            polynomial = 1,
            rbf = 2,
            sigmoid = 3,
            precomputed = 4,
        }

        public enum libsvm_svm_type : int
        {
            //@default = c_svc,
            c_svc = 0,
            nu_svc = 1,
            one_class_svm = 2,
            epsilon_svr = 3,
            nu_svr = 4,
        }

        //[ThreadStatic] private static Random _local;

        //public static Random this_threads_random => _local ?? (_local = new Random(unchecked(Environment.TickCount * 31 + Thread.CurrentThread.ManagedThreadId)));

        public static void shuffle<T>(this IList<T> list, Random random)// = null)
        {
            //if (random == null) random = this_threads_random;

            //var k_list = new List<int>();

            for (var n = list.Count - 1; n >= 0; n--)
            {
                var k = random.Next(0, list.Count - 1);
                //k_list.Add(k);

                var value = list[k];
                list[k] = list[n];
                list[n] = value;
            }

            // if (program.write_console_log) program.WriteLine(string.Join(",",k_list));
        }
    }
}
