using StackExchange.Redis;
using System.Reflection;
using System.Runtime.CompilerServices;
using System;
using System.Collections.Generic;
using System.Threading;

[assembly: AssemblyVersion("1.0.0")]

namespace BasicTest
{
    static class Program
    {
        static void Main()
        {
            using (var conn = ConnectionMultiplexer.Connect("127.0.0.1:6379,syncTimeout=1000"))
            {
                var threads = new List<Thread>();

                for (var i = 0; i < Environment.ProcessorCount; i++)
                {
                    void CallBack(object state)
                    {
                        var spinWait = new SpinWait();
                        spinWait.SpinOnce();
                        ThreadPool.QueueUserWorkItem(CallBack);
                    }

                    ThreadPool.QueueUserWorkItem(CallBack);
                }

                for (var i = 0; i < 1000; i++)
                {
                    threads.Add(new Thread(() =>
                    {
                        var random = new Random(Thread.CurrentThread.ManagedThreadId);

                        try
                        {
                            var attempt = 1L;

                            while (attempt++ < 5_000_000)
                            {
                                var flags = random.Next(10) == 0 ? CommandFlags.HighPriority : CommandFlags.None;

                                if (attempt % 2 == 0)
                                {
                                    var tran = conn.GetDatabase().CreateTransaction();
                                    tran.AddCondition(Condition.KeyNotExists("aksjfhjkq2333"));

                                    tran.StringGetSetAsync("asdasd",
                                        "ASKFJHASKJHFKJASHFKJNAKJnaJKsnfkjansfjkhakjfshKJHASFKJHFKJAHSFKJHAKjsfkjahsfkjahsfkjahskfjhaskjfhashfkjashfKJHFASK",
                                        flags | CommandFlags.FireAndForget);

                                    tran.Execute();
                                }
                                else
                                {
                                    conn.GetDatabase().StringGetSet("asdasd",
                                        "ASKFJHASKJHFKJASHFKJNAKJnaJKsnfkjansfjkhakjfshKJHASFKJHFKJAHSFKJHAKjsfkjahsfkjahsfkjahskfjhaskjfhashfkjashfKJHFASK",
                                        flags);
                                }

                                if ((attempt % 10000) == 0)
                                {
                                    Thread.Sleep(5000);
                                }
                            }
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e);
                            throw;
                        }
                    }));
                }

                foreach (var thread in threads)
                {
                    thread.Start();
                }

                foreach (var thread in threads)
                {
                    thread.Join();
                }

                var db = conn.GetDatabase();

                Console.WriteLine(db.KeyExists("hello"));
                Console.ReadLine();
            }
        }

        internal static string Me([CallerMemberName] string caller = null)
        {
            return caller;
        }
    }
}
