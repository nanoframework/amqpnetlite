﻿//  ------------------------------------------------------------------------------------
//  Copyright (c) Microsoft Corporation
//  All rights reserved. 
//  
//  Licensed under the Apache License, Version 2.0 (the ""License""); you may not use this 
//  file except in compliance with the License. You may obtain a copy of the License at 
//  http://www.apache.org/licenses/LICENSE-2.0  
//  
//  THIS CODE IS PROVIDED *AS IS* BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
//  EITHER EXPRESS OR IMPLIED, INCLUDING WITHOUT LIMITATION ANY IMPLIED WARRANTIES OR 
//  CONDITIONS OF TITLE, FITNESS FOR A PARTICULAR PURPOSE, MERCHANTABLITY OR 
//  NON-INFRINGEMENT. 
// 
//  See the Apache Version 2.0 License for specific language governing permissions and 
//  limitations under the License.
//  ------------------------------------------------------------------------------------

namespace PerfTest
{
    using System;
    using System.Collections.Generic;
    using System.Security.Cryptography.X509Certificates;
    using System.Threading;
    using System.Threading.Tasks;
    using Amqp;
    using Amqp.Framing;
    using Amqp.Listener;
    using Test.Common;
    using Stopwatch = System.Diagnostics.Stopwatch;
    using TestExtensions = Test.Common.Extensions;

    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                PerfArguments perfArgs = new PerfArguments(args);
                if (args.Length == 0 || perfArgs.HasHelp)
                {
                    Usage();
                    return;
                }

                if (perfArgs.TraceLevel != 0)
                {
                    Trace.TraceLevel = perfArgs.TraceLevel;
                    Trace.TraceListener = (l, f, o) => Console.WriteLine(DateTime.Now.ToString("[hh:mm:ss.fff]") + " " + string.Format(f, o));
                }

                Role role;
                if (string.Equals("send", perfArgs.Operation, StringComparison.OrdinalIgnoreCase))
                {
                    role = new Sender(perfArgs);
                }
                else if (string.Equals("loadgen", perfArgs.Operation, StringComparison.OrdinalIgnoreCase))
                {
                    role = new LoadGenerator(perfArgs);
                }
                else if (string.Equals("receive", perfArgs.Operation, StringComparison.OrdinalIgnoreCase))
                {
                    role = new Receiver(perfArgs);
                }
                else if (string.Equals("request", perfArgs.Operation, StringComparison.OrdinalIgnoreCase))
                {
                    role = new Requestor(perfArgs);
                }
                else if (string.Equals("reply", perfArgs.Operation, StringComparison.OrdinalIgnoreCase))
                {
                    role = new ReplyListener(perfArgs);
                }
                else if (string.Equals("listen", perfArgs.Operation, StringComparison.OrdinalIgnoreCase))
                {
                    role = new Listener(perfArgs);
                }
                else
                {
                    Usage();
                    return;
                }

                Console.WriteLine("Running perf test...");
                role.Run();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }
        }

        static void Usage()
        {
            Console.WriteLine(System.Diagnostics.Process.GetCurrentProcess().ProcessName + ".exe send|receive|listen [arguments]");
            Console.WriteLine("  send   \tsend messages to remote peer");
            Console.WriteLine("  receive\treceive messages from remote peer");
            Console.WriteLine("  request\tsend requests to a remote peer");
            Console.WriteLine("  reply  \tstart a request processor and send replies");
            Console.WriteLine("  loadgen\tstart a load generator");
            Console.WriteLine("  listen \tstart a listener and accept messages from remote peer");
            Console.WriteLine("\r\narguments:");
            Console.WriteLine(Arguments.PrintArguments(typeof(PerfArguments)));
        }

        static int GetLatencyMs(Message message)
        {
            if (message.Properties != null && message.Properties.CreationTime.Ticks > 0)
            {
                return (int)((Stopwatch.GetTimestamp() - message.Properties.CreationTime.Ticks) / TimeSpan.TicksPerMillisecond);
            }

            return -1;
        }

        abstract class Role
        {
            protected IBufferManager bufferManager;
            PerfArguments perfArgs;
            long count;
            long started;
            long completed;
            long progress;
            ManualResetEvent completedEvent;
            OperationTracker tracker;

            public Role(PerfArguments perfArgs)
            {
                this.bufferManager = perfArgs.BufferPooling ? new BufferManager(256, 2 * 1024 * 1024, 100 * 1024 * 1024) : null;
                this.perfArgs = perfArgs;
                this.count = perfArgs.Count;
                this.progress = perfArgs.Progress;
                this.completedEvent = new ManualResetEvent(false);
                this.tracker = new OperationTracker(20000);
            }

            protected PerfArguments Args
            {
                 get { return this.perfArgs; }
            }

            public abstract void Run();

            protected string GetAddress(int id)
            {
                return this.perfArgs.Node.Replace("{i}", id.ToString());
            }

            protected Connection CreateConnection(Address address)
            {
                var factory = new ConnectionFactory();
                factory.BufferManager = this.bufferManager;
                factory.AMQP.MaxFrameSize = this.perfArgs.MaxFrameSize;
                factory.AMQP.HostName = this.perfArgs.Host;
                if (address.Scheme.Equals("amqps", StringComparison.OrdinalIgnoreCase))
                {
                    factory.SSL.RemoteCertificateValidationCallback = (a, b, c, d) => true;
                }

                return factory.CreateAsync(address).Result;
            }

            protected bool OnStart()
            {
                return Interlocked.Increment(ref this.started) <= this.count || this.count == 0;
            }

            protected bool OnComplete(int latencyMs)
            {
                this.tracker.Track(latencyMs);
                long done = Interlocked.Increment(ref this.completed);
                if (this.progress > 0 && done % this.progress == 0)
                {
                    Console.WriteLine(this.tracker.Report(reset: true));
                }

                if (this.count > 0 && done >= this.count)
                {
                    this.completedEvent.Set();
                    return false;
                }
                else
                {
                    return this.OnStart();
                }
            }

            protected bool Wait(int milliseconds = -1)
            {
                return this.completedEvent.WaitOne(milliseconds);
            }

            protected void SetComplete()
            {
                this.completedEvent.Set();
            }
        }

        class Sender : Role
        {
            static OutcomeCallback onOutcome = OnSendComplete;
            int bodySize;

            public Sender(PerfArguments args)
                : base(args)
            {
                this.bodySize = args.BodySize;
            }

            public override void Run()
            {
                Task[] tasks = new Task[this.Args.Connections];
                for (int i = 0; i < this.Args.Connections; i++)
                {
                    int id = i;
                    tasks[i] = Task.Run(() => this.RunOnce(id));
                }

                Task.WhenAll(tasks).Wait();
            }

            static void OnSendComplete(ILink link, Message message, Outcome outcome, object state)
            {
                var tuple = (Tuple<Sender, SenderLink>)state;
                Sender thisPtr = tuple.Item1;
                SenderLink sender = tuple.Item2;
                if (thisPtr.bufferManager != null)
                {
                    var buffer = message.GetBody<ByteBuffer>();
                    buffer.Reset();
                    thisPtr.bufferManager.ReturnBuffer(new ArraySegment<byte>(buffer.Buffer, buffer.Offset, buffer.Capacity));
                }

                if (thisPtr.OnComplete(GetLatencyMs(message)))
                {
                    Message msg = thisPtr.CreateMessage();
                    sender.Send(msg, onOutcome, state);
                }
            }

            void RunOnce(int id)
            {
                Connection connection = this.CreateConnection(new Address(this.Args.Address));
                connection.Closed += (o, e) => this.SetComplete();

                Session session = new Session(connection);

                Attach attach = new Attach()
                {
                    Source = new Source(),
                    Target = new Target() { Address = this.GetAddress(id) },
                    SndSettleMode = this.Args.SenderMode,
                    RcvSettleMode = this.Args.ReceiverMode
                };

                SenderLink sender = new SenderLink(session, "perf-test-sender" + id, attach, null);

                for (int i = 1; i <= this.Args.Queue; i++)
                {
                    if (this.OnStart())
                    {
                        var message = this.CreateMessage();
                        sender.Send(message, onOutcome, Tuple.Create(this, sender));
                    }
                }

                this.Wait();

                sender.Close();
                session.Close();
                connection.Close();
            }

            Message CreateMessage()
            {
                ArraySegment<byte> segment = this.bufferManager != null ?
                    this.bufferManager.TakeBuffer(this.bodySize) :
                    new ArraySegment<byte>(new byte[this.bodySize]);
                int seed = DateTime.UtcNow.Millisecond;
                for (int i = 0; i < this.bodySize; i++)
                {
                    segment.Array[segment.Offset + i] = (byte)((i + seed) % 256);
                }

                Message message = new Message();
                message.Properties = new Properties() { MessageId = "msg", CreationTime = new DateTime(Stopwatch.GetTimestamp(), DateTimeKind.Utc) };
                message.BodySection = new Data()
                {
                    Buffer = new ByteBuffer(segment.Array, segment.Offset, this.bodySize, segment.Count)
                };

                return message;
            }
        }

        class LoadGenerator : Role
        {
            byte[] payload;

            public LoadGenerator(PerfArguments args)
                : base(args)
            {
                this.payload = new byte[args.BodySize];
            }

            public override void Run()
            {
                Task[] tasks = new Task[this.Args.Connections];
                for (int i = 0; i < this.Args.Connections; i++)
                {
                    int id = i;
                    tasks[i] = this.RunOnce(id);
                }

                Task.WhenAll(tasks).Wait();
            }

            async Task RunOnce(int id)
            {
                IList<string> nodes = Expand(this.Args.Node);
                var address = new Address(this.Args.Address);
                var factory = new ConnectionFactory();
                factory.AMQP.MaxFrameSize = this.Args.MaxFrameSize;
                factory.AMQP.HostName = this.Args.Host;
                factory.SSL.RemoteCertificateValidationCallback = (a, b, c, d) => true;

                do
                {
                    try
                    {
                        var connection = await factory.CreateAsync(address);
                        Session session = null;

                        for (int m = 0; m < nodes.Count && !connection.IsClosed; m++)
                        {
                            string node = null;
                            try
                            {
                                node = nodes[m];
                                if (m % 100 == 0)
                                {
                                    session = new Session(connection);
                                }

                                Attach attach = new Attach()
                                {
                                    Source = new Source(),
                                    Target = new Target() { Address = node },
                                    SndSettleMode = this.Args.SenderMode,
                                    RcvSettleMode = this.Args.ReceiverMode
                                };
                                SenderLink sender = new SenderLink(session, "load-generator-" + m, attach, null);
                                await Task.Yield();

                                for (int i = 1; i <= this.Args.Queue; i++)
                                {
                                    var message = this.CreateMessage();
                                    sender.Send(message, null, null);
                                }

                                await connection.FlushAsync();
                                Console.WriteLine($"{id}: {node} done {this.Args.Queue} messages");
                            }
                            catch (Exception e)
                            {
                                Console.WriteLine($"{id}: {node} send failure: {e.GetType().Name}-{e.Message}");
                            }
                        }

                        await Task.Delay(4000);
                        connection.Close(TimeSpan.Zero);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine($"{id}: Setup failure-{e.GetType().Name}-{e.Message}");
                    }
                }
                while (!this.Wait(10000));
                Console.WriteLine($"{id}: Exit");
            }

            static IList<string> Expand(string pattern)
            {
                int start = pattern.IndexOf('[');
                int end = -1;
                if (start >= 0)
                {
                    end = pattern.IndexOf(']', start);
                }

                if (start < 0 || end < 0 || end - start <= 1)
                {
                    return new string[] { pattern };
                }

                // [3-4,7,10,1-5]
                string left = pattern.Substring(0, start);
                string right = pattern.Substring(end + 1);

                var items = new List<string>();
                string[] ranges = pattern.Substring(start + 1, end - start - 1).Split(',');
                foreach (var r in ranges)
                {
                    string[] pair = r.Split('-');
                    if (pair.Length == 1 || !int.TryParse(pair[0], out int r1) || !int.TryParse(pair[1], out int r2))
                    {
                        items.Add(left + r + right);
                    }
                    else
                    {
                        for (int i = r1; i <= r2; i++)
                        {
                            items.Add(left + i + right);
                        }
                    }
                }

                return items;
            }

            Message CreateMessage()
            {
                Message message = new Message();
                message.Properties = new Properties() { MessageId = "msg", CreationTime = new DateTime(Stopwatch.GetTimestamp(), DateTimeKind.Utc) };
                message.BodySection = new Data()
                {
                    Buffer = new ByteBuffer(payload, 0, payload.Length, payload.Length)
                };

                return message;
            }
        }

        class Receiver : Role
        {
            public Receiver(PerfArguments args)
                : base(args)
            {
            }

            public override void Run()
            {
                Task[] tasks = new Task[this.Args.Connections];
                for (int i = 0; i < this.Args.Connections; i++)
                {
                    int id = i;
                    tasks[i] = Task.Run(() => this.RunOnce(id));
                }

                Task.WhenAll(tasks).Wait();
            }

            void RunOnce(int id)
            {
                Connection connection = this.CreateConnection(new Address(this.Args.Address));
                connection.Closed += (o, e) => this.SetComplete();

                Session session = new Session(connection);

                Attach attach = new Attach()
                {
                    Source = new Source() { Address = this.GetAddress(id) },
                    Target = new Target(),
                    SndSettleMode = this.Args.SenderMode,
                    RcvSettleMode = this.Args.ReceiverMode
                };

                ReceiverLink receiver = new ReceiverLink(session, "perf-test-receiver" + id, attach, null);
                receiver.Start(
                    this.Args.Queue,
                    (r, m) =>
                    {
                        r.Accept(m);
                        m.Dispose();
                        this.OnComplete(GetLatencyMs(m));
                    });

                this.Wait();

                receiver.Close();
                session.Close();
                connection.Close();
            }
        }

        class Requestor : Role
        {
            byte[] buffer;

            public Requestor(PerfArguments args)
                : base(args)
            {
                if (args.BufferPooling)
                {
                    this.buffer = new byte[args.BodySize];  // a simulation of buffer pooling
                }
            }

            public override void Run()
            {
                Task[] tasks = new Task[this.Args.Connections];
                for (int i = 0; i < this.Args.Connections; i++)
                {
                    int id = i;
                    tasks[i] = Task.Run(() => this.RunOnce(id));
                }

                Task.WhenAll(tasks).Wait();
            }

            void SendRequest(SenderLink sender, string replyTo)
            {
                Message message = new Message();
                message.Properties = new Properties() { ReplyTo = replyTo, CreationTime = new DateTime(Stopwatch.GetTimestamp(), DateTimeKind.Utc) };
                message.Properties.SetCorrelationId(Guid.NewGuid());
                message.BodySection = new Data() { Binary = this.GetBuffer() };
                sender.Send(message, null, null);
            }

            byte[] GetBuffer()
            {
                return this.Args.BufferPooling ? this.buffer : new byte[this.Args.BodySize];
            }

            void RunOnce(int id)
            {
                Connection connection = this.CreateConnection(new Address(this.Args.Address));
                connection.Closed += (o, e) => this.SetComplete();
                Session session = new Session(connection);
                string clientId = "request-" + Guid.NewGuid().ToString().Substring(0, 6);
                Attach sendAttach = new Attach()
                {
                    Source = new Source(),
                    Target = new Target() { Address = this.GetAddress(id) },
                    SndSettleMode = SenderSettleMode.Settled
                };
                Attach recvAttach = new Attach()
                {
                    Source = new Source() { Address = this.GetAddress(id) },
                    Target = new Target() { Address = clientId },
                    SndSettleMode = SenderSettleMode.Settled
                };
                SenderLink sender = new SenderLink(session, "s-" + clientId, sendAttach, null);
                ReceiverLink receiver = new ReceiverLink(session, "r-" + clientId, recvAttach, null);
                receiver.Start(
                    50000,
                    (r, m) =>
                    {
                        r.Accept(m);
                        m.Dispose();
                        if (this.OnComplete(GetLatencyMs(m)))
                        {
                            this.SendRequest(sender, clientId);
                        }
                    });

                for (int i = 1; i <= this.Args.Queue; i++)
                {
                    if (this.OnStart())
                    {
                        this.SendRequest(sender, clientId);
                    }
                }

                this.Wait();

                connection.Close();
            }
        }

        class ReplyListener : Role, IRequestProcessor
        {
            public ReplyListener(PerfArguments args)
                : base(args)
            {
            }

            public override void Run()
            {
                Address addressUri = new Address(this.Args.Address);
                X509Certificate2 certificate = TestExtensions.GetCertificate(addressUri.Scheme, addressUri.Host, this.Args.CertValue);
                ContainerHost host = new ContainerHost(new Address[] { addressUri }, certificate);
                foreach (var listener in host.Listeners)
                {
                    listener.BufferManager = this.bufferManager;
                    listener.AMQP.MaxFrameSize = this.Args.MaxFrameSize;
                }

                host.Open();
                Console.WriteLine("Container host is listening on {0}:{1}", addressUri.Host, addressUri.Port);

                host.RegisterRequestProcessor(this.Args.Node, this);
                Console.WriteLine("Message processor is registered on {0}", this.Args.Node);

                this.Wait();

                host.Close();
            }

            int IRequestProcessor.Credit { get { return this.Args.Queue; } }

            void IRequestProcessor.Process(RequestContext requestContext)
            {
                Message response = new Message("request processed");
                response.Properties = new Properties() { CreationTime = requestContext.Message.Properties.CreationTime };
                response.ApplicationProperties = new ApplicationProperties();
                response.ApplicationProperties["status-code"] = 200;
                requestContext.Complete(response);
                this.OnComplete(GetLatencyMs(requestContext.Message));
            }
        }

        class Listener : Role, IMessageProcessor
        {
            int credit;

            public Listener(PerfArguments args)
                : base(args)
            {
                this.credit = args.Queue;
            }

            public override void Run()
            {
                Address addressUri = new Address(this.Args.Address);
                X509Certificate2 certificate = TestExtensions.GetCertificate(addressUri.Scheme, addressUri.Host, this.Args.CertValue);
                ContainerHost host = new ContainerHost(new Address[] { addressUri }, certificate);
                foreach (var listener in host.Listeners)
                {
                    listener.BufferManager = this.bufferManager;
                    listener.AMQP.MaxFrameSize = this.Args.MaxFrameSize;
                }

                host.Open();
                Console.WriteLine("Container host is listening on {0}:{1}", addressUri.Host, addressUri.Port);

                host.RegisterMessageProcessor(this.Args.Node, this);
                Console.WriteLine("Message processor is registered on {0}", this.Args.Node);

                this.Wait();

                host.Close();
            }

            int IMessageProcessor.Credit
            {
                get { return this.credit; }
            }

            void IMessageProcessor.Process(MessageContext messageContext)
            {
                messageContext.Complete();
                this.OnComplete(-1);
            }
        }

        class PerfArguments : Arguments
        {
            public PerfArguments(string[] args)
                : base(args, 1)
            {
                this.Operation = args.Length > 0 ? args[0] : null;
            }

            public string Operation
            {
                get;
                private set;
            }

            [Argument(Name = "address", Shortcut = "a", Description = "address of the remote peer or the local listener", Default = "amqp://guest:guest@127.0.0.1:5672")]
            public string Address
            {
                get;
                protected set;
            }

            [Argument(Name = "cert", Shortcut = "f", Description = "certificate for SSL authentication. Default to address.host")]
            public string CertValue
            {
                get;
                protected set;
            }

            [Argument(Name = "host", Shortcut = "h", Description = "Set open.hostname")]
            public string Host
            {
                get;
                protected set;
            }

            [Argument(Name = "node", Shortcut = "n", Description = "name of the AMQP node. Can have '{i}' for connection id and '[range]' for multiple names.", Default = "q1")]
            public string Node
            {
                get;
                protected set;
            }

            [Argument(Name = "count", Shortcut = "c", Description = "total number of messages to send or receive (0: infinite)", Default = 100000)]
            public long Count
            {
                get;
                protected set;
            }

            [Argument(Name = "connection", Shortcut = "i", Description = "number of connection to create", Default = 1)]
            public int Connections
            {
                get;
                protected set;
            }

            [Argument(Name = "body-size", Shortcut = "b", Description = "message body size (bytes)", Default = 64)]
            public int BodySize
            {
                get;
                protected set;
            }

            [Argument(Name = "max-frame-size", Shortcut = "m", Description = "connection max frame size (bytes)", Default = 256*1024)]
            public int MaxFrameSize
            {
                get;
                protected set;
            }

            [Argument(Name = "queue", Shortcut = "q", Description = "outgoing queue depth (link credit)", Default = 1000)]
            public int Queue
            {
                get;
                protected set;
            }

            [Argument(Name = "progress", Shortcut = "p", Description = "report progress for every this number of messages", Default = 1000)]
            public int Progress
            {
                get;
                protected set;
            }

            [Argument(Name = "buffer-pool", Shortcut = "u", Description = "enable buffer pooling", Default = false)]
            public bool BufferPooling
            {
                get;
                protected set;
            }

            [Argument(Name = "ack", Shortcut = "k", Description = "ack mode: amo|alo|eo", Default = "amo")]
            protected string Ack
            {
                get;
                set;
            }

            [Argument(Name = "trace", Shortcut = "t", Description = "trace level: err|warn|info|verbose|frm", Default = null)]
            protected string Trace
            {
                get;
                set;
            }

            public SenderSettleMode SenderMode
            {
                get { return this.Ack.ToSenderSettleMode(); }
            }

            public ReceiverSettleMode ReceiverMode
            {
                get { return this.Ack.ToReceiverSettleMode(); }
            }

            public TraceLevel TraceLevel
            {
                get { return this.Trace == null ? 0 : this.Trace.ToTraceLevel(); }
            }
        }
    }
}
