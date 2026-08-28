//  ------------------------------------------------------------------------------------
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

namespace Test.Amqp
{
    using System;
    using System.Collections.Generic;
    using System.Net.WebSockets;
    using System.Reflection;
    using System.Threading;
    using System.Threading.Tasks;
    using global::Amqp;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class WebSocketDeflateReceiveTests
    {
        [TestMethod]
        public async Task ReceiveAsync_ZeroLengthReadWhileOpen_KeepsReadingForRealBytes()
        {
            byte[] payload = new byte[] { 1, 2, 3, 4, 5 };
            var mock = new MockWebSocket(
                // 1st read: zero bytes but socket still Open (deflate consumed the frame).
                seg => new WebSocketReceiveResult(0, WebSocketMessageType.Binary, false),
                // 2nd read: the real decompressed bytes.
                seg =>
                {
                    Array.Copy(payload, 0, seg.Array, seg.Offset, payload.Length);
                    return new WebSocketReceiveResult(payload.Length, WebSocketMessageType.Binary, true);
                });

            IAsyncTransport transport = CreateTransport(mock);

            byte[] buffer = new byte[64];
            int read = await transport.ReceiveAsync(buffer, 0, buffer.Length);

            Assert.AreEqual(payload.Length, read,
                "Transport reported EOF/0 on a benign zero-length deflate read instead of looping for real bytes.");
            Assert.AreEqual(2, mock.ReceiveCallCount,
                "Transport should have retried the receive after the zero-length read.");
            CollectionAssert.AreEqual(payload, Slice(buffer, 0, read));
        }

        [TestMethod]
        public async Task ReceiveAsync_CloseMessage_ReturnsZero()
        {
            // A Close message is a real end-of-stream and must return 0.
            var mock = new MockWebSocket(
                seg => new WebSocketReceiveResult(0, WebSocketMessageType.Close, true));

            IAsyncTransport transport = CreateTransport(mock);

            int read = await transport.ReceiveAsync(new byte[64], 0, 64);

            Assert.AreEqual(0, read);
            Assert.AreEqual(1, mock.ReceiveCallCount);
        }

        [TestMethod]
        public async Task ReceiveAsync_ZeroLengthReadWhenNotOpen_ReturnsZero()
        {
            // A zero-length read while the socket is no longer Open is a genuine EOF.
            var mock = new MockWebSocket(
                seg => new WebSocketReceiveResult(0, WebSocketMessageType.Binary, false))
            {
                StateOverride = WebSocketState.Closed
            };

            IAsyncTransport transport = CreateTransport(mock);

            int read = await transport.ReceiveAsync(new byte[64], 0, 64);

            Assert.AreEqual(0, read);
            Assert.AreEqual(1, mock.ReceiveCallCount);
        }

        static byte[] Slice(byte[] source, int offset, int count)
        {
            byte[] result = new byte[count];
            Array.Copy(source, offset, result, 0, count);
            return result;
        }

        static IAsyncTransport CreateTransport(WebSocket webSocket)
        {
            // The WebSocket-injecting constructor is internal. The product assembly is
            // strong-named, so it cannot grant InternalsVisibleTo to this unsigned test
            // assembly; use reflection to invoke the internal constructor instead.
            ConstructorInfo ctor = typeof(WebSocketTransport).GetConstructor(
                BindingFlags.Instance | BindingFlags.NonPublic,
                null,
                new[] { typeof(WebSocket) },
                null);
            Assert.IsTrue(ctor != null, "Could not find internal WebSocketTransport(WebSocket) constructor.");
            return (IAsyncTransport)ctor.Invoke(new object[] { webSocket });
        }

        /// <summary>
        /// A scripted WebSocket whose ReceiveAsync returns a queued sequence of results,
        /// letting a test drive the transport's receive loop deterministically.
        /// </summary>
        sealed class MockWebSocket : WebSocket
        {
            readonly Queue<Func<ArraySegment<byte>, WebSocketReceiveResult>> steps;

            public MockWebSocket(params Func<ArraySegment<byte>, WebSocketReceiveResult>[] steps)
            {
                this.steps = new Queue<Func<ArraySegment<byte>, WebSocketReceiveResult>>(steps);
            }

            public int ReceiveCallCount { get; private set; }

            public WebSocketState StateOverride { get; set; } = WebSocketState.Open;

            public override WebSocketState State => this.StateOverride;

            public override Task<WebSocketReceiveResult> ReceiveAsync(ArraySegment<byte> buffer, CancellationToken cancellationToken)
            {
                this.ReceiveCallCount++;
                Func<ArraySegment<byte>, WebSocketReceiveResult> step = this.steps.Dequeue();
                return Task.FromResult(step(buffer));
            }

            public override WebSocketCloseStatus? CloseStatus => null;

            public override string CloseStatusDescription => null;

            public override string SubProtocol => "amqp";

            public override void Abort()
            {
            }

            public override Task CloseAsync(WebSocketCloseStatus closeStatus, string statusDescription, CancellationToken cancellationToken)
            {
                return Task.CompletedTask;
            }

            public override Task CloseOutputAsync(WebSocketCloseStatus closeStatus, string statusDescription, CancellationToken cancellationToken)
            {
                return Task.CompletedTask;
            }

            public override void Dispose()
            {
            }

            public override Task SendAsync(ArraySegment<byte> buffer, WebSocketMessageType messageType, bool endOfMessage, CancellationToken cancellationToken)
            {
                return Task.CompletedTask;
            }
        }
    }
}
