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

// Validates the size, count, and depth checks performed by Amqp.Types.Encoder
// during decode. Each test crafts a raw wire payload with an out-of-range value
// and asserts that decoding fails with AmqpException/DecodeError.

namespace Test.Amqp
{
    using System;
    using global::Amqp;
    using global::Amqp.Types;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class DecoderTests
    {
        // --- ReadArray count/size bounds ---------------------------------

        [TestMethod]
        public void DecoderArrayCountExceedsIntMaxRejectedTest()
        {
            // Array32 with count = 0x80000000 (negative when cast to int).
            byte[] payload = new byte[]
            {
                0xF0,
                0x00, 0x00, 0x00, 0x05,     // size
                0x80, 0x00, 0x00, 0x00,     // count > int.MaxValue
                0x70,
            };

            AssertDecodeError(payload);
        }

        [TestMethod]
        public void DecoderArrayCountExceedsBufferRejectedTest()
        {
            // Non-zero-width element (UInt = 4 bytes each). count(0x7FFFFFFF)
            // is much larger than buffer.Length and must fail validation.
            byte[] payload = new byte[]
            {
                0xF0,
                0x00, 0x00, 0x00, 0x05,
                0x7F, 0xFF, 0xFF, 0xFF,
                0x70,
            };

            AssertDecodeError(payload);
        }

        // --- Zero-width array element size validation -------------------

        [TestMethod]
        public void DecoderArrayOfUInt0ExceedsUnboundedBudgetTest()
        {
            // UInt0 consumes 0 wire bytes per item. 16385 items * 4 bytes/uint
            // = 65540 > MaxUnboundedSize (65536).
            int count = (64 * 1024 / 4) + 1;
            byte[] payload = ZeroWidthArrayPayload(count, 0x43);
            AssertDecodeError(payload);
        }

        [TestMethod]
        public void DecoderArrayOfUInt0AtBudgetSucceedsTest()
        {
            // Exactly at the limit: (64K / 4) UInt0 items = 65536 bytes.
            int count = 64 * 1024 / 4;
            byte[] payload = ZeroWidthArrayPayload(count, 0x43);
            uint[] result = (uint[])Encoder.ReadObject(new ByteBuffer(payload, 0, payload.Length, payload.Length));
            Assert.AreEqual(count, result.Length);
        }

        [TestMethod]
        public void DecoderArrayOfULong0ExceedsUnboundedBudgetTest()
        {
            // ULong0: 8 bytes each. 8193 items = 65544 bytes > 64KB.
            int count = (64 * 1024 / 8) + 1;
            byte[] payload = ZeroWidthArrayPayload(count, 0x44);
            AssertDecodeError(payload);
        }

        [TestMethod]
        public void DecoderArrayOfList0ExceedsUnboundedBudgetTest()
        {
            // List0: 8 bytes each (empty List reference). 8193 * 8 = 65544 > 64KB.
            int count = (64 * 1024 / 8) + 1;
            byte[] payload = ZeroWidthArrayPayload(count, 0x45);
            AssertDecodeError(payload);
        }

        [TestMethod]
        public void DecoderArrayOfBooleanTrueExceedsUnboundedBudgetTest()
        {
            // BooleanTrue: 1 byte each in the decoded bool[]. 65537 > 64KB.
            int count = 64 * 1024 + 1;
            byte[] payload = ZeroWidthArrayPayload(count, 0x41);
            AssertDecodeError(payload);
        }

        [TestMethod]
        public void DecoderArrayOfBooleanFalseExceedsUnboundedBudgetTest()
        {
            int count = 64 * 1024 + 1;
            byte[] payload = ZeroWidthArrayPayload(count, 0x42);
            AssertDecodeError(payload);
        }

        // --- Cross-item accumulation across the same decode call --------

        [TestMethod]
        public void DecoderNestedArraysAccumulateUnboundedTest()
        {
            // 5 inner Array<UInt0> with 4000 items each. The unbounded counter
            // must accumulate across sibling arrays: 5 * 4000 * 4 = 80000 > 64KB.
            // Wire format: outer Array32 with 5 items, shared element FC = Array32,
            // each inner item body = size(4) + count(4) + inner-elt-FC(1) = 9 bytes.
            int innerCount = 4000;
            int numInner = 5;
            int innerBodySize = 9;
            int outerSize = 4 + 1 + numInner * innerBodySize; // count(4) + shared-fc(1) + items

            byte[] payload = new byte[1 + 4 + 4 + 1 + numInner * innerBodySize];
            int off = 0;
            payload[off++] = 0xF0;
            WriteInt(payload, ref off, outerSize);
            WriteInt(payload, ref off, numInner);
            payload[off++] = 0xF0;    // shared inner-item constructor
            int innerSize = 4 + 1;                  // count(4) + inner-elt-FC(1)
            for (int i = 0; i < numInner; i++)
            {
                WriteInt(payload, ref off, innerSize);
                WriteInt(payload, ref off, innerCount);
                payload[off++] = 0x43;
            }

            AssertDecodeError(payload);
        }

        [TestMethod]
        public void DecoderArrayOfDescribedUnboundedCrossItemTest()
        {
            // Verifies that Array<Described> threads the unbounded counter
            // through each item. Each described value is Array<UInt0>[15000]
            // = 60000 bytes. 2 items combined = 120000 bytes > 64KB.
            int perArrayCount = 15000;
            byte[] innerBody = new byte[9]; // size(4) + count(4) + inner-fc(1)
            int p = 0;
            WriteInt(innerBody, ref p, 5);
            WriteInt(innerBody, ref p, perArrayCount);
            innerBody[p] = 0x43;

            // Item wire bytes AFTER the shared Described-constructor byte:
            //   descriptor(SmallULong 2 bytes) + value-FC(1) + inner-body(9) = 12
            // Outer Array<Described>[2]:
            //   Array32 FC(1) + size(4) + count(4) + shared-constructor=Described(1)
            //     + item0(12) + item1(12) = 35 bytes
            int itemBytes = 2 + 1 + 9;
            int outerSize = 4 + 1 + 2 * itemBytes;
            byte[] payload = new byte[1 + 4 + 4 + 1 + 2 * itemBytes];
            int off = 0;
            payload[off++] = 0xF0;
            WriteInt(payload, ref off, outerSize);
            WriteInt(payload, ref off, 2);
            payload[off++] = 0x00;
            for (int i = 0; i < 2; i++)
            {
                payload[off++] = 0x53;
                payload[off++] = 0x09;
                payload[off++] = 0xF0;
                Buffer.BlockCopy(innerBody, 0, payload, off, innerBody.Length);
                off += innerBody.Length;
            }

            AssertDecodeError(payload);
        }

        // --- List / Map size and count validation -----------------------

        [TestMethod]
        public void DecoderListCountExceedsBufferRejectedTest()
        {
            // List32 with count = 0x7FFFFFFF. Every list item consumes at
            // least one wire byte (the element format code), so count is
            // bounded by buffer.Length.
            byte[] payload = new byte[]
            {
                0xD0,
                0x00, 0x00, 0x00, 0x05,
                0x7F, 0xFF, 0xFF, 0xFF,
            };

            AssertDecodeError(payload);
        }

        [TestMethod]
        public void DecoderMapCountExceedsBufferRejectedTest()
        {
            byte[] payload = new byte[]
            {
                0xD1,
                0x00, 0x00, 0x00, 0x05,
                0x7F, 0xFF, 0xFF, 0xFF,
            };

            AssertDecodeError(payload);
        }

        // --- Nesting depth ----------------------------------------------

        [TestMethod]
        public void DecoderNestedDescribedDepthTest()
        {
            // Described chain 100 levels deep exceeds MaxNestingDepth (64).
            // Wire form: Described(0), descriptor SmallULong 0x09, then repeat.
            const int levels = 100;
            byte[] payload = new byte[levels * 3 + 1]; // each level: 0x00 (FC) + 0x53 + 0x09; terminal Null.
            int off = 0;
            for (int i = 0; i < levels; i++)
            {
                payload[off++] = 0x00;
                payload[off++] = 0x53;
                payload[off++] = 0x09;
            }
            payload[off] = 0x40;

            AssertDecodeError(payload);
        }

        [TestMethod]
        public void DecoderNestedListDepthTest()
        {
            // List8 containing itself 100 times deep exceeds MaxNestingDepth.
            // Innermost: single List0 (1 byte). Each wrap adds:
            //   List8(1) + size(1) + count(1) = 3 bytes containing 1 child.
            byte[] inner = new byte[] { 0x45 };
            for (int i = 0; i < 100; i++)
            {
                byte[] wrapped = new byte[3 + inner.Length];
                wrapped[0] = 0xC0;
                wrapped[1] = (byte)(1 + inner.Length); // size = count(1) + body
                wrapped[2] = 1;                        // count
                Buffer.BlockCopy(inner, 0, wrapped, 3, inner.Length);
                inner = wrapped;
            }

            AssertDecodeError(inner);
        }

        [TestMethod]
        public void DecoderNestedArrayDepthTest()
        {
            // Array<Array<Array<...>>> 100 levels exceeds MaxNestingDepth.
            // Each wrap: Array8(1) + size(1) + count(1) + inner-fc=Array8(1) + body.
            // The innermost is an Array8 with count=0 to end the recursion.
            byte[] inner = new byte[] { 0xE0, 0x02, 0x00, 0x43 };
            for (int i = 0; i < 100; i++)
            {
                byte[] wrapped = new byte[3 + inner.Length];
                wrapped[0] = 0xE0;
                wrapped[1] = (byte)(1 + inner.Length); // size
                wrapped[2] = 1;                        // count
                // Element constructor for the outer array is the FC of the inner array
                Buffer.BlockCopy(inner, 0, wrapped, 3, inner.Length);
                inner = wrapped;
            }

            AssertDecodeError(inner);
        }

        [TestMethod]
        public void DecoderDescribedDescriptorAloneExceedsBudgetTest()
        {
            // Descriptor by itself exceeds the 64KB unbounded budget:
            // Array<UInt0>[16385] = 65540 bytes > 64KB. Value is Null.
            int count = (64 * 1024 / 4) + 1;
            byte[] descriptor = ZeroWidthArrayPayload(count, 0x43);

            byte[] payload = new byte[1 + descriptor.Length + 1];
            int off = 0;
            payload[off++] = 0x00;                       // Described format code
            Buffer.BlockCopy(descriptor, 0, payload, off, descriptor.Length);
            off += descriptor.Length;
            payload[off] = 0x40;                         // value = Null

            ByteBuffer buffer = new ByteBuffer(payload, 0, payload.Length, payload.Length);
            DescribedValue dv = new DescribedValue(null, null);
            try
            {
                dv.Decode(buffer);
                Assert.IsTrue(false, "Expected AmqpException with decode-error.");
            }
            catch (AmqpException ex)
            {
                Assert.AreEqual((Symbol)ErrorCode.DecodeError, ex.Error.Condition);
            }
        }

        [TestMethod]
        public void DecoderDescribedDescriptorTrackedTest()
        {
            // Described.Decode(buffer) — the public API on a user-instantiated
            // Described — must apply the same size/count checks to the descriptor
            // as to the value. Craft descriptor = Array<UInt0>[10000] (40000 bytes)
            // and value = Array<UInt0>[10000] (40000 bytes). Each fits under the
            // 64KB budget alone; combined 80000 > 64KB. Codec.Decode rejects it
            // (both are accounted); a user-instantiated Described.Decode must
            // reject it too.
            int count = 10000;
            byte[] descriptor = ZeroWidthArrayPayload(count, 0x43);
            byte[] value = ZeroWidthArrayPayload(count, 0x43);

            byte[] payload = new byte[1 + descriptor.Length + value.Length];
            int off = 0;
            payload[off++] = 0x00;                       // Described format code
            Buffer.BlockCopy(descriptor, 0, payload, off, descriptor.Length);
            off += descriptor.Length;
            Buffer.BlockCopy(value, 0, payload, off, value.Length);

            ByteBuffer buffer = new ByteBuffer(payload, 0, payload.Length, payload.Length);
            DescribedValue dv = new DescribedValue(null, null);
            try
            {
                dv.Decode(buffer);
                Assert.IsTrue(false, "Expected AmqpException with decode-error.");
            }
            catch (AmqpException ex)
            {
                Assert.AreEqual((Symbol)ErrorCode.DecodeError, ex.Error.Condition);
            }
        }

        // --- Helpers -----------------------------------------------------

        static byte[] ZeroWidthArrayPayload(int count, byte elementFormatCode)
        {
            byte[] payload = new byte[1 + 4 + 4 + 1];
            int off = 0;
            payload[off++] = 0xF0;
            WriteInt(payload, ref off, 5);           // size = count(4) + inner-fc(1)
            WriteInt(payload, ref off, count);
            payload[off] = elementFormatCode;
            return payload;
        }

        static void WriteInt(byte[] dst, ref int offset, int value)
        {
            dst[offset++] = (byte)(value >> 24);
            dst[offset++] = (byte)(value >> 16);
            dst[offset++] = (byte)(value >> 8);
            dst[offset++] = (byte)value;
        }

        static void AssertDecodeError(byte[] payload)
        {
            ByteBuffer buffer = new ByteBuffer(payload, 0, payload.Length, payload.Length);
            try
            {
                Encoder.ReadObject(buffer);
                Assert.IsTrue(false, "Expected AmqpException with decode-error.");
            }
            catch (AmqpException ex)
            {
                Assert.AreEqual((Symbol)ErrorCode.DecodeError, ex.Error.Condition);
            }
        }
    }
}
