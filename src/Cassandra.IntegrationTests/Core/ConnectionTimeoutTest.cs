﻿using NUnit.Framework;
using System;
using System.Diagnostics;

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture, Category("short")]
    public class ConnectionTimeoutTest
    {
        [Test]
        public void ConnectionDroppingTimeoutTest()
        {
            var originalTraceLevel = Diagnostics.CassandraTraceSwitch.Level;
            Diagnostics.CassandraTraceSwitch.Level = TraceLevel.Verbose;
            var sw = Stopwatch.StartNew();
            var thrown = false;
            try
            {
                var builder = new Builder().WithDefaultKeyspace("system")
                                           .AddContactPoints("1.1.1.1") // IP address that drops (not rejects !) the inbound connection
                                           .WithQueryTimeout(500);
                builder.SocketOptions.SetIdleTimeout(TimeSpan.FromMilliseconds(500));
                var cluster = builder.Build();
                cluster.Connect();
            }
            catch (NoHostAvailableException)
            {
                thrown = true;
            }

            sw.Stop();

            Assert.True(thrown, "Expected exception");
            Assert.True(sw.Elapsed.TotalMilliseconds < 1000, "The connection timeout was not respected");

            Diagnostics.CassandraTraceSwitch.Level = originalTraceLevel;
        }

        [Test]
        public void ConnectionRejectingTimeoutTest()
        {
            var originalTraceLevel = Diagnostics.CassandraTraceSwitch.Level;
            Diagnostics.CassandraTraceSwitch.Level = TraceLevel.Verbose;
            var sw = Stopwatch.StartNew();
            var thrown = false;
            try
            {
                var builder = new Builder().WithDefaultKeyspace("system")
                                           .AddContactPoints(Options.Default.IP_PREFIX + "1") // IP address that rejects
                                           .WithQueryTimeout(500);
                builder.SocketOptions.SetIdleTimeout(TimeSpan.FromMilliseconds(500));
                var cluster = builder.Build();
                cluster.Connect();
            }
            catch (NoHostAvailableException)
            {
                thrown = true;
            }

            sw.Stop();

            Assert.True(thrown, "Expected exception");
            Assert.True(sw.Elapsed.TotalMilliseconds < 1000, "The connection timeout was not respected");

            Diagnostics.CassandraTraceSwitch.Level = originalTraceLevel;
        }
    }
}