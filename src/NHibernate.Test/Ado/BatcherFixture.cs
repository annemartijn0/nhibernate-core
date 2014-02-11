using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Reflection;
using System.Threading.Tasks;
using Moq;
using NHibernate.AdoNet;
using NHibernate.AdoNet.Util;
using NHibernate.Cfg;
using NHibernate.Driver;
using NHibernate.Test.NHSpecificTest.NH2189;
using NUnit.Framework;
using Environment = NHibernate.Cfg.Environment;
using Task = System.Threading.Tasks.Task;

namespace NHibernate.Test.Ado
{
    [TestFixture]
    public class BatcherFixture : TestCase
    {
        protected override string MappingsAssembly
        {
            get { return "NHibernate.Test"; }
        }

        protected override IList Mappings
        {
            get { return new[] { "Ado.VerySimple.hbm.xml", "Ado.AlmostSimple.hbm.xml" }; }
        }

        protected override void Configure(Configuration configuration)
        {
            configuration.SetProperty(Environment.FormatSql, "true");
            configuration.SetProperty(Environment.GenerateStatistics, "true");
            configuration.SetProperty(Environment.BatchSize, "10");
        }

        protected override bool AppliesTo(Engine.ISessionFactoryImplementor factory)
        {
            return !(factory.Settings.BatcherFactory is NonBatchingBatcherFactory);
        }

        [Test, Description("The ExecuteReader method should return NHybridDataReader and should not return null")]
        public void ExecuteReader_ShouldReturn_NHybridDataReader()
        {
            if (sessions.Settings.BatcherFactory is SqlClientBatchingBatcherFactory == false)
                Assert.Ignore("This test is for SqlClientBatchingBatcher only");

            // Arrange
            IDataReader result;

            using (ISession s = sessions.OpenSession())
            {
                var target = new SqlClientBatchingBatcher(s.GetSessionImplementation().ConnectionManager, null);

                // Act
                result = target.ExecuteReader(new Mock<IDbCommand>().Object);
            }

            // Assert
            Assert.IsNotNull(result);
            Assert.IsInstanceOf(typeof(NHybridDataReader), result);
        }

        [Test, Description("The ExecuteReader method should get connection property of command")]
        public void ExecuteReader_ShouldCall_Command_Connection()
        {
            if (sessions.Settings.BatcherFactory is SqlClientBatchingBatcherFactory == false)
                Assert.Ignore("This test is for SqlClientBatchingBatcher only");

            // Arrange
            var dbCommandMock = new Mock<IDbCommand>();

            using (ISession s = sessions.OpenSession())
            {
                var target = new SqlClientBatchingBatcher(s.GetSessionImplementation().ConnectionManager, null);

                // Act
                target.ExecuteReader(dbCommandMock.Object);
            }

            // Assert
            dbCommandMock.Verify(x => x.Connection, Times.Once);
        }

        [Test, Description("The ExecuteReader method should call ExecuteReader on command")]
        public void ExecuteReader_ShouldCall_Command_ExecuteReader()
        {
            if (sessions.Settings.BatcherFactory is SqlClientBatchingBatcherFactory == false)
                Assert.Ignore("This test is for SqlClientBatchingBatcher only");

            // Arrange
            var dbCommandMock = new Mock<IDbCommand>();

            using (ISession s = sessions.OpenSession())
            {
                var target = new SqlClientBatchingBatcher(s.GetSessionImplementation().ConnectionManager, null);

                // Act
                target.ExecuteReader(dbCommandMock.Object);
            }

            // Assert
            dbCommandMock.Verify(x => x.ExecuteReader(), Times.Once);
        }

        [Test, Description("The ExecuteReader method should catch Exceptions and add extra details to it")]
        public void ExecuteReader_ShouldCatchExceptionAndAddDetails()
        {
            if (sessions.Settings.BatcherFactory is SqlClientBatchingBatcherFactory == false)
                Assert.Ignore("This test is for SqlClientBatchingBatcher only");

            // Arrange
            var dbCommandMock = new Mock<IDbCommand>();
            dbCommandMock.Setup(x => x.ExecuteReader()).Throws(new Exception());

            using (ISession s = sessions.OpenSession())
            {
                var target = new SqlClientBatchingBatcher(s.GetSessionImplementation().ConnectionManager, null);

                try
                {
                    // Act
                    target.ExecuteReader(dbCommandMock.Object);
                }
                catch (Exception e)
                {
                    // Assert
                    Assert.IsTrue(e.Data.Contains("actual-sql-query"));
                }
            }
        }

        [Test, Description("The ExecuteReader method should add the returned reader to readers to close")]
        public void ExecuteReader_ShouldAddReaderToReadersToClose()
        {
            if (sessions.Settings.BatcherFactory is SqlClientBatchingBatcherFactory == false)
                Assert.Ignore("This test is for SqlClientBatchingBatcher only");

            // Arrange
            const string queryString = "SELECT * FROM dbo.VerySimple;";

            using (ISession s = sessions.OpenSession())
            {
                var target =
                    new SqlClientBatchingBatcher(s.GetSessionImplementation().ConnectionManager, null) as
                        AbstractBatcher;
                var readersToClose = GetReadersToClose(target);

                // Act
                var result = target.ExecuteReader(new System.Data.SqlClient.SqlCommand
                    (queryString, s.Connection as System.Data.SqlClient.SqlConnection));

                // Assert
                Assert.IsTrue(readersToClose.Contains(result));
            }
        }

        [Test, Description("The ExecuteReaderAsync method should only take sqlcommands")]
        [ExpectedException(typeof(NotSupportedException))]
        public void ExecuteReaderAsync_ShouldOnlyTake_SqlCommand()
        {
            if (sessions.Settings.BatcherFactory is SqlClientBatchingBatcherFactory == false)
                Assert.Ignore("This test is for SqlClientBatchingBatcher only");

            // Arrange
            var dbCommandMock = new Mock<IDbCommand>();

            using (ISession s = sessions.OpenSession())
            {
                var target = new SqlClientBatchingBatcher(s.GetSessionImplementation().ConnectionManager, null) as AbstractBatcher;

                // Act
                try
                {
                    target.ExecuteReaderAsync(dbCommandMock.Object).Wait();
                }
                catch (AggregateException aggregateException)
                {
                    HandleExecuteReaderAsyncExceptions(aggregateException);
                }
            }

            // Assert
            // Expected Exception: NotSupportedException
        }

        private static void HandleExecuteReaderAsyncExceptions(AggregateException aggregateException)
        {
            foreach (var exception in aggregateException.InnerExceptions)
            {
                if (exception is NotSupportedException)
                {
                    throw exception;
                }
            }
        }

        [Test]
        [Description("The ExecuteReaderAsync method should return NHybridDataReader and should not return null")]
        public void ExecuteReaderAsync_ShouldReturn_NHybridDataReader()
        {
            if (sessions.Settings.BatcherFactory is SqlClientBatchingBatcherFactory == false)
                Assert.Ignore("This test is for SqlClientBatchingBatcher only");

            // Arrange
            IDataReader result;
            const string queryString = "SELECT * FROM dbo.VerySimple;";

            using (ISession s = sessions.OpenSession())
            {
                var target = new SqlClientBatchingBatcher(s.GetSessionImplementation().ConnectionManager, null) as AbstractBatcher;

                // Act
                result = target.ExecuteReaderAsync(new System.Data.SqlClient.SqlCommand
                    (queryString, s.Connection as System.Data.SqlClient.SqlConnection)).Result;
            }

            // Assert
            Assert.IsNotNull(result);
            Assert.IsInstanceOf(typeof(NHybridDataReader), result);
        }

        [Test, Description("The ExecuteReaderAsync method should return working task")]
        public void ExecuteReaderAsync_ShouldReturn_WorkingTask()
        {
            if (sessions.Settings.BatcherFactory is SqlClientBatchingBatcherFactory == false)
                Assert.Ignore("This test is for SqlClientBatchingBatcher only");

            // Arrange
            const string queryString = "SELECT * FROM dbo.VerySimple;";

            using (ISession s = sessions.OpenSession())
            {
                var target =
                    new SqlClientBatchingBatcher(s.GetSessionImplementation().ConnectionManager, null) as
                        AbstractBatcher;

                // Act
                var result = target.ExecuteReaderAsync(new System.Data.SqlClient.SqlCommand
                    (queryString, s.Connection as System.Data.SqlClient.SqlConnection));

                // Assert
                Assert.IsTrue(result.Status != TaskStatus.Canceled);
                Assert.IsTrue(result.Status != TaskStatus.Faulted);

                result.Wait();
            }
        }

        [Test, Description("The ExecuteReaderAsync method should add the returned reader to readers to close")]
        public void ExecuteReaderAsync_ShouldAddReaderToReadersToClose()
        {
            if (sessions.Settings.BatcherFactory is SqlClientBatchingBatcherFactory == false)
                Assert.Ignore("This test is for SqlClientBatchingBatcher only");

            // Arrange
            const string queryString = "SELECT * FROM dbo.VerySimple;";

            using (ISession s = sessions.OpenSession())
            {
                var target =
                    new SqlClientBatchingBatcher(s.GetSessionImplementation().ConnectionManager, null) as
                        AbstractBatcher;
                var readersToClose = GetReadersToClose(target);

                // Act
                var result = target.ExecuteReaderAsync(new System.Data.SqlClient.SqlCommand
                    (queryString, s.Connection as System.Data.SqlClient.SqlConnection))
                    .Result;

                // Assert
                Assert.IsTrue(readersToClose.Contains(result));
            }
        }

        [Test, Description("The ExecuteReaderAsync method should add the returned reader to readers to close")]
        public void ExecuteReaderAsync_MultipleTimes()
        {
            if (sessions.Settings.BatcherFactory is SqlClientBatchingBatcherFactory == false)
                Assert.Ignore("This test is for SqlClientBatchingBatcher only");

            // Arrange
            const string queryString = "SELECT * FROM dbo.VerySimple;";
            const int numberOfTasks = 4;
            var tasks = new Task[numberOfTasks];

            using (ISession s = sessions.OpenSession())
            {
                var target =
                    new SqlClientBatchingBatcher(s.GetSessionImplementation().ConnectionManager, null) as
                        AbstractBatcher;
                var readersToClose = GetReadersToClose(target);

                // Act
                for (int i = 0; i < numberOfTasks; i++)
                {
                    tasks[i] = target.ExecuteReaderAsync(new System.Data.SqlClient.SqlCommand
                        (queryString, s.Connection as System.Data.SqlClient.SqlConnection));
                }

                Task.WaitAll(tasks);

                // Assert
                Assert.AreEqual(numberOfTasks, readersToClose.Count);
            }
        }

        private static HashSet<IDataReader> GetReadersToClose(AbstractBatcher target)
        {
            const string fieldname = "_readersToClose";
            var abstractBatcherType = typeof(AbstractBatcher);

            var field = abstractBatcherType
                .GetField(fieldname, BindingFlags.Instance | BindingFlags.NonPublic);

            if (field == null)
                Assert.Fail("{0}.{1} does not exist anymore, " +
                            "if you renamed this, you should adjust this test accordingly"
                            , abstractBatcherType.ToString(), fieldname);

            return field.GetValue(target) as HashSet<IDataReader>;
        }

        [Test]
        [Description("The batcher should run all INSERT queries in only one roundtrip.")]
        public void OneRoundTripInserts()
        {
            sessions.Statistics.Clear();
            FillDb();

            Assert.That(sessions.Statistics.PrepareStatementCount, Is.EqualTo(1));
            Cleanup();
        }

        private void Cleanup()
        {
            using (ISession s = sessions.OpenSession())
            using (s.BeginTransaction())
            {
                s.CreateQuery("delete from VerySimple").ExecuteUpdate();
                s.CreateQuery("delete from AlmostSimple").ExecuteUpdate();
                s.Transaction.Commit();
            }
        }

        private void FillDb()
        {
            using (ISession s = sessions.OpenSession())
            using (ITransaction tx = s.BeginTransaction())
            {
                s.Save(new VerySimple { Id = 1, Name = "Fabio", Weight = 119.5 });
                s.Save(new VerySimple { Id = 2, Name = "Fiamma", Weight = 9.8 });
                tx.Commit();
            }
        }

        [Test]
        [Description("The batcher should run all UPDATE queries in only one roundtrip.")]
        public void OneRoundTripUpdate()
        {
            FillDb();

            using (ISession s = sessions.OpenSession())
            using (ITransaction tx = s.BeginTransaction())
            {
                var vs1 = s.Get<VerySimple>(1);
                var vs2 = s.Get<VerySimple>(2);
                vs1.Weight -= 10;
                vs2.Weight -= 1;
                sessions.Statistics.Clear();
                s.Update(vs1);
                s.Update(vs2);
                tx.Commit();
            }

            Assert.That(sessions.Statistics.PrepareStatementCount, Is.EqualTo(1));
            Cleanup();
        }

        [Test, Ignore("Not fixed yet.")]
        [Description("SqlClient: The batcher should run all different INSERT queries in only one roundtrip.")]
        public void SqlClientOneRoundTripForUpdateAndInsert()
        {
            if (sessions.Settings.BatcherFactory is SqlClientBatchingBatcherFactory == false)
                Assert.Ignore("This test is for SqlClientBatchingBatcher only");

            FillDb();

            using (var sqlLog = new SqlLogSpy())
            using (ISession s = sessions.OpenSession())
            using (ITransaction tx = s.BeginTransaction())
            {
                s.Save(new VerySimple
                {
                    Name = "test441",
                    Weight = 894
                });

                s.Save(new AlmostSimple
                {
                    Name = "test441",
                    Weight = 894
                });

                tx.Commit();

                var log = sqlLog.GetWholeLog();
                //log should only contain NHibernate.SQL once, because that means 
                //that we ony generated a single batch (NHibernate.SQL log will output
                //once per batch)
                Assert.AreEqual(0, log.IndexOf("NHibernate.SQL"), "log should start with NHibernate.SQL");
                Assert.AreEqual(-1, log.IndexOf("NHibernate.SQL", "NHibernate.SQL".Length), "NHibernate.SQL should only appear once in the log");
            }

            Cleanup();
        }

        [Test]
        [Description("SqlClient: The batcher log output should be formatted")]
        public void BatchedoutputShouldBeFormatted()
        {
            if (sessions.Settings.BatcherFactory is SqlClientBatchingBatcherFactory == false)
                Assert.Ignore("This test is for SqlClientBatchingBatcher only");

            using (var sqlLog = new SqlLogSpy())
            {
                FillDb();
                var log = sqlLog.GetWholeLog();
                Assert.IsTrue(log.Contains("INSERT \n    INTO"));
            }

            Cleanup();
        }


        [Test]
        [Description("The batcher should run all DELETE queries in only one roundtrip.")]
        public void OneRoundTripDelete()
        {
            FillDb();

            using (ISession s = sessions.OpenSession())
            using (ITransaction tx = s.BeginTransaction())
            {
                var vs1 = s.Get<VerySimple>(1);
                var vs2 = s.Get<VerySimple>(2);
                sessions.Statistics.Clear();
                s.Delete(vs1);
                s.Delete(vs2);
                tx.Commit();
            }

            Assert.That(sessions.Statistics.PrepareStatementCount, Is.EqualTo(1));
            Cleanup();
        }

        [Test]
        [Description(@"Activating the SQL and turning off the batcher's log the log stream:
-should not contains adding to batch
-should contain batch command
-the batcher should work.")]
        public void SqlLog()
        {
            using (new LogSpy(typeof(AbstractBatcher), true))
            {
                using (var sl = new SqlLogSpy())
                {
                    sessions.Statistics.Clear();
                    FillDb();
                    string logs = sl.GetWholeLog();
                    Assert.That(logs, Is.Not.StringContaining("Adding to batch").IgnoreCase);
                    Assert.That(logs, Is.StringContaining("Batch command").IgnoreCase);
                    Assert.That(logs, Is.StringContaining("INSERT").IgnoreCase);
                }
            }

            Assert.That(sessions.Statistics.PrepareStatementCount, Is.EqualTo(1));
            Cleanup();
        }

        [Test]
        [Description(@"Activating the AbstractBatcher's log the log stream:
-should not contains batch info 
-should contain SQL log info only regarding batcher (SQL log should not be duplicated)
-the batcher should work.")]
        public void AbstractBatcherLog()
        {
            using (new LogSpy(typeof(AbstractBatcher)))
            {
                using (var sl = new SqlLogSpy())
                {
                    sessions.Statistics.Clear();
                    FillDb();
                    string logs = sl.GetWholeLog();
                    Assert.That(logs, Is.StringContaining("batch").IgnoreCase);
                    foreach (var loggingEvent in sl.Appender.GetEvents())
                    {
                        string message = loggingEvent.RenderedMessage;
                        if (message.ToLowerInvariant().Contains("insert"))
                        {
                            Assert.That(message, Is.StringContaining("batch").IgnoreCase);
                        }
                    }
                }
            }

            Assert.That(sessions.Statistics.PrepareStatementCount, Is.EqualTo(1));
            Cleanup();
        }

        [Test]
        public void SqlLogShouldGetBatchCommandNotification()
        {
            using (new LogSpy(typeof(AbstractBatcher)))
            {
                using (var sl = new SqlLogSpy())
                {
                    sessions.Statistics.Clear();
                    FillDb();
                    string logs = sl.GetWholeLog();
                    Assert.That(logs, Is.StringContaining("Batch commands:").IgnoreCase);
                }
            }

            Assert.That(sessions.Statistics.PrepareStatementCount, Is.EqualTo(1));
            Cleanup();
        }

        [Test]
        [Description(@"Activating the AbstractBatcher's log the log stream:
-should contain well formatted SQL log info")]
        public void AbstractBatcherLogFormattedSql()
        {
            using (new LogSpy(typeof(AbstractBatcher)))
            {
                using (var sl = new SqlLogSpy())
                {
                    sessions.Statistics.Clear();
                    FillDb();
                    foreach (var loggingEvent in sl.Appender.GetEvents())
                    {
                        string message = loggingEvent.RenderedMessage;
                        if (message.StartsWith("Adding"))
                        {
                            // should be the line with the formatted SQL
                            var strings = message.Split(System.Environment.NewLine.ToCharArray());
                            foreach (var sqlLine in strings)
                            {
                                if (sqlLine.Contains("p0"))
                                {
                                    Assert.That(sqlLine, Is.StringContaining("p1"));
                                    Assert.That(sqlLine, Is.StringContaining("p2"));
                                }
                            }
                        }
                    }
                }
            }

            Assert.That(sessions.Statistics.PrepareStatementCount, Is.EqualTo(1));
            Cleanup();
        }
    }
}