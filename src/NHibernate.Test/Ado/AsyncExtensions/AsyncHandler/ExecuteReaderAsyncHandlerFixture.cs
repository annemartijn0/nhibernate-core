using System.Collections;
using System.Data.Odbc;
using System.Data.SqlClient;
using Moq;
using NHibernate.AdoNet.AsyncExtensions.AsyncHandler;
using NHibernate.Cfg;
using NUnit.Framework;
using System;
using System.Data.Common;
using System.Threading.Tasks;

namespace NHibernate.Test.Ado.AsyncExtensions.AsyncHandler
{
    [TestFixture]
    public class ExecuteReaderAsyncHandlerFixture : TestCase
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
            configuration.SetProperty(NHibernate.Cfg.Environment.FormatSql, "true");
            configuration.SetProperty(NHibernate.Cfg.Environment.GenerateStatistics, "true");
            configuration.SetProperty(NHibernate.Cfg.Environment.BatchSize, "10");
        }

        [Test, Description("ExecuteReaderAsyncHandler.Handle() should not take null arguments")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void ExecuteReaderAsyncHandlerHandle_ShouldNotTakeZeroArgument()
        {
            // Arrange
            var handlerMock = new Mock<IHandler<DbCommand, Task<DbDataReader>>>();
            var target = new SqlClientExecuteReaderAsyncHandler(handlerMock.Object);

            // Act
            target.Handle(null);

            // Assert: Expected Exception ArgumentNullException
        }

        [Test, Description("ExecuteReaderAsyncHandler.Handle() should throw NotSupportedException if successor is null")]
        [ExpectedException(typeof(NotSupportedException))]
        public void ExecuteReaderAsyncHandlerHandle_ShouldThrowNotSupportedException_IfSuccessorIsNull()
        {
            // Arrange
            var target = new SqlClientExecuteReaderAsyncHandler(null);

            // Act
            target.Handle(new OdbcCommand());

            // Assert: Expected Exception NotSupportedException
        }

        [Test, Description("DbClientExecuteReaderAsyncHandler.Handle() should return a completed task")]
        public void DbClientExecuteReaderAsyncHandlerHandle_ShouldReturnCompletedTask()
        {
            // Arrange
            Task<DbDataReader> result;
            var target = new DbClientExecuteReaderAsyncHandler();
            const string queryString = "SELECT * FROM dbo.VerySimple;";
            using (var connection = sessions.ConnectionProvider.GetConnection() as SqlConnection)
            {
                if (connection == null)
                    Assert.Ignore("This test is for SqlConnection only");

                using (var dbCommand = new System.Data.SqlClient.SqlCommand(queryString, connection))
                {
                    // Act
                    result = target.Handle(dbCommand);
                }
            }

            // Assert
            Assert.That(result.IsCompleted);
        }

        [Test, Description("SqlClientExecuteReaderAsyncHandler.Handle() should return a running task")]
        public void SqlClientExecuteReaderAsyncHandlerHandle_ShouldReturnRunningTask()
        {
            // Arrange
            var target = new DbClientExecuteReaderAsyncHandler();
            const string queryString = "SELECT * FROM dbo.VerySimple;";
            using (var connection = sessions.ConnectionProvider.GetConnection() as SqlConnection)
            {
                if (connection == null)
                    Assert.Ignore("This test is for SqlConnection only");

                using (var dbCommand = new System.Data.SqlClient.SqlCommand(queryString, connection))
                {
                    // Act
                    var result = target.Handle(dbCommand);

                    // Assert
                    Assert.That(result.Status == TaskStatus.Running || result.Status == TaskStatus.RanToCompletion);
                }
            }
        }

        [Test, Description("SqlClientExecuteReaderAsyncHandler.Handle() should call successor.Handle() if dbCommand is not SqlCommand")]
        public void SqlClientExecuteReaderAsyncHandlerHandle_ShouldCallSuccessorIfNotSqlCommand()
        {
            // Arrange
            var handlerMock = new Mock<IHandler<DbCommand, Task<DbDataReader>>>();
            var target = new SqlClientExecuteReaderAsyncHandler(handlerMock.Object);
            var dbCommand = new OdbcCommand();

            // Act
            target.Handle(dbCommand);

            // Assert
            handlerMock.Verify(x => x.Handle(dbCommand), Times.Once);
        }
    }
}
