using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Moq;
using NHibernate.AdoNet.AsyncExtensions;
using NHibernate.AdoNet.AsyncExtensions.AsyncHandler;
using NHibernate.Test.NHSpecificTest.NH2189;
using NUnit.Framework;

namespace NHibernate.Test.Ado.AsyncExtensions
{
    [TestFixture]
    public class DbCommandExtensionsFixture
    {
        [Test, Description("ExecuteReaderAsync should call IExecuteReaderAsyncProcessor.Process()")]
        public void ExecuteReaderAsync_ShouldCall_IExecuteReaderAsyncProcessorProcess()
        {
            // Arrange
            var target = new System.Data.SqlClient.SqlCommand();
            var mock = new Mock<IHandler<DbCommand, Task<DbDataReader>>>();
            DbCommandExtensions.ProcessExecuteReaderAsync = (dbCommand, processor) => mock.Object.Handle(dbCommand);

            // Act
            var result = target.ExecuteReaderAsync(CancellationToken.None);

            // Assert
            mock.Verify(x => x.Handle(target), Times.Once);
        }

        [Test, Description("The Func delegate ProcessExecuteReaderAsync should have an implementation")]
        public void ProcessExecuteReaderAsync_ShouldBeSet()
        {
            // Assert
            Assert.IsNotNull(DbCommandExtensions.ProcessExecuteReaderAsync);
        }

        [Test, Description("ExecuteReaderAsync should return canceled Task if CancelationToken is canceled")]
        public void ExecuteReaderAsync_ShouldReturnCanceledTask_IfCancelationTokenCanceled()
        {
            // Arrange
            var target = new System.Data.SqlClient.SqlCommand();
            var cancellationToken = new CancellationTokenSource();
            cancellationToken.Cancel();

            // Act
            var result = target.ExecuteReaderAsync(cancellationToken.Token);

            // Assert
            Assert.That(result.IsCanceled);
        }
    }
}
