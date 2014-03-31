using System;
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
