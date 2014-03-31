using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using NHibernate.AdoNet.AsyncExtensions.AsyncHandler;

namespace NHibernate.AdoNet.AsyncExtensions
{
    /// <summary>
    /// Extensions for <see cref="DbCommand"/> to provide asynchronous operations in .NET 4.0
    /// </summary>
    public static class DbCommandExtensions
    {
        /// <summary>
        /// This is the asynchronous version of System.Data.Common.DbDataReader.Read().
        /// The cancellationToken may optionally be ignored.
        /// The default implementation invokes the synchronous <see cref="System.Data.Common.DbDataReader.Read()"/> method
        /// and returns a completed task, blocking the calling thread.
        /// Will return a cancelled task if passed an already cancelled cancellationToken.
        /// Exceptions thrown by Read will be communicated via the returned Task Exception property.
        /// Do not invoke other methods and properties of the DbDataReader object until
        /// the returned Task is complete.
        /// </summary>
        /// <param name="dbCommand">The DbCommand that is extended</param>
        /// <param name="cancellationToken">The cancellation instruction.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        public static Task<DbDataReader> ExecuteReaderAsync(this DbCommand dbCommand, CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
                return CreatedTaskWithCancellation();

	        return ExecuteReaderAsyncHandler.FirstOfChain.Handle(dbCommand);
        }

        private static Task<DbDataReader> CreatedTaskWithCancellation()
        {
            var taskCompletionSource = new TaskCompletionSource<DbDataReader>();
            taskCompletionSource.SetCanceled();
            return taskCompletionSource.Task;
        }
    }
}