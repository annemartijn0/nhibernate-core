using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NHibernate.AdoNet.AsyncExtensions.AsyncHandler
{
    public class DbClientExecuteReaderAsyncHandler : ExecuteReaderAsyncHandler
    {
        private DbCommand _dbCommand;

        public DbClientExecuteReaderAsyncHandler() : base(null) { }

        protected override bool CanHandle(DbCommand dbCommand)
        {
            _dbCommand = dbCommand;
            return true;
        }

        protected override Task<DbDataReader> ExecuteReaderAsync()
        {
            var taskCompletionSource = new TaskCompletionSource<DbDataReader>();
            taskCompletionSource.SetResult(_dbCommand.ExecuteReader());
            return taskCompletionSource.Task;
        }
    }
}
