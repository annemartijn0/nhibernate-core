using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NHibernate.AdoNet.AsyncExtensions.AsyncHandler
{
    public abstract class ExecuteReaderAsyncHandler : IHandler<DbCommand, Task<DbDataReader>>
    {
        private readonly IHandler<DbCommand, Task<DbDataReader>> _successor;

        protected ExecuteReaderAsyncHandler(IHandler<DbCommand, Task<DbDataReader>> successor)
        {
            _successor = successor;
        }

        public Task<DbDataReader> Handle(DbCommand dbCommand)
        {
            if (dbCommand == null)
                throw new ArgumentNullException("dbCommand");

            if (CanHandle(dbCommand))
                return ExecuteReaderAsync();

            if (_successor == null)
                throw new NotSupportedException("ExecuteReaderAsync() could not find matching handler");
            else
                return _successor.Handle(dbCommand);
        }

        protected abstract bool CanHandle(DbCommand dbCommand);

        protected abstract Task<DbDataReader> ExecuteReaderAsync();
    }
}
