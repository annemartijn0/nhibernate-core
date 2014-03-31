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

		public IHandler<DbCommand, Task<DbDataReader>> Successor
		{
			get { return _successor; }
		}

		public static IHandler<DbCommand, Task<DbDataReader>> FirstOfChain
		{
			get
			{
				var dbClientExecuteReaderAsyncHandler = new DbClientExecuteReaderAsyncHandler();
				return new SqlClientExecuteReaderAsyncHandler(dbClientExecuteReaderAsyncHandler);
			}
		}

		protected ExecuteReaderAsyncHandler() { }

		/// <summary>
		/// Constructor for setting successor in chain of responsibility
		/// </summary>
		/// <param name="successor">next handler in chain of responsibility</param>
		protected ExecuteReaderAsyncHandler(IHandler<DbCommand, Task<DbDataReader>> successor)
		{
			_successor = successor;
		}

		public Task<DbDataReader> Handle(DbCommand dbCommand)
		{
			if (dbCommand == null)
				throw new ArgumentNullException("dbCommand");

			if (CanHandleCommand(dbCommand))
				return ExecuteReaderAsync();

			if (_successor == null)
				throw new NotSupportedException("ExecuteReaderAsync() could not find matching handler");

			return _successor.Handle(dbCommand);
		}

		protected abstract bool CanHandleCommand(DbCommand dbCommand);

		protected abstract Task<DbDataReader> ExecuteReaderAsync();
	}
}
