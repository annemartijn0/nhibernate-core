using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NHibernate.AdoNet.AsyncExtensions.AsyncHandler
{
	public class SqlClientExecuteReaderAsyncHandler : ExecuteReaderAsyncHandler
	{
		private System.Data.SqlClient.SqlCommand _sqlCommand;

		public SqlClientExecuteReaderAsyncHandler(IHandler<DbCommand, Task<DbDataReader>> successor)
			: base(successor) { }

		protected override bool CanHandleCommand(DbCommand dbCommand)
		{
			_sqlCommand = dbCommand as System.Data.SqlClient.SqlCommand;
			return _sqlCommand != null;
		}

		protected override Task<DbDataReader> ExecuteReaderAsync()
		{
			return Task<DbDataReader>.Factory
				.FromAsync(_sqlCommand.BeginExecuteReader, _sqlCommand.EndExecuteReader, null);
		}
	}
}
