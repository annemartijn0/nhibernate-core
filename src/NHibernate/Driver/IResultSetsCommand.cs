using System.Data;using System.Data.Common;
using System.Threading.Tasks;
using NHibernate.SqlCommand;

namespace NHibernate.Driver
{
	public interface IResultSetsCommand
	{
		void Append(ISqlCommand command);
		bool HasQueries { get; }
		SqlString Sql { get; }
		Task<DbDataReader> GetReaderAsync(int? commandTimeout);
		DbDataReader GetReader(int? commandTimeout);
	}
}