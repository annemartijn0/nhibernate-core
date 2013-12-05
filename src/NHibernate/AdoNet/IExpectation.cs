using System;
using System.Data;using System.Data.Common;

namespace NHibernate.AdoNet
{
	public interface IExpectation
	{
		void VerifyOutcomeNonBatched(int rowCount, DbCommand statement);
		bool CanBeBatched { get; }

		/// <summary>
		/// Expected row count. Valid only for batchable expectations.
		/// </summary>
		int ExpectedRowCount { get; }
	}
}