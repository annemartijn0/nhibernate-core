using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace NHibernate
{
	public interface IAwaitableEnumerable<T> : IEnumerable<T>
	{
		/// <summary>
		/// Asynchronously queries the database an creates an 
		/// <see cref="IEnumerable{T}" /> from results.
		/// </summary>
		/// <returns>
		/// A <see cref="Task" /> containing a <see cref="IEnumerable{T}" /> 
		/// that contains elements that comply with the criteria
		/// </returns>
		Task<IEnumerable<T>> AsTask();

		/// <summary>
		/// Asynchronously queries the database an creates an 
		/// <see cref="IEnumerable{T}" /> from results.
		/// </summary>
		/// <param name="cancellationToken">Token to cancel the request.</param>
		/// <returns>
		/// A <see cref="Task" /> containing a <see cref="IEnumerable{T}" /> 
		/// that contains elements that comply with the criteria
		/// </returns>
		Task<IEnumerable<T>> AsTask(CancellationToken cancellationToken);
	}
}
