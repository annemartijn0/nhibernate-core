using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NHibernate.AdoNet.AsyncExtensions.AsyncHandler
{
    public interface IHandler<in TSource, out TDestination>
    {
		/// <summary>
		/// Next handler in chain of responsibility
		/// </summary>
		IHandler<TSource, TDestination> Successor { get; }

		/// <summary>
		/// Let the current <see cref="IHandler{TSource, TDestination}"/> handle the request.
		/// Will probably pass <param name="source" /> to successor if current 
		/// <see cref="IHandler{TSource, TDestination}"/> cannot handle the request.
		/// </summary>
		/// <param name="source">Item to handle.</param>
		/// <returns>The result of this chain.</returns>
		TDestination Handle(TSource source);
    }
}
