using System.Threading;
using System.Threading.Tasks;

namespace NHibernate
{
	public interface IFutureValue<T>
	{
		T Value { get; }

		Task<T> ValueAsync();

		Task<T> ValueAsync(CancellationToken cancellationToken);
	}
}