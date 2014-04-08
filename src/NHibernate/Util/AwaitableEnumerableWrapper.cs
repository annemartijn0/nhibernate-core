using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace NHibernate.Util
{
	public class AwaitableEnumerableWrapper<T> : IAwaitableEnumerable<T>
	{
		private readonly IEnumerable<T> _source;

		public AwaitableEnumerableWrapper(IEnumerable<T> source)
		{
			_source = source;
		}

		public Task<IEnumerable<T>> AsTask()
		{
			return AsTask(CancellationToken.None);
		}

		public Task<IEnumerable<T>> AsTask(CancellationToken cancellationToken)
		{
			var taskCompletionSource = new TaskCompletionSource<IEnumerable<T>>();

			if (cancellationToken.IsCancellationRequested)
				taskCompletionSource.SetCanceled();
			else
				taskCompletionSource.SetResult(_source);

			return taskCompletionSource.Task;
		}

		public IEnumerator<T> GetEnumerator()
		{
			return _source.GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return ((IEnumerable)_source).GetEnumerator();
		}
	}
}
