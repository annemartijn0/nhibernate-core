using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NHibernate.Util
{
	public static class TaskExtensions
	{
		public static Task<T> CanceledTask<T>(this Task task)
		{
			var taskCompletionSource = new TaskCompletionSource<T>();
			taskCompletionSource.SetCanceled();
			return taskCompletionSource.Task;
		}

		public static Task<T> FromResult<T>(this Task<T> task, T result)
		{
			var taskCompletionSource = new TaskCompletionSource<T>();
			taskCompletionSource.SetResult(result);
			return taskCompletionSource.Task;
		}
	}
}
