using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Moq;
using NHibernate.Util;
using NUnit.Framework;

namespace NHibernate.Test.UtilityTest
{
	[TestFixture]
	class AwaitableEnumerableWrapperTests
	{
		[Test]
		public void AsTaskReturnsOriginalEnumerable()
		{
			// Arrange
			var mock = new Mock<IEnumerable<object>>().Object;
			var target = new AwaitableEnumerableWrapper<object>(mock);

			// Act
			var result = target.AsTask().Result;

			// Assert
			Assert.That(result, Is.SameAs(mock));
		}

		[Test]
		public void AsTaskReturnsCanceledTaskIfTokenIsCanceled()
		{
			// Arrange
			var mock = new Mock<IEnumerable<object>>().Object;
			var target = new AwaitableEnumerableWrapper<object>(mock);
			var cancelationTokenSource = new CancellationTokenSource();
			cancelationTokenSource.Cancel();

			// Act
			try
			{
				var task = target.AsTask(cancelationTokenSource.Token);
				Assert.That(task.IsCanceled);
				task.Wait();
			}
			catch (AggregateException aggregateException)
			{
				Assert.That(aggregateException.InnerException, 
					Is.TypeOf(typeof (TaskCanceledException)));
			}
		}

		[Test]
		public void GetEnumerator_CallsWrappedEnumerable()
		{
			// Arrange
			var mock = new Mock<IEnumerable<object>>();
			var target = new AwaitableEnumerableWrapper<object>(mock.Object);

			// Act
			target.GetEnumerator();

			// Assert
			mock.Verify(x => x.GetEnumerator(), Times.Once);
		}

		[Test]
		public void ExplicitGetEnumerator_CallsWrappedEnumerable()
		{
			// Arrange
			var mock = new Mock<IEnumerable<object>>().As<IEnumerable>();
			var target = new AwaitableEnumerableWrapper<object>((IEnumerable<object>)mock.Object);

			// Act
			((IEnumerable)target).GetEnumerator();

			// Assert
			mock.Verify(x => x.GetEnumerator(), Times.Once);
		}
	}
}
