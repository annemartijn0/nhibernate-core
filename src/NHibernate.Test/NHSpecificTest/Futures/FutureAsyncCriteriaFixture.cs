using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NHibernate.Criterion;
using NUnit.Framework;

namespace NHibernate.Test.NHSpecificTest.Futures
{
	[TestFixture]
	public class FutureAsyncCriteriaFixture : FutureFixture
	{
		protected override void OnSetUp()
		{
			base.OnSetUp();

			using (ISession s = sessions.OpenSession())
			using (ITransaction tx = s.BeginTransaction())
			{
				s.Save(new Person
				{
					Name = "person1"
				});
				s.Save(new Person
				{
					Name = "person2"
				});
				s.Save(new Person
				{
					Name = "person3"
				});

				tx.Commit();
			}
		}

		protected override void OnTearDown()
		{
			using (ISession s = sessions.OpenSession())
			using (ITransaction tx = s.BeginTransaction())
			{
				s.Delete("FROM Person");
				tx.Commit();
			}

			base.OnTearDown();
		}

		[Test]
		public void FutureAsyncReturnsPeople()
		{
			// Arrange
			using (var s = sessions.OpenSession())
			{
				IgnoreThisTestIfMultipleQueriesArentSupportedByDriver();

				// Act
				s.CreateCriteria(typeof(Person))
						.Future<Person>()
						.AsTask()
						.ContinueWith(persons5 =>
							// Assert
							Assert.That(3, Is.EqualTo(persons5.Result.Count()))).Wait();
			}
		}

		[Test]
		public void FutureAsyncReturnsPeopleLimitOneAndTwo()
		{
			// Arrange
			using (var s = sessions.OpenSession())
			{
				IgnoreThisTestIfMultipleQueriesArentSupportedByDriver();

				// Act
				var persons1 = s.CreateCriteria(typeof(Person))
					.SetMaxResults(1)
					.Future<Person>();

				var persons2 = s.CreateCriteria(typeof(Person))
					.SetMaxResults(2)
					.Future<Person>();

				var persons3 = s.CreateCriteria(typeof(Person))
					.SetMaxResults(2)
					.Future<Person>();

				// Assert
				persons1.AsTask().ContinueWith(task =>
					Assert.That(1, Is.EqualTo(task.Result.Count()))).Wait();

				persons2.AsTask().ContinueWith(task =>
					Assert.That(2, Is.EqualTo(task.Result.Count()))).Wait();

				persons3.AsTask().ContinueWith(task =>
					Assert.That(2, Is.EqualTo(task.Result.Count()))).Wait();
			}
		}

		[Test]
		public void FutureAsyncReturnsPeopleLimitOneAndTwoParallel()
		{
			// Arrange
			using (var s = sessions.OpenSession())
			{
				IgnoreThisTestIfMultipleQueriesArentSupportedByDriver();

				// Act
				var persons1Task = s.CreateCriteria(typeof(Person))
					.SetMaxResults(1)
					.Future<Person>()
					.AsTask();

				var persons2Task = s.CreateCriteria(typeof(Person))
					.SetMaxResults(2)
					.Future<Person>()
					.AsTask();

				Task.Factory.ContinueWhenAll(new Task[] { persons1Task, persons2Task }, _ =>
				{
					// Assert
					Assert.That(1, Is.EqualTo(persons1Task.Result.Count()));
					Assert.That(2, Is.EqualTo(persons2Task.Result.Count()));
				}).Wait();
			}
		}

		[Test]
		public void CanCombineSingleFutureValueWithEnumerableFutures()
		{
			// Assign
			using (var s = sessions.OpenSession())
			{
				IgnoreThisTestIfMultipleQueriesArentSupportedByDriver();

				// Act
				var persons1 = s.CreateCriteria(typeof(Person))
					.SetMaxResults(1)
					.Future<Person>();

				var persons2 = s.CreateCriteria(typeof(Person))
					.SetMaxResults(2)
					.Future<Person>();

				var personCount = s.CreateCriteria(typeof(Person))
					.SetProjection(Projections.RowCount())
					.FutureValue<int>();

				// Assert
				persons1.AsTask().ContinueWith(task =>
					Assert.That(1, Is.EqualTo(1))).Wait();

				Assert.That(personCount.Value, Is.EqualTo(3));

				persons2.AsTask().ContinueWith(task =>
					Assert.That(2, Is.EqualTo(2))).Wait();
			}
		}

		[Test]
		public void CombineFutureAsyncAndNormalFuture()
		{
			// Arrange
			using (var s = sessions.OpenSession())
			{
				IgnoreThisTestIfMultipleQueriesArentSupportedByDriver();

				// Act
				var persons2 = s.CreateCriteria(typeof(Person))
					.SetMaxResults(2)
					.Future<Person>();
				var persons3 = s.CreateCriteria(typeof(Person))
					.SetMaxResults(3)
					.Future<Person>();

				// Assert
				Assert.That(2, Is.EqualTo(persons2.AsTask().Result.Count())); // fire first future round-trip
				Assert.That(3, Is.EqualTo(persons3.Count())); // fire second future round-trip
			}
		}

		[Test]
		public void CombinedFutureAsyncAndNormalFutureDoOneRoundTrip()
		{
			// Arrange
			using (var s = sessions.OpenSession())
			using (var logSpy = new SqlLogSpy())
			{
				IgnoreThisTestIfMultipleQueriesArentSupportedByDriver();

				// Act
				var persons2 = s.CreateCriteria(typeof(Person))
					.SetMaxResults(2)
					.Future<Person>();
				var persons3 = s.CreateCriteria(typeof(Person))
					.SetMaxResults(3)
					.Future<Person>();

				foreach (var p in persons3) { }
				persons2.AsTask().ContinueWith(task =>
				{
					foreach (var p in task.Result) { }

					// Assert
					var events = logSpy.Appender.GetEvents();
					Assert.That(events.Length, Is.EqualTo(1));
				}).Wait();
			}
		}

		[Test]
		public void TwoFutureAsyncssRunInTwoRoundTrips()
		{
			// Arrange
			using (var s = sessions.OpenSession())
			{
				IgnoreThisTestIfMultipleQueriesArentSupportedByDriver();

				using (var logSpy = new SqlLogSpy())
				{
					// Act
					var persons2 = s.CreateCriteria(typeof(Person))
						.SetMaxResults(2)
						.Future<Person>();

					// Assert
					Assert.That(2, Is.EqualTo(persons2.AsTask().Result.Count())); // fire first future round-trip

					// Act
					var persons3 = s.CreateCriteria(typeof(Person))
						.SetMaxResults(3)
						.Future<Person>();

					// Assert
					Assert.That(3, Is.EqualTo(persons3.AsTask().Result.Count())); // fire second future round-trip
				}
			}
		}

		[Test]
		[ExpectedException(typeof(AggregateException))]
		public void IAwaitableEnumerable_AsTask_ShouldReturnCanceledTaskWhenPassedCanceledToken()
		{
			// Arrange
			var cancellationTokenSource = new CancellationTokenSource();
			cancellationTokenSource.Cancel();
			Task result;

			using (ISession session = OpenSession())
			{
				// Act
				result = session.CreateCriteria<Person>()
					.Future<Person>()
					.AsTask(cancellationTokenSource.Token);
			}

			// Assert
			Assert.That(result.IsCanceled);
			result.Wait();
		}

		[Test]
		public void IAwaitableEnumerable_AsTask_ShouldThrowExceptionWhenCancellationTokenIsCanceled()
		{
			// Arrange
			var cancellationTokenSource = new CancellationTokenSource();
			Task task;
			using (ISession session = OpenSession())
			{
				// Act
				task = session.CreateCriteria<Person>()
					.Future<Person>().AsTask(cancellationTokenSource.Token);

				cancellationTokenSource.Cancel();
			}

			try
			{
				task.Wait();
			}
			catch (AggregateException aggregateException)
			{
				// Assert
				Assert.That(aggregateException.InnerExceptions.Count, Is.EqualTo(1));
				Assert.That(aggregateException.InnerExceptions[0], Is.TypeOf(typeof(TaskCanceledException)));
			}
		}
	}
}
