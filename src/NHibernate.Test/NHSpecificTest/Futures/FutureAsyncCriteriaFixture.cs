using System.Linq;
using System.Threading.Tasks;
using NHibernate.Criterion;
using NHibernate.Impl;
using NHibernate.Test.NHSpecificTest.NH2189;
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
			using (var s = sessions.OpenSession())
			{
				IgnoreThisTestIfMultipleQueriesArentSupportedByDriver();

				s.CreateCriteria(typeof(Person))
						.SetMaxResults(5)
						.FutureAsync<Person>()
						.ContinueWith(persons5 =>
							Assert.That(3, Is.EqualTo(persons5.Result.Count()))).Wait();
			}
		}

		[Test]
		public void FutureAsyncReturnsPeopleLimitOneAndTwo()
		{
			using (var s = sessions.OpenSession())
			{
				IgnoreThisTestIfMultipleQueriesArentSupportedByDriver();

				s.CreateCriteria(typeof(Person))
					.SetMaxResults(1)
					.FutureAsync<Person>()
					.ContinueWith(persons1 =>
					{
						Assert.That(1, Is.EqualTo(persons1.Result.Count()));

						s.CreateCriteria(typeof(Person))
							.SetMaxResults(2)
							.FutureAsync<Person>()
							.ContinueWith(persons2 =>
								Assert.That(2, Is.EqualTo(persons2.Result.Count()))).Wait();
					}).Wait();
			}
		}

		[Test]
		public void FutureAsyncReturnsPeopleLimitOneAndTwoParallel()
		{
			using (var s = sessions.OpenSession())
			{
				IgnoreThisTestIfMultipleQueriesArentSupportedByDriver();

				var persons1 = s.CreateCriteria(typeof(Person))
					.SetMaxResults(1)
					.FutureAsync<Person>();

				var persons2 = s.CreateCriteria(typeof(Person))
					.SetMaxResults(2)
					.FutureAsync<Person>();

				Task.Factory.ContinueWhenAll(new Task[] { persons1, persons2 }, _ =>
				{
					Assert.That(1, Is.EqualTo(persons1.Result.Count()));
					Assert.That(2, Is.EqualTo(persons2.Result.Count()));
				}).Wait();
			}
		}

		[Test]
		public void CanUseFutureCriteriaAsync()
		{
			using (var s = sessions.OpenSession())
			{
				IgnoreThisTestIfMultipleQueriesArentSupportedByDriver();

				s.CreateCriteria(typeof(Person))
					.SetMaxResults(10)
					.FutureAsync<Person>()
					.ContinueWith(persons10 =>
				{
					s.CreateCriteria(typeof(Person))
					.SetMaxResults(5)
					.FutureAsync<Person>()
					.ContinueWith(persons5 =>
					{
						using (var logSpy = new SqlLogSpy())
						{
							foreach (var person in persons5.Result)
							{

							}

							foreach (var person in persons10.Result)
							{

							}

							var events = logSpy.Appender.GetEvents();
							Assert.AreEqual(1, events.Length);
						}
					}).Wait();
				}).Wait();
			}
		}

		[Test]
		public void TwoFutureAsyncssRunInTwoRoundTrips()
		{
			using (var s = sessions.OpenSession())
			{
				IgnoreThisTestIfMultipleQueriesArentSupportedByDriver();

				using (var logSpy = new SqlLogSpy())
				{
					s.CreateCriteria(typeof(Person))
						.SetMaxResults(10)
						.FutureAsync<Person>()
						.ContinueWith(persons10 =>
						{
							foreach (var person in persons10.Result) { } // fire first future round-trip
						}).Wait();

					s.CreateCriteria(typeof(Person))
						.SetMaxResults(5)
						.FutureAsync<Person>()
						.ContinueWith(persons5 =>
						{
							foreach (var person in persons5.Result) { } // fire second future round-trip
						}).Wait();

					var events = logSpy.Appender.GetEvents();
					Assert.AreEqual(2, events.Length);
				}
			}
		}

		[Test]
		public void CanCombineSingleFutureValueWithEnumerableFutures()
		{
			using (var s = sessions.OpenSession())
			{
				IgnoreThisTestIfMultipleQueriesArentSupportedByDriver();

				var persons = s.CreateCriteria(typeof(Person))
					.SetMaxResults(10)
					.FutureAsync<Person>();

				var personCount = s.CreateCriteria(typeof(Person))
					.SetProjection(Projections.RowCount())
					.FutureValue<int>();

				using (var logSpy = new SqlLogSpy())
				{
					int count = personCount.Value;

					persons.ContinueWith(_ =>
					{
						foreach (var person in persons.Result)
						{

						}
					}).Wait();
					
					var events = logSpy.Appender.GetEvents();
					Assert.AreEqual(1, events.Length);
				}
			}
		}
	}
}
