using System.Linq;
using NHibernate.Criterion;
using NHibernate.Impl;
using NUnit.Framework;

namespace NHibernate.Test.NHSpecificTest.Futures
{
	[TestFixture]
	public class FutureQueryOverFixtureAsync : FutureFixture
	{

		protected override void OnSetUp()
		{
			base.OnSetUp();
			using (ISession s = OpenSession())
			using (ITransaction tx = s.BeginTransaction())
			{
				s.Save(new Person());
				tx.Commit();
			}
		}

		protected override void OnTearDown()
		{
			base.OnTearDown();
			using (ISession s = OpenSession())
			using (ITransaction tx = s.BeginTransaction())
			{
				s.Delete("from Person");
				tx.Commit();
			}
		}

		[Test]
		public void CanUseFutureCriteriaAsync()
		{
			// Arrange
			using (var s = sessions.OpenSession())
			{
				IgnoreThisTestIfMultipleQueriesArentSupportedByDriver();

				var persons10Awaitable = s.QueryOver<Person>()
					.Take(10)
					.Future();
				var persons5Awaitable = s.QueryOver<Person>()
					.Select(p => p.Id)
					.Take(5)
					.Future<int>();

				// Act
				persons5Awaitable.AsTask().ContinueWith(persons5 =>
					persons10Awaitable.AsTask().ContinueWith(persons10 =>
					{
						int actualPersons5Count = persons5.Result.Count();
						int actualPersons10Count = persons10.Result.Count();

						Assert.That(actualPersons5Count, Is.EqualTo(1));
						Assert.That(actualPersons10Count, Is.EqualTo(1));
					}).Wait()).Wait();
			}
		}

		[Test]
		public void TwoFuturesRunInTwoRoundTripsAsync()
		{
			// Arrange
			int actualPersons5Count = 0;
			int actualPersons10Count = 0;
			using (var s = sessions.OpenSession())
			{
				IgnoreThisTestIfMultipleQueriesArentSupportedByDriver();

				var persons10Awaitable = s.QueryOver<Person>()
					.Take(10)
					.Future();
				var persons5Awaitable = s.QueryOver<Person>()
					.Select(p => p.Id)
					.Take(5)
					.Future<int>();

				// Act
				persons10Awaitable.AsTask().ContinueWith(persons10 => 
					persons5Awaitable.AsTask().ContinueWith(persons5 =>
					{
						actualPersons10Count += persons10.Result.Count();
						actualPersons5Count += persons5.Result.Count();

						// Assert
						Assert.That(actualPersons5Count, Is.EqualTo(1));
						Assert.That(actualPersons10Count, Is.EqualTo(1));
					}).Wait()).Wait();
			}
		}

		[Test]
		public void CanCombineSingleFutureValueWithEnumerableFuturesAsync()
		{
			// Arrange
			using (var s = sessions.OpenSession())
			{
				IgnoreThisTestIfMultipleQueriesArentSupportedByDriver();

				var personsAwaitable = s.QueryOver<Person>()
					.Take(10)
					.Future();

				var personIds = s.QueryOver<Person>()
					.Select(p => p.Id)
					.FutureValue<int>();

				var singlePerson = s.QueryOver<Person>()
					.FutureValue();

				// Act
				singlePerson.ValueAsync().ContinueWith(singlePersonValue =>
					personIds.ValueAsync().ContinueWith(personId =>
						personsAwaitable.AsTask().ContinueWith(persons =>
						{
							foreach (var person in persons.Result) { }

							// Assert
							Assert.That(singlePersonValue, Is.Not.Null);
							Assert.That(personId, Is.Not.EqualTo(0));
						}).Wait()).Wait()).Wait();
			}
		}
	}
}
