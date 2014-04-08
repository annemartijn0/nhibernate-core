using System.Linq;
using NHibernate.Cfg;
using NHibernate.Connection;
using NHibernate.Criterion;
using NHibernate.Dialect;
using NHibernate.Driver;
using NHibernate.Linq;
using NUnit.Framework;

using Environment=NHibernate.Cfg.Environment;

namespace NHibernate.Test.NHSpecificTest.Futures
{
	[TestFixture]
	public class FallbackFixtureAsync : FutureFixture
	{
		protected override bool AppliesTo(Dialect.Dialect dialect)
		{
			var cp = ConnectionProviderFactory.NewConnectionProvider(cfg.Properties);
			return !cp.Driver.SupportsMultipleQueries;
		}

		protected override void Configure(Configuration configuration)
		{
			base.Configure(configuration);
			if (Dialect is MsSql2000Dialect)
			{
				configuration.Properties[Environment.ConnectionDriver] =
					typeof (TestDriverThatDoesntSupportQueryBatching).AssemblyQualifiedName;
			}
		}

		protected override void OnTearDown()
		{
			using (var session = sessions.OpenSession())
			{
				session.Delete("from Person");
				session.Flush();
			}

			base.OnTearDown();
		}

		[Test]
		public void FutureValueOfCriteriaCanGetSingleEntityWhenQueryBatchingIsNotSupported_Async()
		{
			// Arrange
			int personId = CreatePerson();

			using (var session = sessions.OpenSession())
			{
				var futurePerson = session.CreateCriteria<Person>()
					.Add(Restrictions.Eq("Id", personId))
					.FutureValue<Person>();

				// Act
				var result = futurePerson.ValueAsync().Result;

				// Assert
				Assert.That(result, Is.Not.Null);
			}
		}

		[Test]
		public void FutureValueOfCriteriaCanGetScalarValueWhenQueryBatchingIsNotSupported_Async()
		{
			// Arrange
			CreatePerson();

			using (var session = sessions.OpenSession())
			{
				var futureCount = session.CreateCriteria<Person>()
					.SetProjection(Projections.RowCount())
					.FutureValue<int>();

				// Act
				var result = futureCount.ValueAsync().Result;

				// Assert
				Assert.That(result, Is.EqualTo(1));
			}
		}

		[Test]
		public void FutureValueOfQueryCanGetSingleEntityWhenQueryBatchingIsNotSupported_Async()
		{
			// Arrange
			int personId = CreatePerson();

			using (var session = sessions.OpenSession())
			{
				var futurePerson = session.CreateQuery("from Person where Id = :id")
					.SetInt32("id", personId)
					.FutureValue<Person>();

				//Act
				var result = futurePerson.ValueAsync().Result;

				// Assert
				Assert.That(result, Is.Not.Null);
			}
		}

		[Test]
		public void FutureValueOfQueryCanGetScalarValueWhenQueryBatchingIsNotSupported_Async()
		{
			// Arrange
			CreatePerson();

			using (var session = sessions.OpenSession())
			{
				var futureCount = session.CreateQuery("select count(*) from Person")
					.FutureValue<long>();

				// Act
				var result = futureCount.ValueAsync().Result;

				// Assert
				Assert.That(result, Is.EqualTo(1L));
			}
		}

		[Test]
		public void FutureValueOfLinqCanGetSingleEntityWhenQueryBatchingIsNotSupported_Async()
		{
			// Arrange
			var personId = CreatePerson();

			using (var session = sessions.OpenSession())
			{
				var futurePerson = session.Query<Person>()
					.Where(x => x.Id == personId)
					.ToFutureValue();

				// Act 
				var result = futurePerson.ValueAsync().Result;

				// Assert
				Assert.That(result, Is.Not.Null);
			}
		}

		private int CreatePerson()
		{
			using (var session = sessions.OpenSession())
			{
				var person = new Person();
				session.Save(person);
				session.Flush();
				return person.Id;
			}
		}
	}
}