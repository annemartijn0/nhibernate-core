using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

using NUnit.Framework;
using SharpTestsEx;

using NHibernate.Criterion;

namespace NHibernate.Test.Criteria.Lambda
{
	[TestFixture]
	public class IntegrationFixtureAsync : TestCase
	{
		protected override string MappingsAssembly { get { return "NHibernate.Test"; } }

		protected override IList Mappings
		{
			get { return new[] { "Criteria.Lambda.Mappings.hbm.xml" }; }
		}

		protected override void OnTearDown()
		{
			using (var s = OpenSession())
			using (var t = s.BeginTransaction())
			{
				s.CreateQuery("delete from Child").ExecuteUpdate();
				s.CreateQuery("update Person p set p.Father = null").ExecuteUpdate();
				s.CreateQuery("delete from Person").ExecuteUpdate();
				s.CreateQuery("delete from JoinedChild").ExecuteUpdate();
				s.CreateQuery("delete from Parent").ExecuteUpdate();
				t.Commit();
			}
		}

		[Test]
		public void DetachedQuery_SimpleCriterionAsync()
		{
			// Arrange
			using (ISession s = OpenSession())
			using (ITransaction t = s.BeginTransaction())
			{
				s.Save(new Person() { Name = "test person 1", Age = 20 });
				t.Commit();
			}

			using (ISession s = OpenSession())
			{
				QueryOver<Person> personQuery =
					QueryOver.Of<Person>()
						.Where(p => p.Name == "test person 1");

				// Act
				personQuery.GetExecutableQueryOver(s)
					.ListAsync()
					.ContinueWith(task =>
					{
						IList<Person> actual = task.Result;

						// Assert
						Assert.That(actual[0].Age, Is.EqualTo(20));
					}).Wait();
			}
		}

		[Test]
		public void FilterNullComponentAsync()
		{
			// Arrange
			using (ISession s = OpenSession())
			using (ITransaction t = s.BeginTransaction())
			{
				var p1 = new Person() { Detail = new PersonDetail() { MaidenName = "test", Anniversary = new DateTime(2007, 06, 05) } };
				var p2 = new Person() { Detail = null };

				s.Save(p1);
				s.Save(p2);

				// Act
				s.QueryOver<Person>()
					.Where(p => p.Detail == null)
					.ListAsync()
					.ContinueWith(task =>
					{
						var nullDetails = task.Result;

						// Assert
						Assert.That(nullDetails.Count, Is.EqualTo(1));
						Assert.That(nullDetails[0].Id, Is.EqualTo(p2.Id));
					}).Wait();
			}
		}

		[Test]
		public void OnClauseAsync()
		{
			// Arrange
			using (ISession s = OpenSession())
			using (ITransaction t = s.BeginTransaction())
			{
				s.Save(new Person() { Name = "John" }
						.AddChild(new Child() { Nickname = "John" })
						.AddChild(new Child() { Nickname = "Judy" }));

				s.Save(new Person() { Name = "Jean" });
				s.Save(new Child() { Nickname = "James" });

				t.Commit();
			}

			using (ISession s = OpenSession())
			{
				Child childAlias = null;
				Person parentAlias = null;

				// Act
				s.QueryOver(() => childAlias)
					.Left.JoinQueryOver(c => c.Parent, () => parentAlias, p => p.Name == childAlias.Nickname)
						.WhereRestrictionOn(p => p.Name).IsNotNull
						.ListAsync()
						.ContinueWith(task =>
						{
							// Assert
							task.Result.Should().Have.Count.EqualTo(1);
						}).Wait();
			}

			using (ISession s = OpenSession())
			{
				Child childAlias = null;
				Person parentAlias = null;

				// Act
				s.QueryOver(() => childAlias)
					.Left.JoinAlias(c => c.Parent, () => parentAlias, p => p.Name == childAlias.Nickname)
					.Select(c => parentAlias.Name)
					.ListAsync<string>()
					.ContinueWith(task =>
					{
						// Assert
						task.Result
							.Where(n => !string.IsNullOrEmpty(n))
							.Should().Have.Count.EqualTo(1);
					}).Wait();
			}

			using (ISession s = OpenSession())
			{
				Person personAlias = null;
				Child childAlias = null;

				// Act
				s.QueryOver<Person>(() => personAlias)
					.Left.JoinQueryOver(p => p.Children, () => childAlias, c => c.Nickname == personAlias.Name)
					.WhereRestrictionOn(c => c.Nickname).IsNotNull
					.ListAsync()
					.ContinueWith(task =>
					{
						// Assert
						task.Result.Should().Have.Count.EqualTo(1);
					}).Wait();
			}

			using (ISession s = OpenSession())
			{
				Person personAlias = null;
				Child childAlias = null;

				// Act
				s.QueryOver<Person>(() => personAlias)
					.Left.JoinAlias(p => p.Children, () => childAlias, c => c.Nickname == personAlias.Name)
					.Select(p => childAlias.Nickname)
					.ListAsync<string>()
					.ContinueWith(task =>
					{
						// Assert
						task.Result
							.Where(n => !string.IsNullOrEmpty(n))
							.Should().Have.Count.EqualTo(1);
					}).Wait();
			}
		}

		[Test]
		public void IsTypeAsync()
		{
			// Arrange
			using (ISession s = OpenSession())
			using (ITransaction t = s.BeginTransaction())
			{
				var father1 = new Person() { Name = "Father 1" };
				var father2 = new CustomPerson() { Name = "Father 2" };

				var person1 = new Person() { Name = "Person 1", Father = father2 };
				var person2 = new CustomPerson() { Name = "Person 2", Father = father1 };

				s.Save(father1);
				s.Save(father2);

				s.Save(person1);
				s.Save(person2);

				t.Commit();
			}

			using (ISession s = OpenSession())
			{
				// Act
				s.QueryOver<Person>()
					.Where(p => p is CustomPerson)
					.And(p => p.Father != null)
					.ListAsync()
					.ContinueWith(task =>
					{
						var actual = task.Result;

						// Assert
						Assert.That(actual.Count, Is.EqualTo(1));
						Assert.That(actual[0].Name, Is.EqualTo("Person 2"));
					}).Wait();
			}

			using (ISession s = OpenSession())
			{
				// Act
				s.QueryOver<Person>()
					.Where(p => p.GetType() == typeof(CustomPerson))
					.And(p => p.Father != null)
					.ListAsync()
					.ContinueWith(task =>
					{
						var actual = task.Result;

						// Assert
						Assert.That(actual.Count, Is.EqualTo(1));
						Assert.That(actual[0].Name, Is.EqualTo("Person 2"));
					}).Wait();
			}

			using (ISession s = OpenSession())
			{
				Person f = null;

				// Act
				s.QueryOver<Person>()
					.JoinAlias(p => p.Father, () => f)
					.Where(() => f is CustomPerson)
					.ListAsync()
					.ContinueWith(task =>
					{
						var actual = task.Result;

						// Assert
						Assert.That(actual.Count, Is.EqualTo(1));
						Assert.That(actual[0].Name, Is.EqualTo("Person 1"));
					}).Wait();
			}

			using (ISession s = OpenSession())
			{
				Person f = null;

				// Act
				s.QueryOver<Person>()
					.JoinAlias(p => p.Father, () => f)
					.Where(() => f.GetType() == typeof(CustomPerson))
					.ListAsync()
					.ContinueWith(task =>
					{
						var actual = task.Result;

						// Assert
						Assert.That(actual.Count, Is.EqualTo(1));
						Assert.That(actual[0].Name, Is.EqualTo("Person 1"));
					}).Wait();
			}
		}

		[Test]
		public void OverrideEagerJoinAsync()
		{
			// Arrange
			using (ISession s = OpenSession())
			using (ITransaction t = s.BeginTransaction())
			{
				s.Save(new Parent()
						.AddChild(new JoinedChild())
						.AddChild(new JoinedChild()));

				t.Commit();
			}

			using (ISession s = OpenSession())
			{
				// Act
				s.QueryOver<Parent>()
					.ListAsync()
					.ContinueWith(task =>
					{
						var persons = task.Result;

						// Assert
						Assert.That(NHibernateUtil.IsInitialized(persons[0].Children), "Default query did not eagerly load children");
					}).Wait();
			}

			using (ISession s = OpenSession())
			{
				// Act
				s.QueryOver<Parent>()
					.Fetch(p => p.Children).Lazy
					.ListAsync()
					.ContinueWith(task =>
					{
						var persons = task.Result;

						// Assert
						Assert.That(persons.Count, Is.EqualTo(1));
						Assert.That(!NHibernateUtil.IsInitialized(persons[0].Children), "Children not lazy loaded");
					}).Wait();
			}
		}

		[Test]
		public void RowCountAsync()
		{
			// Arrange
			SetupPagingData();

			using (ISession s = OpenSession())
			{
				IQueryOver<Person> query =
					s.QueryOver<Person>()
						.JoinQueryOver(p => p.Children)
						.OrderBy(c => c.Age).Desc
						.Skip(2)
						.Take(1);

				// Act
				query.ListAsync()
					.ContinueWith(task =>
					{
						IList<Person> results = task.Result;
						int rowCount = query.RowCount();
						object bigRowCount = query.RowCountInt64();

						// Assert
						Assert.That(results.Count, Is.EqualTo(1));
						Assert.That(results[0].Name, Is.EqualTo("Name 3"));
						Assert.That(rowCount, Is.EqualTo(4));
						Assert.That(bigRowCount, Is.TypeOf<long>());
						Assert.That(bigRowCount, Is.EqualTo(4));
					}).Wait();
			}
		}

		[Test]
		public void FunctionsOrderAsync()
		{
			// Arrange
			using (ISession s = OpenSession())
			using (ITransaction t = s.BeginTransaction())
			{
				s.Save(new Person() { Name = "p2", BirthDate = new DateTime(2008, 07, 06) });
				s.Save(new Person() { Name = "p1", BirthDate = new DateTime(2009, 08, 07) });
				s.Save(new Person() { Name = "p3", BirthDate = new DateTime(2007, 06, 05) });

				t.Commit();
			}

			using (ISession s = OpenSession())
			using (s.BeginTransaction())
			{
				// Act
				s.QueryOver<Person>()
					.OrderBy(p => p.BirthDate.Year).Desc
					.ListAsync()
					.ContinueWith(task =>
					{
						var persons = task.Result;

						// Assert
						persons.Count.Should().Be(3);
						persons[0].Name.Should().Be("p1");
						persons[1].Name.Should().Be("p2");
						persons[2].Name.Should().Be("p3");
					}).Wait();
			}
		}

		private void SetupPagingData()
		{
			using (ISession s = OpenSession())
			using (ITransaction t = s.BeginTransaction())
			{
				s.Save(new Person() { Name = "Name 1", Age = 1 }
						.AddChild(new Child() { Nickname = "Name 1.1", Age = 1 }));

				s.Save(new Person() { Name = "Name 2", Age = 2 }
						.AddChild(new Child() { Nickname = "Name 2.1", Age = 3 }));

				s.Save(new Person() { Name = "Name 3", Age = 3 }
						.AddChild(new Child() { Nickname = "Name 3.1", Age = 2 }));

				s.Save(new Person() { Name = "Name 4", Age = 4 }
						.AddChild(new Child() { Nickname = "Name 4.1", Age = 4 }));

				t.Commit();
			}
		}

		[Test]
		public void StatelessSessionAsync()
		{
			// Arrange
			int personId;
			using (var ss = sessions.OpenStatelessSession())
			using (var t = ss.BeginTransaction())
			{
				var person = new Person { Name = "test1" };
				ss.Insert(person);
				personId = person.Id;
				t.Commit();
			}

			using (var ss = sessions.OpenStatelessSession())
			using (ss.BeginTransaction())
			{
				// Act
				ss.QueryOver<Person>()
					.ListAsync()
					.ContinueWith(task =>
					{
						var statelessPerson1 = task.Result[0];

						// Assert
						Assert.That(statelessPerson1.Id, Is.EqualTo(personId));
					}).Wait();

				// Act
				QueryOver.Of<Person>()
					.GetExecutableQueryOver(ss)
					.ListAsync()
					.ContinueWith(task =>
					{
						var statelessPerson2 = task.Result[0];

						// Assert
						Assert.That(statelessPerson2.Id, Is.EqualTo(personId));
					}).Wait();
			}
		}
	}
}