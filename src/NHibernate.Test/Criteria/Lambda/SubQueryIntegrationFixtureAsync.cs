using System.Collections;
using System.Linq;
using NHibernate.Criterion;
using NUnit.Framework;

namespace NHibernate.Test.Criteria.Lambda
{
	[TestFixture]
	public class SubQueryIntegrationFixtureAsync : TestCase
	{
		protected override string MappingsAssembly
		{
			get { return "NHibernate.Test"; }
		}

		protected override IList Mappings
		{
			get { return new[] { "Criteria.Lambda.Mappings.hbm.xml" }; }
		}

		protected override void OnSetUp()
		{
			using (var s = OpenSession())
			using (var t = s.BeginTransaction())
			{
				s.Save(new Person { Name = "Name 1", Age = 1 }
					.AddChild(new Child { Nickname = "Name 1.1", Age = 1 }));

				s.Save(new Person { Name = "Name 2", Age = 2 }
					.AddChild(new Child { Nickname = "Name 2.1", Age = 2 })
					.AddChild(new Child { Nickname = "Name 2.2", Age = 2 }));

				s.Save(new Person { Name = "Name 3", Age = 3 }
					.AddChild(new Child { Nickname = "Name 3.1", Age = 3 }));

				t.Commit();
			}
		}

		protected override void OnTearDown()
		{
			using (var s = OpenSession())
			using (var t = s.BeginTransaction())
			{
				s.Delete("from Child");
				s.Delete("from Person");
				t.Commit();
			}
		}

		[Test]
		public void JoinQueryOverAsync()
		{
			// Arrange
			using (var s = OpenSession())
			using (s.BeginTransaction())
			{
				// Act
				s.QueryOver<Person>()
					.JoinQueryOver(p => p.Children)
					.Where(c => c.Nickname == "Name 2.1")
					.ListAsync()
					.ContinueWith(task =>
					{
						var persons = task.Result;

						// Assert
						Assert.That(persons.Count, Is.EqualTo(1));
						Assert.That(persons[0].Name, Is.EqualTo("Name 2"));
					}).Wait();
			}
		}
	}
}
