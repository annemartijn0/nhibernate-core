using System.Collections;
using System.Linq;
using NHibernate.Criterion;
using NHibernate.Transform;
using NUnit.Framework;

namespace NHibernate.Test.Criteria.Lambda
{
	[TestFixture]
	public class ProjectIntegrationFixtureAsync : TestCase
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
				s.Save(new Person { Name = "test person 1", Age = 20 });
				s.Save(new Person { Name = "test person 1", Age = 30 });
				s.Save(new Person { Name = "test person 2", Age = 40 });
				t.Commit();
			}
		}

		protected override void OnTearDown()
		{
			using (var s = OpenSession())
			using (var t = s.BeginTransaction())
			{
				s.CreateQuery("delete from Person").ExecuteUpdate();
				t.Commit();
			}
		}

		[Test]
		public void SinglePropertyAsync()
		{
			// Arrange
			using (var s = OpenSession())
			using (s.BeginTransaction())
			{
				// Act
				s.QueryOver<Person>()
					.Select(p => p.Age)
					.OrderBy(p => p.Age).Asc
					.ListAsync<int>()
					.ContinueWith(task =>
					{
						var actual = task.Result;

						// Assert
						Assert.That(actual[0], Is.EqualTo(20));
					}).Wait();
			}
		}

		[Test]
		public void ProjectTransformToDtoAsync()
		{
			// Arrange
			using (var s = OpenSession())
			using (s.BeginTransaction())
			{
				PersonSummary summary = null;

				// Act
				s.QueryOver<Person>()
					.SelectList(list => list
						.SelectGroup(p => p.Name).WithAlias(() => summary.Name)
						.Select(Projections.RowCount()).WithAlias(() => summary.Count))
					.OrderByAlias(() => summary.Name).Asc
					.TransformUsing(Transformers.AliasToBean<PersonSummary>())
					.ListAsync<PersonSummary>()
					.ContinueWith(task =>
					{
						var actual = task.Result;

						// Assert
						Assert.That(actual.Count, Is.EqualTo(2));
						Assert.That(actual[0].Name, Is.EqualTo("test person 1"));
						Assert.That(actual[0].Count, Is.EqualTo(2));
						Assert.That(actual[1].Name, Is.EqualTo("test person 2"));
						Assert.That(actual[1].Count, Is.EqualTo(1));
					});
			}
		}
	}
}
