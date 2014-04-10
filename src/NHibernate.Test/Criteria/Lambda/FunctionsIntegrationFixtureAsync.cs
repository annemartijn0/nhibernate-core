using System;
using System.Collections;
using NHibernate.Criterion;
using NUnit.Framework;
using SharpTestsEx;

namespace NHibernate.Test.Criteria.Lambda
{
	[TestFixture]
	public class FunctionsIntegrationFixtureAsync : TestCase
	{
		protected override string MappingsAssembly
		{
			get { return "NHibernate.Test"; }
		}

		protected override IList Mappings
		{
			get { return new[] { "Criteria.Lambda.Mappings.hbm.xml" }; }
		}

		protected override void OnTearDown()
		{
			using (var s = OpenSession())
			using (var t = s.BeginTransaction())
			{
				s.Delete("from Person");
				t.Commit();
			}
		}

		protected override void OnSetUp()
		{
			using (var s = OpenSession())
			using (var t = s.BeginTransaction())
			{
				s.Save(new Person { Name = "p2", BirthDate = new DateTime(2008, 07, 07) });
				s.Save(new Person { Name = "p1", BirthDate = new DateTime(2009, 08, 07), Age = 90 });
				s.Save(new Person { Name = "pP3", BirthDate = new DateTime(2007, 06, 05) });

				t.Commit();
			}
		}

		[Test]
		public void YearPartEqualAsync()
		{
			// Arrange
			using (var s = OpenSession())
			using (s.BeginTransaction())
			{
				// Act
				s.QueryOver<Person>()
					.Where(p => p.BirthDate.YearPart() == 2008)
					.ListAsync()
					.ContinueWith(task =>
					{
						var persons = task.Result;

						// Assert
						persons.Count.Should().Be(1);
						persons[0].Name.Should().Be("p2");
					}).Wait();
			}
		}

		[Test]
		public void YearPartIsInAsync()
		{
			// Arrange
			using (var s = OpenSession())
			using (s.BeginTransaction())
			{
				// Act
				s.QueryOver<Person>()
					.Where(p => p.BirthDate.YearPart().IsIn(new[] { 2008, 2009 }))
					.OrderBy(p => p.Name).Asc
					.ListAsync()
					.ContinueWith(task =>
					{
						var persons = task.Result;

						// Assert
						persons.Count.Should().Be(2);
						persons[0].Name.Should().Be("p1");
						persons[1].Name.Should().Be("p2");
					}).Wait();
			}
		}

		[Test]
		public void MonthPartEqualsDayPartAsync()
		{
			// Arrange
			using (var s = OpenSession())
			using (s.BeginTransaction())
			{
				// Act
				s.QueryOver<Person>()
					.Where(p => p.BirthDate.MonthPart() == p.BirthDate.DayPart())
					.ListAsync()
					.ContinueWith(task =>
					{
						var persons = task.Result;

						// Assert
						persons.Count.Should().Be(1);
						persons[0].Name.Should().Be("p2");
					}).Wait();
			}
		}

		[Test]
		public void OrderByYearPartAsync()
		{
			// Arrange
			using (var s = OpenSession())
			using (s.BeginTransaction())
			{
				// Act
				s.QueryOver<Person>()
					.OrderBy(p => p.BirthDate.YearPart()).Desc
					.ListAsync()
					.ContinueWith(task =>
					{
						var persons = task.Result;

						// Assert
						persons.Count.Should().Be(3);
						persons[0].Name.Should().Be("p1");
						persons[1].Name.Should().Be("p2");
						persons[2].Name.Should().Be("pP3");
					}).Wait();
			}
		}

		[Test]
		public void YearEqualAsync()
		{
			// Arrange
			using (var s = OpenSession())
			using (s.BeginTransaction())
			{
				// Act
				s.QueryOver<Person>()
					.Where(p => p.BirthDate.Year == 2008)
					.ListAsync()
					.ContinueWith(task =>
					{
						var persons = task.Result;

						// Assert
						persons.Count.Should().Be(1);
						persons[0].Name.Should().Be("p2");
					}).Wait();
			}
		}

		[Test]
		public void YearIsInAsync()
		{
			// Arrange
			using (var s = OpenSession())
			using (s.BeginTransaction())
			{
				// Act
				s.QueryOver<Person>()
					.Where(p => p.BirthDate.Year.IsIn(new[] { 2008, 2009 }))
					.OrderBy(p => p.Name).Asc
					.ListAsync()
					.ContinueWith(task =>
					{
						var persons = task.Result;

						// Assert
						persons.Count.Should().Be(2);
						persons[0].Name.Should().Be("p1");
						persons[1].Name.Should().Be("p2");
					}).Wait();
			}
		}

		[Test]
		public void OrderByYearAsync()
		{
			// Arrange
			using (var s = OpenSession())
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
						persons[2].Name.Should().Be("pP3");
					}).Wait();
			}
		}

		[Test]
		public void MonthEqualsDayAsync()
		{
			// Arrange
			using (var s = OpenSession())
			using (s.BeginTransaction())
			{
				// Act
				s.QueryOver<Person>()
					.Where(p => p.BirthDate.Month == p.BirthDate.Day)
					.ListAsync()
					.ContinueWith(task =>
					{
						var persons = task.Result;

						// Assert
						persons.Count.Should().Be(1);
						persons[0].Name.Should().Be("p2");
					}).Wait();
			}
		}
	}
}
