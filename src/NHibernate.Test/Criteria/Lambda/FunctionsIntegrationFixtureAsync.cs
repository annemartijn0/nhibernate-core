using System;
using System.Collections;
using System.Threading;
using System.Threading.Tasks;
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
		[ExpectedException(typeof(AggregateException))]
		public void QueryOverListAsync_ShouldReturnCanceledTaskWhenPassedCanceledToken()
		{
			// Arrange
			var cancellationTokenSource = new CancellationTokenSource();
			cancellationTokenSource.Cancel();
			Task result;

			using (ISession session = OpenSession())
			{
				// Act
				result = session.QueryOver<Person>()
					.ListAsync(cancellationTokenSource.Token);
			}

			// Assert
			Assert.That(result.IsCanceled);
			result.Wait();
		}

		[Test]
		public void QueryOverListAsync_ShouldThrowExceptionWhenCancellationTokenIsCanceled()
		{
			// Arrange
			var cancellationTokenSource = new CancellationTokenSource();
			Task task;
			using (ISession session = OpenSession())
			{
				// Act
				task = session.QueryOver<Person>()
					.ListAsync(cancellationTokenSource.Token);

				cancellationTokenSource.Cancel();
			}

			try
			{
				task.Wait();
				Assert.Fail("Should have thrown exception");
			}
			catch (AggregateException aggregateException)
			{
				// Assert
				Assert.That(aggregateException.InnerExceptions.Count, Is.EqualTo(1));
				Assert.That(aggregateException.InnerExceptions[0], Is.TypeOf(typeof(TaskCanceledException)));
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
		[ExpectedException(typeof(AggregateException))]
		public void SingleOrDefaultAsync_ShouldReturnCanceledTaskWhenPassedCanceledToken()
		{
			// Arrange
			var cancellationTokenSource = new CancellationTokenSource();
			cancellationTokenSource.Cancel();
			Task result;

			using (ISession session = OpenSession())
			{
				// Act
				result = session.QueryOver<Person>()
					.SingleOrDefaultAsync<object>(cancellationTokenSource.Token);
			}

			// Assert
			Assert.That(result.IsCanceled);
			result.Wait();
		}

		[Test]
		public void SingleOrDefaultAsync_ShouldThrowExceptionWhenCancellationTokenIsCanceled()
		{
			// Arrange
			var cancellationTokenSource = new CancellationTokenSource();
			Task task;
			using (ISession session = OpenSession())
			{
				// Act
				task = session.QueryOver<Person>()
					.SingleOrDefaultAsync<object>(cancellationTokenSource.Token);

				cancellationTokenSource.Cancel();
			}

			try
			{
				task.Wait();
				Assert.Fail("Should have thrown exception");
			}
			catch (AggregateException aggregateException)
			{
				// Assert
				Assert.That(aggregateException.Flatten().InnerExceptions.Count, Is.EqualTo(1));
				Assert.That(aggregateException.Flatten().InnerExceptions[0], Is.TypeOf(typeof(TaskCanceledException)));
			}
		}

		[Test]
		public void YearPartSingleOrDefaultAsync()
		{
			// Arrange
			using (var s = OpenSession())
			using (s.BeginTransaction())
			{
				// Act
				s.QueryOver<Person>()
					.Where(p => p.Name == "p2")
					.Select(p => p.BirthDate.YearPart())
					.SingleOrDefaultAsync<object>()
					.ContinueWith(task =>
					{
						var yearOfBirth = task.Result;

						// Assert
						yearOfBirth.GetType().Should().Be(typeof(int));
						yearOfBirth.Should().Be(2008);
					}).Wait();
			}
		}

		[Test]
		public void SelectAvgYearPartAsync()
		{
			// Arrange
			using (var s = OpenSession())
			using (s.BeginTransaction())
			{
				// Act
				s.QueryOver<Person>()
					.SelectList(list => list.SelectAvg(p => p.BirthDate.YearPart()))
					.SingleOrDefaultAsync<object>()
					.ContinueWith(task =>
					{
						var avgYear = task.Result;

						// Assert
						avgYear.GetType().Should().Be(typeof(double));
						string.Format("{0:0}", avgYear).Should().Be("2008");
					}).Wait();
			}
		}

		[Test]
		public void SqrtSingleOrDefaultAsync()
		{
			// Arrange
			using (var s = OpenSession())
			using (s.BeginTransaction())
			{
				// Act
				s.QueryOver<Person>()
					.Where(p => p.Name == "p1")
					.Select(p => p.Age.Sqrt())
					.SingleOrDefaultAsync<object>()
					.ContinueWith(task =>
					{
						var sqrtOfAge = task.Result;

						// Assert
						sqrtOfAge.Should().Be.InstanceOf<double>();
						string.Format("{0:0.00}", sqrtOfAge).Should().Be((9.49).ToString());
					}).Wait();
			}
		}

		[Test]
		public void FunctionsToLowerToUpperAsync()
		{
			// Arrange
			using (var s = OpenSession())
			using (s.BeginTransaction())
			{
				// Act
				s.QueryOver<Person>()
					.Where(p => p.Name == "pP3")
					.Select(p => p.Name.Lower(), p => p.Name.Upper())
					.SingleOrDefaultAsync<object[]>()
					.ContinueWith(task =>
					{
						var names = task.Result;

						// Assert
						names[0].Should().Be("pp3");
						names[1].Should().Be("PP3");
					}).Wait();
			}
		}

		[Test]
		public void ConcatAsync()
		{
			// Arrange
			using (var s = OpenSession())
			using (s.BeginTransaction())
			{
				// Act
				s.QueryOver<Person>()
					.Where(p => p.Name == "p1")
					.Select(p => Projections.Concat(p.Name, ", ", p.Name))
					.SingleOrDefaultAsync<string>()
					.ContinueWith(task =>
						// Assert
						task.Result.Should().Be("p1, p1")).Wait();
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
		public void YearSingleOrDefaultAsync()
		{
			// Arrange
			using (var s = OpenSession())
			using (s.BeginTransaction())
			{
				// Act
				s.QueryOver<Person>()
					.Where(p => p.Name == "p2")
					.Select(p => p.BirthDate.Year)
					.SingleOrDefaultAsync<object>()
					.ContinueWith(task =>
					{
						var yearOfBirth = task.Result;

						// Assert
						yearOfBirth.GetType().Should().Be(typeof(int));
						yearOfBirth.Should().Be(2008);
					}).Wait();
			}
		}

		[Test]
		public void SelectAvgYearAsync()
		{
			// Arrange
			using (var s = OpenSession())
			using (s.BeginTransaction())
			{
				// Act
				s.QueryOver<Person>()
					.SelectList(list => list.SelectAvg(p => p.BirthDate.Year))
					.SingleOrDefaultAsync<object>()
					.ContinueWith(task =>
					{
						var avgYear = task.Result;

						// Assert
						avgYear.GetType().Should().Be(typeof(double));
						string.Format("{0:0}", avgYear).Should().Be("2008");
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
