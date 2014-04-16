using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NHibernate.Hql.Ast.ANTLR;
using NUnit.Framework;
using SharpTestsEx;
using Environment = NHibernate.Cfg.Environment;

namespace NHibernate.Test.Hql.Ast
{
	[TestFixture]
	public class LimitClauseFixtureAsync : BaseFixture
	{
		protected override bool AppliesTo(Dialect.Dialect dialect)
		{
			return dialect.SupportsVariableLimit
				&& !(Dialect is Dialect.MsSql2000Dialect &&  cfg.Properties[Environment.ConnectionDriver] == typeof(Driver.OdbcDriver).FullName); // don't know why, but these tests don't work on SQL Server using ODBC
		}

		protected override void OnSetUp()
		{
			ISession session = OpenSession();
			ITransaction txn = session.BeginTransaction();

			var mother = new Human {BodyWeight = 10, Description = "mother"};
			var father = new Human {BodyWeight = 15, Description = "father"};
			var child1 = new Human {BodyWeight = 5, Description = "child1"};
			var child2 = new Human {BodyWeight = 6, Description = "child2"};
			var friend = new Human {BodyWeight = 20, Description = "friend"};

			session.Save(mother);
			session.Save(father);
			session.Save(child1);
			session.Save(child2);
			session.Save(friend);

			txn.Commit();
			session.Close();
		}

		protected override void OnTearDown()
		{
			ISession session = OpenSession();
			ITransaction txn = session.BeginTransaction();
			session.Delete("from Animal");
			txn.Commit();
			session.Close();
		}

		[Test]
		[ExpectedException(typeof(AggregateException))]
		public void ListAsyncTShouldReturnCanceledTaskWhenPassedCanceledToken()
		{
			// Arrange
			var cancellationTokenSource = new CancellationTokenSource();
			cancellationTokenSource.Cancel();
			const string hql = "from Human";
			Task result;

			using (ISession session = OpenSession())
			{
				// Act
				result = session.CreateQuery(hql).ListAsync<Human>(cancellationTokenSource.Token);
			}

			// Assert
			Assert.That(result.IsCanceled);
			result.Wait();
		}

		[Test]
		public void NoneAsync()
		{
			// Arrange
			ISession s = OpenSession();
			ITransaction txn = s.BeginTransaction();

			// Act
			s.CreateQuery("from Human h order by h.bodyWeight").ListAsync<Human>().ContinueWith(task =>
			{
				float[] actual = task.Result.Select(h => h.BodyWeight).ToArray();
				var expected = new[] { 5, 6, 10, 15, 20 };
				// Assert
				CollectionAssert.AreEqual(expected, actual);
			}).Wait();
			

			txn.Commit();
			s.Close();
		}

		[Test]
		public void SkipAsync()
		{
			// Arrange
			ISession s = OpenSession();
			ITransaction txn = s.BeginTransaction();

			// Act
			s.CreateQuery("from Human h where h.bodyWeight > :minW order by h.bodyWeight skip 2")
				.SetDouble("minW", 0d)
				.ListAsync<Human>()
				.ContinueWith(
					task =>
					{
						float[] actual = task.Result.Select(h => h.BodyWeight).ToArray();
						var expected = new[] { 10, 15, 20 };
						// Assert
						CollectionAssert.AreEqual(expected, actual);
					}).Wait();

			txn.Commit();
			s.Close();
		}

		[Test]
		public void SkipTakeAsync()
		{
			// Arrange
			ISession s = OpenSession();
			ITransaction txn = s.BeginTransaction();

			// Act
			s.CreateQuery("from Human h order by h.bodyWeight skip 1 take 3").ListAsync<Human>().ContinueWith(task =>
			{
				float[] actual = task.Result.Select(h => h.BodyWeight).ToArray();
				var expected = new[] { 6, 10, 15 };
				// Assert
				CollectionAssert.AreEqual(expected, actual);
			}).Wait();

			txn.Commit();
			s.Close();
		}

		[Test]
		public void SkipTakeWithParameterAsync()
		{
			// Arrange
			ISession s = OpenSession();
			ITransaction txn = s.BeginTransaction();

			// Act
			s.CreateQuery("from Human h order by h.bodyWeight skip :pSkip take :pTake")
				.SetInt32("pSkip", 1)
				.SetInt32("pTake", 3).ListAsync<Human>().ContinueWith(task =>
				{
					float[] actual = task.Result.Select(h => h.BodyWeight).ToArray();

					// Assert
					var expected = new[] { 6f, 10f, 15f };
					actual.Should().Have.SameSequenceAs(expected);
				}).Wait();

			txn.Commit();
			s.Close();
		}

		[Test]
		public void SkipTakeWithParameterListAsync()
		{
			// Arrange
			ISession s = OpenSession();
			ITransaction txn = s.BeginTransaction();

			// Act
			s.CreateQuery("from Human h where h.bodyWeight in (:list) order by h.bodyWeight skip :pSkip take :pTake")
				.SetParameterList("list", new[] {10f, 15f, 5f})
				.SetInt32("pSkip", 1)
				.SetInt32("pTake", 4).ListAsync<Human>().ContinueWith(task =>
				{
					float[] actual = task.Result.Select(h => h.BodyWeight).ToArray();

					// Assert
					var expected = new[] { 10f, 15f };
					actual.Should().Have.SameSequenceAs(expected);
				}).Wait();

			txn.Commit();
			s.Close();
		}

		[Test]
		public void SkipWithParameterAsync()
		{
			// Arrange
			ISession s = OpenSession();
			ITransaction txn = s.BeginTransaction();

			// Act
			s.CreateQuery("from Human h order by h.bodyWeight skip :jump").SetInt32("jump", 2).ListAsync<Human>().ContinueWith(
				task =>
				{
					float[] actual = task.Result.Select(h => h.BodyWeight).ToArray();

					// Assert
					var expected = new[] { 10f, 15f, 20f };
					actual.Should().Have.SameSequenceAs(expected);
				}).Wait();
			
			txn.Commit();
			s.Close();
		}

		[Test]
		public void TakeAsync()
		{
			// Arrange
			ISession s = OpenSession();
			ITransaction txn = s.BeginTransaction();

			// Act
			s.CreateQuery("from Human h order by h.bodyWeight take 2").ListAsync<Human>().ContinueWith(task =>
			{
				float[] actual = task.Result.Select(h => h.BodyWeight).ToArray();

				// Assert
				var expected = new[] { 5, 6 };
				CollectionAssert.AreEqual(expected, actual);
			}).Wait();

			txn.Commit();
			s.Close();
		}

		[Test]
		public void TakeSkipAsync()
		{
			// Arramge
			ISession s = OpenSession();
			ITransaction txn = s.BeginTransaction();

			// Act && Assert
			Assert.Throws<QuerySyntaxException>(() => 
				s.CreateQuery("from Human h order by h.bodyWeight take 1 skip 2").ListAsync<Human>().Wait(), 
				"take should not be allowed before skip");

			txn.Commit();
			s.Close();
		}

		[Test]
		public void TakeWithParameterAsync()
		{
			// Arrange
			ISession s = OpenSession();
			ITransaction txn = s.BeginTransaction();

			// Act
			s.CreateQuery("from Human h where h.bodyWeight > :minW order by h.bodyWeight take :jump")
				.SetDouble("minW", 1d)
				.SetInt32("jump", 2).ListAsync<Human>().ContinueWith(task =>
				{
					float[] actual = task.Result.Select(h => h.BodyWeight).ToArray();

					// Assert
					var expected = new[] { 5, 6 };
					CollectionAssert.AreEqual(expected, actual);
				}).Wait();

			txn.Commit();
			s.Close();
		}
	}
}