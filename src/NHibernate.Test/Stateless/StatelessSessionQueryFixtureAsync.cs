using System.Collections;
using NHibernate.Cfg;
using NUnit.Framework;

namespace NHibernate.Test.Stateless
{
	[TestFixture]
	public class StatelessSessionQueryFixtureAsync : TestCase
	{
		protected override string MappingsAssembly
		{
			get { return "NHibernate.Test"; }
		}

		protected override IList Mappings
		{
			get { return new[] {"Stateless.Contact.hbm.xml"}; }
		}

		protected override void Configure(Configuration configuration)
		{
			base.Configure(configuration);
			cfg.SetProperty(Environment.MaxFetchDepth, 1.ToString());
		}

		private class TestData
		{
			internal readonly IList list = new ArrayList();

			private readonly ISessionFactory sessions;

			public TestData(ISessionFactory sessions)
			{
				this.sessions = sessions;
			}

			public virtual void createData()
			{
				using (ISession session = sessions.OpenSession())
				{
					using (ITransaction tx = session.BeginTransaction())
					{
						var usa = new Country();
						session.Save(usa);
						list.Add(usa);
						var disney = new Org();
						disney.Country = usa;
						session.Save(disney);
						list.Add(disney);
						var waltDisney = new Contact();
						waltDisney.Org = disney;
						session.Save(waltDisney);
						list.Add(waltDisney);
						tx.Commit();
					}
				}
			}

			public virtual void cleanData()
			{
				using (ISession session = sessions.OpenSession())
				{
					using (ITransaction tx = session.BeginTransaction())
					{
						foreach (object obj in list)
						{
							session.Delete(obj);
						}

						tx.Commit();
					}
				}
			}
		}

		[Test]
		public void CriteriaAsync()
		{
			// Arrange
			var testData = new TestData(sessions);
			testData.createData();

			using (IStatelessSession s = sessions.OpenStatelessSession())
			{
				// Act
				s.CreateCriteria<Contact>().ListAsync().ContinueWith(task =>
					// Assert
					Assert.AreEqual(1, task.Result.Count)).Wait();
			}

			testData.cleanData();
		}

		[Test]
		public void CriteriaWithSelectFetchModeAsync()
		{
			// Arrange
			var testData = new TestData(sessions);
			testData.createData();

			using (IStatelessSession s = sessions.OpenStatelessSession())
			{
				// Act
				s.CreateCriteria<Contact>().SetFetchMode("Org", FetchMode.Select).ListAsync().ContinueWith(task =>
					// Assert
					Assert.AreEqual(1, task.Result.Count)).Wait();
			}

			testData.cleanData();
		}

		[Test]
		public void HqlAsync()
		{
			// Arrange
			var testData = new TestData(sessions);
			testData.createData();

			using (IStatelessSession s = sessions.OpenStatelessSession())
			{
				// Act
				s.CreateQuery("from Contact c join fetch c.Org join fetch c.Org.Country").ListAsync<Contact>().ContinueWith(task =>
					// Assert
					Assert.AreEqual(1, task.Result.Count)).Wait();
			}

			testData.cleanData();
		}
	}
}