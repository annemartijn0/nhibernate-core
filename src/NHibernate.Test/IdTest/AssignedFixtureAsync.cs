using System;
using System.Collections.Generic;
using log4net;
using log4net.Core;
using NUnit.Framework;

namespace NHibernate.Test.IdTest
{

	[TestFixture]
	public class AssignedFixtureAsync : IdFixtureBase
	{

		private string[] GetAssignedIdentifierWarnings(LogSpy ls)
		{
			List<string> warnings = new List<string>();

			foreach (string logEntry in ls.GetWholeLog().Split('\n'))
				if (logEntry.Contains("Unable to determine if") && logEntry.Contains("is transient or detached"))
					warnings.Add(logEntry);

			return warnings.ToArray();
		}

		protected override string TypeName
		{
			get { return "Assigned"; }
		}

		protected override void OnTearDown()
		{
			base.OnTearDown();

			using (ISession s = OpenSession())
			using (ITransaction t = s.BeginTransaction())
			{
				s.CreateQuery("delete from Child").ExecuteUpdate();
				s.CreateQuery("delete from Parent").ExecuteUpdate();
				t.Commit();
			}
		}

		[Test]
		public void SaveOrUpdate_SaveAsync()
		{
			// Arrange
			using (LogSpy ls = new LogSpy(LogManager.GetLogger("NHibernate"), Level.Warn))
			using (ISession s = OpenSession())
			{
				ITransaction t = s.BeginTransaction();

				Parent parent =
					new Parent()
					{
						Id = "parent",
						Children = new List<Child>(),
					};

				s.SaveOrUpdate(parent);
				t.Commit();

				// Act
				s.CreateQuery("select count(p) from Parent p").UniqueResultAsync<long>()
					.ContinueWith(task =>
					{
						long actual = task.Result;

						//Assert
						Assert.That(actual, Is.EqualTo(1));

						string[] warnings = GetAssignedIdentifierWarnings(ls);
						Assert.That(warnings.Length, Is.EqualTo(1));
						Assert.IsTrue(warnings[0].Contains("parent"));
					}).Wait();
			}
		}

		[Test]
		public void SaveNoWarningAsync()
		{
			// Arrange
			using (LogSpy ls = new LogSpy(LogManager.GetLogger("NHibernate"), Level.Warn))
			using (ISession s = OpenSession())
			{
				ITransaction t = s.BeginTransaction();

				Parent parent =
					new Parent()
					{
						Id = "parent",
						Children = new List<Child>(),
					};

				s.Save(parent);
				t.Commit();

				s.CreateQuery("select count(p) from Parent p").UniqueResultAsync<long>()
					.ContinueWith(task =>
					{
						long actual = task.Result;

						// Assert
						Assert.That(actual, Is.EqualTo(1));

						string[] warnings = GetAssignedIdentifierWarnings(ls);
						Assert.That(warnings.Length, Is.EqualTo(0));
					}).Wait();
			}
		}

		[Test]
		public void SaveOrUpdate_UpdateAsync()
		{
			// Arrange
			using (ISession s = OpenSession())
			{
				ITransaction t = s.BeginTransaction();

				s.Save(new Parent() { Id = "parent", Name = "before" });
				t.Commit();
			}

			using (LogSpy ls = new LogSpy(LogManager.GetLogger("NHibernate"), Level.Warn))
			using (ISession s = OpenSession())
			{
				ITransaction t = s.BeginTransaction();

				Parent parent =
					new Parent()
					{
						Id = "parent",
						Name = "after",
					};

				// Act
				s.SaveOrUpdate(parent);
				t.Commit();

				// Assert
				string[] warnings = GetAssignedIdentifierWarnings(ls);
				Assert.That(warnings.Length, Is.EqualTo(1));
				Assert.IsTrue(warnings[0].Contains("parent"));
			}

			using (ISession s = OpenSession())
			{
				// Act
				s.CreateQuery("from Parent").UniqueResultAsync<Parent>()
					.ContinueWith(task =>
					{
						Parent parent = task.Result;

						// Assert
						Assert.That(parent.Name, Is.EqualTo("after"));
					}).Wait();
			}
		}

		[Test]
		public void UpdateNoWarningAsync()
		{
			// Arrange
			using (ISession s = OpenSession())
			{
				ITransaction t = s.BeginTransaction();

				s.Save(new Parent() { Id = "parent", Name = "before" });
				t.Commit();
			}

			using (LogSpy ls = new LogSpy(LogManager.GetLogger("NHibernate"), Level.Warn))
			using (ISession s = OpenSession())
			{
				ITransaction t = s.BeginTransaction();

				Parent parent =
					new Parent()
					{
						Id = "parent",
						Name = "after",
					};

				// Act
				s.Update(parent);
				t.Commit();

				// Assert
				string[] warnings = GetAssignedIdentifierWarnings(ls);
				Assert.That(warnings.Length, Is.EqualTo(0));
			}

			using (ISession s = OpenSession())
			{
				// Act
				s.CreateQuery("from Parent").UniqueResultAsync<Parent>()
					.ContinueWith(task =>
					{
						Parent parent = task.Result;

						// Assert
						Assert.That(parent.Name, Is.EqualTo("after"));
					}).Wait();
			}
		}

		[Test]
		public void InsertCascadeAsync()
		{
			// Arrange
			using (ISession s = OpenSession())
			{
				ITransaction t = s.BeginTransaction();

				s.Save(new Child() { Id = "detachedChild" });
				t.Commit();
			}

			using (LogSpy ls = new LogSpy(LogManager.GetLogger("NHibernate"), Level.Warn))
			using (ISession s = OpenSession())
			{
				ITransaction t = s.BeginTransaction();

				Parent parent =
					new Parent()
					{
						Id = "parent",
						Children = new List<Child>(),
					};

				parent.Children.Add(new Child() { Id = "detachedChild", Parent = parent });
				parent.Children.Add(new Child() { Id = "transientChild", Parent = parent });

				s.Save(parent);
				t.Commit();

				// Act
				s.CreateQuery("select count(c) from Child c").UniqueResultAsync<long>()
					.ContinueWith(task =>
					{
						long actual = task.Result;

						// Assert
						Assert.That(actual, Is.EqualTo(2));

						string[] warnings = GetAssignedIdentifierWarnings(ls);
						Assert.That(warnings.Length, Is.EqualTo(2));
						Assert.IsTrue(warnings[0].Contains("detachedChild"));
						Assert.IsTrue(warnings[1].Contains("transientChild"));
					}).Wait();
			}
		}

		[Test]
		public void InsertCascadeNoWarningAsync()
		{
			// Arrange
			using (ISession s = OpenSession())
			{
				ITransaction t = s.BeginTransaction();

				s.Save(new Child() { Id = "persistedChild" });
				t.Commit();
			}

			using (LogSpy ls = new LogSpy(LogManager.GetLogger("NHibernate"), Level.Warn))
			using (ISession s = OpenSession())
			{
				ITransaction t = s.BeginTransaction();

				Parent parent =
					new Parent()
					{
						Id = "parent",
						Children = new List<Child>(),
					};

				s.Save(parent);

				Child child1 = s.Load<Child>("persistedChild");
				child1.Parent = parent;
				parent.Children.Add(child1);

				Child child2 = new Child() { Id = "transientChild", Parent = parent };
				s.Save(child2);
				parent.Children.Add(child2);

				t.Commit();

				// Act
				s.CreateQuery("select count(c) from Child c").UniqueResultAsync<long>()
					.ContinueWith(task =>
					{
						long actual = task.Result;

						// Assert
						Assert.That(actual, Is.EqualTo(2));

						string[] warnings = GetAssignedIdentifierWarnings(ls);
						Assert.That(warnings.Length, Is.EqualTo(0));
					}).Wait();
			}
		}

	}

}
