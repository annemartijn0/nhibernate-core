using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using NHibernate.AdoNet;
using NHibernate.Cache;
using NHibernate.Collection;
using NHibernate.Driver;
using NHibernate.Engine;
using NHibernate.Event;
using NHibernate.Exceptions;
using NHibernate.Hql;
using NHibernate.Hql.Util;
using NHibernate.Impl;
using NHibernate.Param;
using NHibernate.Persister.Collection;
using NHibernate.Persister.Entity;
using NHibernate.Proxy;
using NHibernate.SqlCommand;
using NHibernate.Transform;
using NHibernate.Type;
using NHibernate.Util;

namespace NHibernate.Loader
{
	/// <summary>
	/// Abstract superclass of object loading (and querying) strategies.
	/// </summary>
	/// <remarks>
	/// <p>
	/// This class implements useful common functionality that concrete loaders would delegate to.
	/// It is not intended that this functionality would be directly accessed by client code (Hence,
	/// all methods of this class are declared <c>protected</c> or <c>private</c>.) This class relies heavily upon the
	/// <see cref="ILoadable" /> interface, which is the contract between this class and 
	/// <see cref="IEntityPersister" />s that may be loaded by it.
	/// </p>
	/// <p>
	/// The present implementation is able to load any number of columns of entities and at most 
	/// one collection role per query.
	/// </p>
	/// </remarks>
	/// <seealso cref="NHibernate.Persister.Entity.ILoadable"/>
	public abstract class Loader
	{
		private static readonly IInternalLogger Log = LoggerProvider.LoggerFor(typeof(Loader));

		private readonly ISessionFactoryImplementor _factory;
		private readonly SessionFactoryHelper _helper;
		private ColumnNameCache _columnNameCache;

		/// <summary>
		/// Indicates whether the dialect is able to add limit and/or offset clauses to <see cref="SqlString"/>.
		/// Even if a dialect generally supports the addition of limit and/or offset clauses to SQL statements,
		/// there may (custom) SQL statements where this is not possible, for example in case of SQL Server 
		/// stored procedure invocations.
		/// </summary>
		private bool? _canUseLimits;

		protected Loader(ISessionFactoryImplementor factory)
		{
			_factory = factory;
			_helper = new SessionFactoryHelper(factory);
		}

		protected SessionFactoryHelper Helper
		{
			get { return _helper; }
		}

		/// <summary> 
		/// An array indicating whether the entities have eager property fetching
		/// enabled. 
		/// </summary>
		/// <value> Eager property fetching indicators. </value>
		protected virtual bool[] EntityEagerPropertyFetches
		{
			get { return null; }
		}

		/// <summary>
		/// An array of indexes of the entity that owns a one-to-one association
		/// to the entity at the given index (-1 if there is no "owner")
		/// </summary>
		/// <remarks>
		/// The indexes contained here are relative to the result of <see cref="EntityPersisters"/>.
		/// </remarks>
		protected virtual int[] Owners
		{
			get { return null; }
		}

		/// <summary> 
		/// An array of the owner types corresponding to the <see cref="Owners"/>
		/// returns.  Indices indicating no owner would be null here. 
		/// </summary>
		protected virtual EntityType[] OwnerAssociationTypes
		{
			get { return null; }
		}

		/// <summary>
		/// Get the index of the entity that owns the collection, or -1
		/// if there is no owner in the query results (i.e. in the case of a 
		/// collection initializer) or no collection.
		/// </summary>
		protected virtual int[] CollectionOwners
		{
			get { return null; }
		}

		/// <summary>
		/// Return false is this loader is a batch entity loader
		/// </summary>
		protected virtual bool IsSingleRowLoader
		{
			get { return false; }
		}

		public virtual bool IsSubselectLoadingEnabled
		{
			get { return false; }
		}

		/// <summary>
		/// Get the result set descriptor
		/// </summary>
		protected abstract IEntityAliases[] EntityAliases { get; }

		protected abstract ICollectionAliases[] CollectionAliases { get; }

		public ISessionFactoryImplementor Factory
		{
			get { return _factory; }
		}

		/// <summary>
		/// The SqlString to be called; implemented by all subclasses
		/// </summary>
		public abstract SqlString SqlString { get; }

		/// <summary>
		/// An array of persisters of entity classes contained in each row of results;
		/// implemented by all subclasses
		/// </summary>
		/// <remarks>
		/// The <c>setter</c> was added so that classes inheriting from Loader could write a 
		/// value using the Property instead of directly to the field.
		/// </remarks>
		public abstract ILoadable[] EntityPersisters { get; }

		/// <summary>
		/// An (optional) persister for a collection to be initialized; only collection loaders
		/// return a non-null value
		/// </summary>
		protected virtual ICollectionPersister[] CollectionPersisters
		{
			get { return null; }
		}

		/// <summary>
		/// What lock mode does this load entities with?
		/// </summary>
		/// <param name="lockModes">A Collection of lock modes specified dynamically via the Query Interface</param>
		/// <returns></returns>
		public abstract LockMode[] GetLockModes(IDictionary<string, LockMode> lockModes);

		/// <summary>
		/// Append <c>FOR UPDATE OF</c> clause, if necessary. This
		/// empty superclass implementation merely returns its first
		/// argument.
		/// </summary>
		protected virtual SqlString ApplyLocks(SqlString sql, IDictionary<string, LockMode> lockModes, Dialect.Dialect dialect)
		{
			return sql;
		}

		/// <summary>
		/// Does this query return objects that might be already cached by 
		/// the session, whose lock mode may need upgrading.
		/// </summary>
		/// <returns></returns>
		protected virtual bool UpgradeLocks()
		{
			return false;
		}

		/// <summary>
		/// Get the SQL table aliases of entities whose
		/// associations are subselect-loadable, returning
		/// null if this loader does not support subselect
		/// loading
		/// </summary>
		protected virtual string[] Aliases
		{
			get { return null; }
		}

		/// <summary>
		/// Modify the SQL, adding lock hints and comments, if necessary
		/// </summary>
		protected virtual SqlString PreprocessSQL(SqlString sql, QueryParameters parameters, Dialect.Dialect dialect)
		{
			sql = ApplyLocks(sql, parameters.LockModes, dialect);

			return Factory.Settings.IsCommentsEnabled ? PrependComment(sql, parameters) : sql;
		}

		private static SqlString PrependComment(SqlString sql, QueryParameters parameters)
		{
			string comment = parameters.Comment;
			if (string.IsNullOrEmpty(comment))
			{
				return sql;
			}
			else
			{
				return sql.Insert(0, "/* " + comment + " */");
			}
		}

		/// <summary>
		/// Execute an SQL query and attempt to instantiate instances of the class mapped by the given
		/// persister from each row of the <c>DataReader</c>. If an object is supplied, will attempt to
		/// initialize that object. If a collection is supplied, attempt to initialize that collection.
		/// </summary>
		private IList DoQueryAndInitializeNonLazyCollections(ISessionImplementor session, QueryParameters queryParameters, bool returnProxies)
		{
			var beforeParams = BeforeDoQueryAndInitializeNonLazyCollections(new BeforeDoQueryAndInitializeNonLazyCollectionsParams(session, queryParameters));
			IList result;
			try
			{
				try
				{
					result = DoQuery(session, queryParameters, returnProxies);
				}
				finally
				{
					beforeParams.PersistenceContext.AfterLoad();
				}
				beforeParams.PersistenceContext.InitializeNonLazyCollections();
			}
			finally
			{
				beforeParams.PersistenceContext.DefaultReadOnly = beforeParams.DefaultReadOnlyOrig;
			}

			return result;
		}

		/// <summary>
		/// Execute an SQL query asynchronously and attempt to instantiate instances of the class mapped by the given
		/// persister from each row of the <c>DataReader</c>. If an object is supplied, will attempt to
		/// initialize that object. If a collection is supplied, attempt to initialize that collection.
		/// </summary>
		private Task<IList> DoQueryAndInitializeNonLazyCollectionsAsync(ISessionImplementor session, CancellationToken cancellationToken, QueryParameters queryParameters, bool returnProxies)
		{
			var beforeParams = BeforeDoQueryAndInitializeNonLazyCollections(new BeforeDoQueryAndInitializeNonLazyCollectionsParams(session, queryParameters));
			var task = DoQueryAsync(session, cancellationToken, queryParameters, returnProxies);
			if (task.IsCanceled)
			{
				EndDoQueryAndInitializeNonLazyCollectionsAsync(beforeParams.PersistenceContext, beforeParams.DefaultReadOnlyOrig);
				return CanceledIListTask();
			}
			return task.ContinueWith(delegate
				{
					try
					{
						return task.Result;
					}
					finally
					{
						EndDoQueryAndInitializeNonLazyCollectionsAsync(beforeParams.PersistenceContext, beforeParams.DefaultReadOnlyOrig);
					}
				});
		}

		private static BeforeDoQueryAndInitializeNonLazyCollectionsParams BeforeDoQueryAndInitializeNonLazyCollections(BeforeDoQueryAndInitializeNonLazyCollectionsParams beforeParams)
		{
			beforeParams.PersistenceContext = beforeParams.Session.PersistenceContext;
			beforeParams.DefaultReadOnlyOrig = beforeParams.PersistenceContext.DefaultReadOnly;

			if (beforeParams.QueryParameters.IsReadOnlyInitialized)
				beforeParams.PersistenceContext.DefaultReadOnly = beforeParams.QueryParameters.ReadOnly;
			else
				beforeParams.QueryParameters.ReadOnly = beforeParams.PersistenceContext.DefaultReadOnly;

			beforeParams.PersistenceContext.BeforeLoad();
			return beforeParams;
		}

		private static Task<IList> CanceledIListTask()
		{
			var taskCompletionSource = new TaskCompletionSource<IList>();
			taskCompletionSource.SetCanceled();
			return taskCompletionSource.Task;
		}

		private static void EndDoQueryAndInitializeNonLazyCollectionsAsync(IPersistenceContext persistenceContext, bool defaultReadOnlyOrig)
		{
			try
			{
				persistenceContext.InitializeNonLazyCollections();
			}
			finally
			{
				persistenceContext.AfterLoad();

				persistenceContext.DefaultReadOnly = defaultReadOnlyOrig;
			}
		}

		/// <summary>
		/// Loads a single row from the result set.  This is the processing used from the
		/// ScrollableResults where no collection fetches were encountered.
		/// </summary>
		/// <param name="resultSet">The result set from which to do the load.</param>
		/// <param name="session">The session from which the request originated.</param>
		/// <param name="queryParameters">The query parameters specified by the user.</param>
		/// <param name="returnProxies">Should proxies be generated</param>
		/// <returns>The loaded "row".</returns>
		/// <exception cref="HibernateException" />
		protected object LoadSingleRow(DbDataReader resultSet, ISessionImplementor session, QueryParameters queryParameters,
									   bool returnProxies)
		{
			int entitySpan = EntityPersisters.Length;
			IList hydratedObjects = entitySpan == 0 ? null : new List<object>(entitySpan);

			object result;
			try
			{
				result =
					GetRowFromResultSet(resultSet, session, queryParameters, GetLockModes(queryParameters.LockModes), null,
										hydratedObjects, new EntityKey[entitySpan], returnProxies);
			}
			catch (HibernateException)
			{
				throw; // Don't call Convert on HibernateExceptions
			}
			catch (Exception sqle)
			{
				throw ADOExceptionHelper.Convert(Factory.SQLExceptionConverter, sqle, "could not read next row of results",
												 SqlString, queryParameters.PositionalParameterValues,
												 queryParameters.NamedParameters);
			}

			InitializeEntitiesAndCollections(hydratedObjects, resultSet, session, queryParameters.IsReadOnly(session));
			session.PersistenceContext.InitializeNonLazyCollections();
			return result;
		}

		// Not ported: sequentialLoad, loadSequentialRowsForward, loadSequentialRowsReverse

		internal static EntityKey GetOptionalObjectKey(QueryParameters queryParameters, ISessionImplementor session)
		{
			object optionalObject = queryParameters.OptionalObject;
			object optionalId = queryParameters.OptionalId;
			string optionalEntityName = queryParameters.OptionalEntityName;

			if (optionalObject != null && !string.IsNullOrEmpty(optionalEntityName))
			{
				return session.GenerateEntityKey(optionalId, session.GetEntityPersister(optionalEntityName, optionalObject));
			}
			else
			{
				return null;
			}
		}

		internal object GetRowFromResultSet(DbDataReader resultSet, ISessionImplementor session,
											QueryParameters queryParameters, LockMode[] lockModeArray,
											EntityKey optionalObjectKey, IList hydratedObjects, EntityKey[] keys,
											bool returnProxies)
		{
			ILoadable[] persisters = EntityPersisters;
			int entitySpan = persisters.Length;

			for (int i = 0; i < entitySpan; i++)
			{
				keys[i] =
					GetKeyFromResultSet(i, persisters[i], i == entitySpan - 1 ? queryParameters.OptionalId : null, resultSet, session);
				//TODO: the i==entitySpan-1 bit depends upon subclass implementation (very bad)
			}

			RegisterNonExists(keys, session);

			// this call is side-effecty
			object[] row =
				GetRow(resultSet, persisters, keys, queryParameters.OptionalObject, optionalObjectKey, lockModeArray,
					   hydratedObjects, session);

			ReadCollectionElements(row, resultSet, session);

			if (returnProxies)
			{
				// now get an existing proxy for each row element (if there is one)
				for (int i = 0; i < entitySpan; i++)
				{
					object entity = row[i];
					object proxy = session.PersistenceContext.ProxyFor(persisters[i], keys[i], entity);

					if (entity != proxy)
					{
						// Force the proxy to resolve itself
						((INHibernateProxy)proxy).HibernateLazyInitializer.SetImplementation(entity);
						row[i] = proxy;
					}
				}
			}

			return GetResultColumnOrRow(row, queryParameters.ResultTransformer, resultSet, session);
		}

		/// <summary>
		/// Read any collection elements contained in a single row of the result set
		/// </summary>
		private void ReadCollectionElements(object[] row, DbDataReader resultSet, ISessionImplementor session)
		{
			//TODO: make this handle multiple collection roles!

			ICollectionPersister[] collectionPersisters = CollectionPersisters;

			if (collectionPersisters != null)
			{
				ICollectionAliases[] descriptors = CollectionAliases;
				int[] collectionOwners = CollectionOwners;

				for (int i = 0; i < collectionPersisters.Length; i++)
				{
					bool hasCollectionOwners = collectionOwners != null && collectionOwners[i] > -1;
					//true if this is a query and we are loading multiple instances of the same collection role
					//otherwise this is a CollectionInitializer and we are loading up a single collection or batch

					object owner = hasCollectionOwners ? row[collectionOwners[i]] : null;
					//if null, owner will be retrieved from session

					ICollectionPersister collectionPersister = collectionPersisters[i];
					object key;

					if (owner == null)
					{
						key = null;
					}
					else
					{
						key = collectionPersister.CollectionType.GetKeyOfOwner(owner, session);
						//TODO: old version did not require hashmap lookup:
						//keys[collectionOwner].getIdentifier()
					}

					ReadCollectionElement(owner, key, collectionPersister, descriptors[i], resultSet, session);
				}
			}
		}

		private IList DoQuery(ISessionImplementor session, QueryParameters queryParameters, bool returnProxies)
		{
			using (new SessionIdLoggingContext(session.SessionId))
			{
				var beforeParams = BeforeDoQuery(new BeforeDoQueryParams(session, queryParameters));
				DbDataReader rs = GetResultSet(
					beforeParams.DbCommand, 
					queryParameters.HasAutoDiscoverScalarTypes, 
					queryParameters.Callable,
					beforeParams.Selection,
					session);

				return Results(
					session, 
					CancellationToken.None, 
					queryParameters, 
					returnProxies, 
					rs,
					beforeParams.EntitySpan,
					beforeParams.MaxRows,
					beforeParams.HydratedObjects,
					beforeParams.DbCommand);
			}
		}

		private Task<IList> DoQueryAsync(ISessionImplementor session, CancellationToken cancellationToken,
			QueryParameters queryParameters, bool returnProxies)
		{
			var sessionIdLoggingContext = new SessionIdLoggingContext(session.SessionId);
			var beforeParams = BeforeDoQuery(new BeforeDoQueryParams(session, queryParameters));

			return GetResultSetAsync(
				beforeParams.DbCommand, 
				cancellationToken, 
				queryParameters.HasAutoDiscoverScalarTypes, 
				queryParameters.Callable, 
				beforeParams.Selection, 
				session)
				.ContinueWith(task =>
				{
					try
					{
						return Results(
							session, 
							cancellationToken, 
							queryParameters, 
							returnProxies, 
							task.Result, 
							beforeParams.EntitySpan, 
							beforeParams.MaxRows, 
							beforeParams.HydratedObjects, 
							beforeParams.DbCommand);
					}
					finally
					{
						sessionIdLoggingContext.Dispose();
					}
				}, cancellationToken);
		}

		private BeforeDoQueryParams BeforeDoQuery(BeforeDoQueryParams beforeParams)
		{
			beforeParams.Selection = beforeParams.QueryParameters.RowSelection;
			beforeParams.MaxRows = HasMaxRows(beforeParams.Selection) ? beforeParams.Selection.MaxRows : int.MaxValue;

			beforeParams.EntitySpan = EntityPersisters.Length;

			beforeParams.HydratedObjects = beforeParams.EntitySpan == 0 ? null : new List<object>(beforeParams.EntitySpan * 10);

			beforeParams.DbCommand = PrepareQueryCommand(beforeParams.QueryParameters, false, beforeParams.Session);
			return beforeParams;
		}

		private IList Results(ISessionImplementor session, CancellationToken cancellationToken, QueryParameters queryParameters, bool returnProxies, DbDataReader rs, int entitySpan, int maxRows, List<object> hydratedObjects, DbCommand st)
		{
			LockMode[] lockModeArray = GetLockModes(queryParameters.LockModes);
			EntityKey optionalObjectKey = GetOptionalObjectKey(queryParameters, session);

			bool createSubselects = IsSubselectLoadingEnabled;
			List<EntityKey[]> subselectResultKeys = createSubselects ? new List<EntityKey[]>() : null;
			IList results = new List<object>();

			try
			{
				HandleEmptyCollections(queryParameters.CollectionKeys, rs, session);
				EntityKey[] keys = new EntityKey[entitySpan]; // we can reuse it each time

				if (Log.IsDebugEnabled)
				{
					Log.Debug("processing result set");
				}

				int count;
				for (count = 0; count < maxRows && rs.Read(); count++)
				{
					cancellationToken.ThrowIfCancellationRequested();

					if (Log.IsDebugEnabled)
					{
						Log.Debug("result set row: " + count);
					}

					object result = GetRowFromResultSet(rs, session, queryParameters, lockModeArray, optionalObjectKey,
						hydratedObjects,
						keys, returnProxies);
					results.Add(result);

					if (createSubselects)
					{
						subselectResultKeys.Add(keys);
						keys = new EntityKey[entitySpan]; //can't reuse in this case
					}
				}

				if (Log.IsDebugEnabled)
				{
					Log.Debug(string.Format("done processing result set ({0} rows)", count));
				}
			}
			catch (Exception e)
			{
				e.Data["actual-sql-query"] = st.CommandText;
				throw;
			}
			finally
			{
				session.Batcher.CloseCommand(st, rs);
			}

			InitializeEntitiesAndCollections(hydratedObjects, rs, session, queryParameters.IsReadOnly(session));

			if (createSubselects)
			{
				CreateSubselects(subselectResultKeys, queryParameters, session);
			}

			return results;
		}

		protected bool HasSubselectLoadableCollections()
		{
			foreach (ILoadable loadable in EntityPersisters)
			{
				if (loadable.HasSubselectLoadableCollections)
				{
					return true;
				}
			}

			return false;
		}

		private static ISet<EntityKey>[] Transpose(IList<EntityKey[]> keys)
		{
			ISet<EntityKey>[] result = new ISet<EntityKey>[keys[0].Length];
			for (int j = 0; j < result.Length; j++)
			{
				result[j] = new HashSet<EntityKey>();
				for (int i = 0; i < keys.Count; i++)
				{
					EntityKey key = keys[i][j];
					if (key != null)
					{
						result[j].Add(key);
					}
				}
			}
			return result;
		}

		internal void CreateSubselects(IList<EntityKey[]> keys, QueryParameters queryParameters, ISessionImplementor session)
		{
			if (keys.Count > 1)
			{
				//if we only returned one entity, query by key is more efficient
				var subSelects = CreateSubselects(keys, queryParameters).ToArray();

				foreach (EntityKey[] rowKeys in keys)
				{
					for (int i = 0; i < rowKeys.Length; i++)
					{
						if (rowKeys[i] != null && subSelects[i] != null)
						{
							session.PersistenceContext.BatchFetchQueue.AddSubselect(rowKeys[i], subSelects[i]);
						}
					}
				}
			}
		}

		private IEnumerable<SubselectFetch> CreateSubselects(IList<EntityKey[]> keys, QueryParameters queryParameters)
		{
			// see NH-2123 NH-2125
			ISet<EntityKey>[] keySets = Transpose(keys);
			ILoadable[] loadables = EntityPersisters;
			string[] aliases = Aliases;

			for (int i = 0; i < loadables.Length; i++)
			{
				if (loadables[i].HasSubselectLoadableCollections)
				{
					yield return new SubselectFetch(aliases[i], loadables[i], queryParameters, keySets[i]);
				}
				else
				{
					yield return null;
				}
			}
		}

		internal void InitializeEntitiesAndCollections(IList hydratedObjects, object resultSetId, ISessionImplementor session, bool readOnly)
		{
			ICollectionPersister[] collectionPersisters = CollectionPersisters;
			if (collectionPersisters != null)
			{
				for (int i = 0; i < collectionPersisters.Length; i++)
				{
					if (collectionPersisters[i].IsArray)
					{
						//for arrays, we should end the collection load before resolving
						//the entities, since the actual array instances are not instantiated
						//during loading
						//TODO: or we could do this polymorphically, and have two
						//      different operations implemented differently for arrays
						EndCollectionLoad(resultSetId, session, collectionPersisters[i]);
					}
				}
			}
			//important: reuse the same event instances for performance!
			PreLoadEvent pre;
			PostLoadEvent post;
			if (session.IsEventSource)
			{
				var eventSourceSession = (IEventSource)session;
				pre = new PreLoadEvent(eventSourceSession);
				post = new PostLoadEvent(eventSourceSession);
			}
			else
			{
				pre = null;
				post = null;
			}

			if (hydratedObjects != null)
			{
				int hydratedObjectsSize = hydratedObjects.Count;

				if (Log.IsDebugEnabled)
				{
					Log.Debug(string.Format("total objects hydrated: {0}", hydratedObjectsSize));
				}

				for (int i = 0; i < hydratedObjectsSize; i++)
				{
					TwoPhaseLoad.InitializeEntity(hydratedObjects[i], readOnly, session, pre, post);
				}
			}

			if (collectionPersisters != null)
			{
				for (int i = 0; i < collectionPersisters.Length; i++)
				{
					if (!collectionPersisters[i].IsArray)
					{
						//for sets, we should end the collection load after resolving
						//the entities, since we might call hashCode() on the elements
						//TODO: or we could do this polymorphically, and have two
						//      different operations implemented differently for arrays
						EndCollectionLoad(resultSetId, session, collectionPersisters[i]);
					}
				}
			}
		}

		private static void EndCollectionLoad(object resultSetId, ISessionImplementor session, ICollectionPersister collectionPersister)
		{
			//this is a query and we are loading multiple instances of the same collection role
			session.PersistenceContext.LoadContexts.GetCollectionLoadContext((DbDataReader)resultSetId).EndLoadingCollections(
				collectionPersister);
		}

		public virtual IList GetResultList(IList results, IResultTransformer resultTransformer)
		{
			return results;
		}

		/// <summary>
		/// Get the actual object that is returned in the user-visible result list.
		/// </summary>
		/// <remarks>
		/// This empty implementation merely returns its first argument. This is
		/// overridden by some subclasses.
		/// </remarks>
		protected virtual object GetResultColumnOrRow(object[] row, IResultTransformer resultTransformer, DbDataReader rs, ISessionImplementor session)
		{
			return row;
		}

		/// <summary>
		/// For missing objects associated by one-to-one with another object in the
		/// result set, register the fact that the the object is missing with the
		/// session.
		/// </summary>
		private void RegisterNonExists(EntityKey[] keys, ISessionImplementor session)
		{
			int[] owners = Owners;
			if (owners != null)
			{
				EntityType[] ownerAssociationTypes = OwnerAssociationTypes;
				for (int i = 0; i < keys.Length; i++)
				{
					int owner = owners[i];
					if (owner > -1)
					{
						EntityKey ownerKey = keys[owner];
						if (keys[i] == null && ownerKey != null)
						{
							bool isOneToOneAssociation = ownerAssociationTypes != null && ownerAssociationTypes[i] != null
														 && ownerAssociationTypes[i].IsOneToOne;
							if (isOneToOneAssociation)
							{
								session.PersistenceContext.AddNullProperty(ownerKey, ownerAssociationTypes[i].PropertyName);
							}
						}
					}
				}
			}
		}

		/// <summary>
		/// Read one collection element from the current row of the ADO.NET result set
		/// </summary>
		private static void ReadCollectionElement(object optionalOwner, object optionalKey, ICollectionPersister persister,
												  ICollectionAliases descriptor, DbDataReader rs, ISessionImplementor session)
		{
			IPersistenceContext persistenceContext = session.PersistenceContext;

			object collectionRowKey = persister.ReadKey(rs, descriptor.SuffixedKeyAliases, session);

			if (collectionRowKey != null)
			{
				// we found a collection element in the result set

				if (Log.IsDebugEnabled)
				{
					Log.Debug("found row of collection: " + MessageHelper.InfoString(persister, collectionRowKey));
				}

				object owner = optionalOwner;
				if (owner == null)
				{
					owner = persistenceContext.GetCollectionOwner(collectionRowKey, persister);
					if (owner == null)
					{
						//TODO: This is assertion is disabled because there is a bug that means the
						//      original owner of a transient, uninitialized collection is not known 
						//      if the collection is re-referenced by a different object associated 
						//      with the current Session
						//throw new AssertionFailure("bug loading unowned collection");
					}
				}
				IPersistentCollection rowCollection =
					persistenceContext.LoadContexts.GetCollectionLoadContext(rs).GetLoadingCollection(persister, collectionRowKey);

				if (rowCollection != null)
				{
					rowCollection.ReadFrom(rs, persister, descriptor, owner);
				}
			}
			else if (optionalKey != null)
			{
				// we did not find a collection element in the result set, so we
				// ensure that a collection is created with the owner's identifier,
				// since what we have is an empty collection

				if (Log.IsDebugEnabled)
				{
					Log.Debug("result set contains (possibly empty) collection: " + MessageHelper.InfoString(persister, optionalKey));
				}
				persistenceContext.LoadContexts.GetCollectionLoadContext(rs).GetLoadingCollection(persister, optionalKey);
				// handle empty collection
			}

			// else no collection element, but also no owner
		}

		/// <summary>
		/// If this is a collection initializer, we need to tell the session that a collection
		/// is being initilized, to account for the possibility of the collection having
		/// no elements (hence no rows in the result set).
		/// </summary>
		internal void HandleEmptyCollections(object[] keys, object resultSetId, ISessionImplementor session)
		{
			if (keys != null)
			{
				// this is a collection initializer, so we must create a collection
				// for each of the passed-in keys, to account for the possibility
				// that the collection is empty and has no rows in the result set

				ICollectionPersister[] collectionPersisters = CollectionPersisters;
				for (int j = 0; j < collectionPersisters.Length; j++)
				{
					for (int i = 0; i < keys.Length; i++)
					{
						// handle empty collections
						if (Log.IsDebugEnabled)
						{
							Log.Debug("result set contains (possibly empty) collection: "
									  + MessageHelper.InfoString(collectionPersisters[j], keys[i]));
						}
						session.PersistenceContext.LoadContexts.GetCollectionLoadContext((DbDataReader)resultSetId).GetLoadingCollection(
							collectionPersisters[j], keys[i]);
					}
				}
			}
			// else this is not a collection initializer (and empty collections will
			// be detected by looking for the owner's identifier in the result set)
		}

		/// <summary>
		/// Read a row of <c>EntityKey</c>s from the <c>DbDataReader</c> into the given array.
		/// </summary>
		/// <remarks>
		/// Warning: this method is side-effecty. If an <c>id</c> is given, don't bother going
		/// to the <c>DbDataReader</c>
		/// </remarks>
		private EntityKey GetKeyFromResultSet(int i, IEntityPersister persister, object id, DbDataReader rs, ISessionImplementor session)
		{
			object resultId;

			// if we know there is exactly 1 row, we can skip.
			// it would be great if we could _always_ skip this;
			// it is a problem for <key-many-to-one>

			if (IsSingleRowLoader && id != null)
			{
				resultId = id;
			}
			else
			{
				IType idType = persister.IdentifierType;
				resultId = idType.NullSafeGet(rs, EntityAliases[i].SuffixedKeyAliases, session, null);

				bool idIsResultId = id != null && resultId != null && idType.IsEqual(id, resultId, session.EntityMode, _factory);

				if (idIsResultId)
				{
					resultId = id; //use the id passed in
				}
			}

			return resultId == null ? null : session.GenerateEntityKey(resultId, persister);
		}

		/// <summary>
		/// Check the version of the object in the <c>DbDataReader</c> against
		/// the object version in the session cache, throwing an exception
		/// if the version numbers are different.
		/// </summary>
		/// <exception cref="StaleObjectStateException"></exception>
		private void CheckVersion(int i, IEntityPersister persister, object id, object entity, DbDataReader rs, ISessionImplementor session)
		{
			object version = session.PersistenceContext.GetEntry(entity).Version;

			// null version means the object is in the process of being loaded somewhere else in the ResultSet
			if (version != null)
			{
				IVersionType versionType = persister.VersionType;
				object currentVersion = versionType.NullSafeGet(rs, EntityAliases[i].SuffixedVersionAliases, session, null);
				if (!versionType.IsEqual(version, currentVersion))
				{
					if (session.Factory.Statistics.IsStatisticsEnabled)
					{
						session.Factory.StatisticsImplementor.OptimisticFailure(persister.EntityName);
					}

					throw new StaleObjectStateException(persister.EntityName, id);
				}
			}
		}

		/// <summary>
		/// Resolve any ids for currently loaded objects, duplications within the <c>DbDataReader</c>,
		/// etc. Instanciate empty objects to be initialized from the <c>DbDataReader</c>. Return an
		/// array of objects (a row of results) and an array of booleans (by side-effect) that determine
		/// wheter the corresponding object should be initialized
		/// </summary>
		private object[] GetRow(DbDataReader rs, ILoadable[] persisters, EntityKey[] keys, object optionalObject,
								EntityKey optionalObjectKey, LockMode[] lockModes, IList hydratedObjects,
								ISessionImplementor session)
		{
			int cols = persisters.Length;
			IEntityAliases[] descriptors = EntityAliases;

			if (Log.IsDebugEnabled)
			{
				Log.Debug("result row: " + StringHelper.ToString(keys));
			}

			object[] rowResults = new object[cols];

			for (int i = 0; i < cols; i++)
			{
				object obj = null;
				EntityKey key = keys[i];

				if (keys[i] == null)
				{
					// do nothing
					/* TODO NH-1001 : if (persisters[i]...EntityType) is an OneToMany or a ManyToOne and
					 * the keys.length > 1 and the relation IsIgnoreNotFound probably we are in presence of
					 * an load with "outer join" the relation can be considerer loaded even if the key is null (mean not found)
					*/
				}
				else
				{
					//If the object is already loaded, return the loaded one
					obj = session.GetEntityUsingInterceptor(key);
					if (obj != null)
					{
						//its already loaded so dont need to hydrate it
						InstanceAlreadyLoaded(rs, i, persisters[i], key, obj, lockModes[i], session);
					}
					else
					{
						obj =
							InstanceNotYetLoaded(rs, i, persisters[i], key, lockModes[i], descriptors[i].RowIdAlias, optionalObjectKey,
												 optionalObject, hydratedObjects, session);
					}
				}

				rowResults[i] = obj;
			}
			return rowResults;
		}

		/// <summary>
		/// The entity instance is already in the session cache
		/// </summary>
		private void InstanceAlreadyLoaded(DbDataReader rs, int i, IEntityPersister persister, EntityKey key, object obj,
										   LockMode lockMode, ISessionImplementor session)
		{
			if (!persister.IsInstance(obj, session.EntityMode))
			{
				string errorMsg = string.Format("loading object was of wrong class [{0}]", obj.GetType().FullName);
				throw new WrongClassException(errorMsg, key.Identifier, persister.EntityName);
			}

			if (LockMode.None != lockMode && UpgradeLocks())
			{
				EntityEntry entry = session.PersistenceContext.GetEntry(obj);
				bool isVersionCheckNeeded = persister.IsVersioned && entry.LockMode.LessThan(lockMode);

				// we don't need to worry about existing version being uninitialized
				// because this block isn't called by a re-entrant load (re-entrant
				// load _always_ have lock mode NONE
				if (isVersionCheckNeeded)
				{
					// we only check the version when _upgrading_ lock modes
					CheckVersion(i, persister, key.Identifier, obj, rs, session);
					// we need to upgrade the lock mode to the mode requested
					entry.LockMode = lockMode;
				}
			}
		}

		/// <summary>
		/// The entity instance is not in the session cache
		/// </summary>
		private object InstanceNotYetLoaded(DbDataReader dr, int i, ILoadable persister, EntityKey key, LockMode lockMode,
											string rowIdAlias, EntityKey optionalObjectKey, object optionalObject,
											IList hydratedObjects, ISessionImplementor session)
		{
			object obj;

			string instanceClass = GetInstanceClass(dr, i, persister, key.Identifier, session);

			if (optionalObjectKey != null && key.Equals(optionalObjectKey))
			{
				// its the given optional object
				obj = optionalObject;
			}
			else
			{
				obj = session.Instantiate(instanceClass, key.Identifier);
			}

			// need to hydrate it

			// grab its state from the DataReader and keep it in the Session
			// (but don't yet initialize the object itself)
			// note that we acquired LockMode.READ even if it was not requested
			LockMode acquiredLockMode = lockMode == LockMode.None ? LockMode.Read : lockMode;
			LoadFromResultSet(dr, i, obj, instanceClass, key, rowIdAlias, acquiredLockMode, persister, session);

			// materialize associations (and initialize the object) later
			hydratedObjects.Add(obj);

			return obj;
		}

		private bool IsEagerPropertyFetchEnabled(int i)
		{
			bool[] array = EntityEagerPropertyFetches;
			return array != null && array[i];
		}

		/// <summary>
		/// Hydrate the state of an object from the SQL <c>DbDataReader</c>, into
		/// an array of "hydrated" values (do not resolve associations yet),
		/// and pass the hydrated state to the session.
		/// </summary>
		private void LoadFromResultSet(DbDataReader rs, int i, object obj, string instanceClass, EntityKey key,
									   string rowIdAlias, LockMode lockMode, ILoadable rootPersister,
									   ISessionImplementor session)
		{
			object id = key.Identifier;

			// Get the persister for the _subclass_
			ILoadable persister = (ILoadable)Factory.GetEntityPersister(instanceClass);

			if (Log.IsDebugEnabled)
			{
				Log.Debug("Initializing object from DataReader: " + MessageHelper.InfoString(persister, id));
			}

			bool eagerPropertyFetch = IsEagerPropertyFetchEnabled(i);

			// add temp entry so that the next step is circular-reference
			// safe - only needed because some types don't take proper
			// advantage of two-phase-load (esp. components)
			TwoPhaseLoad.AddUninitializedEntity(key, obj, persister, lockMode, !eagerPropertyFetch, session);

			// This is not very nice (and quite slow):
			string[][] cols = persister == rootPersister
								? EntityAliases[i].SuffixedPropertyAliases
								: EntityAliases[i].GetSuffixedPropertyAliases(persister);

			object[] values = persister.Hydrate(rs, id, obj, rootPersister, cols, eagerPropertyFetch, session);

			object rowId = persister.HasRowId ? rs[rowIdAlias] : null;

			IAssociationType[] ownerAssociationTypes = OwnerAssociationTypes;
			if (ownerAssociationTypes != null && ownerAssociationTypes[i] != null)
			{
				string ukName = ownerAssociationTypes[i].RHSUniqueKeyPropertyName;
				if (ukName != null)
				{
					int index = ((IUniqueKeyLoadable)persister).GetPropertyIndex(ukName);
					IType type = persister.PropertyTypes[index];

					// polymorphism not really handled completely correctly,
					// perhaps...well, actually its ok, assuming that the
					// entity name used in the lookup is the same as the
					// the one used here, which it will be

					EntityUniqueKey euk =
						new EntityUniqueKey(rootPersister.EntityName, ukName, type.SemiResolve(values[index], session, obj), type,
											session.EntityMode, session.Factory);
					session.PersistenceContext.AddEntity(euk, obj);
				}
			}

			TwoPhaseLoad.PostHydrate(persister, id, values, rowId, obj, lockMode, !eagerPropertyFetch, session);
		}

		/// <summary>
		/// Determine the concrete class of an instance for the <c>DbDataReader</c>
		/// </summary>
		private string GetInstanceClass(DbDataReader rs, int i, ILoadable persister, object id, ISessionImplementor session)
		{
			if (persister.HasSubclasses)
			{
				// code to handle subclasses of topClass
				object discriminatorValue =
					persister.DiscriminatorType.NullSafeGet(rs, EntityAliases[i].SuffixedDiscriminatorAlias, session, null);

				string result = persister.GetSubclassForDiscriminatorValue(discriminatorValue);

				if (result == null)
				{
					// woops we got an instance of another class hierarchy branch.
					throw new WrongClassException(string.Format("Discriminator was: '{0}'", discriminatorValue), id,
												  persister.EntityName);
				}

				return result;
			}
			return persister.EntityName;
		}

		/// <summary>
		/// Advance the cursor to the first required row of the <c>DbDataReader</c>
		/// </summary>
		internal static void Advance(DbDataReader rs, RowSelection selection)
		{
			int firstRow = GetFirstRow(selection);

			if (firstRow != 0)
			{
				// DataReaders are forward-only, readonly, so we have to step through
				for (int i = 0; i < firstRow; i++)
				{
					rs.Read();
				}
			}
		}

		internal static bool HasMaxRows(RowSelection selection)
		{
			// it used to be selection.MaxRows != null -> since an Int32 will always
			// have a value I'll compare it to the static field NoValue used to initialize 
			// max rows to nothing
			return selection != null && selection.MaxRows != RowSelection.NoValue;
		}

		private static bool HasOffset(RowSelection selection)
		{
			return selection != null && selection.FirstRow != RowSelection.NoValue;
		}

		internal static int GetFirstRow(RowSelection selection)
		{
			if (selection == null || !selection.DefinesLimits)
			{
				return 0;
			}
			return selection.FirstRow > 0 ? selection.FirstRow : 0;
		}

		/// <summary>
		/// Should we pre-process the SQL string, adding a dialect-specific
		/// LIMIT clause.
		/// </summary>
		/// <param name="selection"></param>
		/// <param name="dialect"></param>
		/// <returns></returns>
		internal bool UseLimit(RowSelection selection, Dialect.Dialect dialect)
		{
			return (_canUseLimits ?? true)
				&& dialect.SupportsLimit
				&& (HasMaxRows(selection) || HasOffset(selection));
		}

		/// <summary>
		/// Performs dialect-specific manipulations on the offset value before returning it.
		/// This method is applicable for use in limit statements only.
		/// </summary>
		internal static int? GetOffsetUsingDialect(RowSelection selection, Dialect.Dialect dialect)
		{
			int firstRow = GetFirstRow(selection);
			if (firstRow == 0)
				return null;
			return dialect.GetOffsetValue(firstRow);
		}

		/// <summary>
		/// Performs dialect-specific manipulations on the limit value before returning it.
		/// This method is applicable for use in limit statements only.
		/// </summary>
		internal static int? GetLimitUsingDialect(RowSelection selection, Dialect.Dialect dialect)
		{
			if (selection == null || selection.MaxRows == RowSelection.NoValue)
				return null;
			return dialect.GetLimitValue(GetFirstRow(selection), selection.MaxRows);
		}

		/// <summary>
		/// Obtain an <c>DbCommand</c> with all parameters pre-bound. Bind positional parameters,
		/// named parameters, and limit parameters.
		/// </summary>
		/// <remarks>
		/// Creates an DbCommand object and populates it with the values necessary to execute it against the 
		/// database to Load an Entity.
		/// </remarks>
		/// <param name="queryParameters">The <see cref="QueryParameters"/> to use for the DbCommand.</param>
		/// <param name="scroll">TODO: find out where this is used...</param>
		/// <param name="session">The SessionImpl this Command is being prepared in.</param>
		/// <returns>A CommandWrapper wrapping an DbCommand that is ready to be executed.</returns>
		protected internal virtual DbCommand PrepareQueryCommand(QueryParameters queryParameters, bool scroll, ISessionImplementor session)
		{
			ISqlCommand sqlCommand = CreateSqlCommand(queryParameters, session);
			SqlString sqlString = sqlCommand.Query;

			sqlCommand.ResetParametersIndexesForTheCommand(0);
			DbCommand command = session.Batcher.PrepareQueryCommand(CommandType.Text, sqlString, sqlCommand.ParameterTypes);

			try
			{
				RowSelection selection = queryParameters.RowSelection;
				if (selection != null && selection.Timeout != RowSelection.NoValue)
				{
					command.CommandTimeout = selection.Timeout;
				}

				sqlCommand.Bind(command, session);

				IDriver driver = _factory.ConnectionProvider.Driver;
				driver.RemoveUnusedCommandParameters(command, sqlString);
				driver.ExpandQueryParameters(command, sqlString);
			}
			catch (HibernateException)
			{
				session.Batcher.CloseCommand(command, null);
				throw;
			}
			catch (Exception sqle)
			{
				session.Batcher.CloseCommand(command, null);
				ADOExceptionReporter.LogExceptions(sqle);
				throw;
			}
			return command;
		}

		/// <summary> 
		/// Some dialect-specific LIMIT clauses require the maximium last row number
		/// (aka, first_row_number + total_row_count), while others require the maximum
		/// returned row count (the total maximum number of rows to return). 
		/// </summary>
		/// <param name="selection">The selection criteria </param>
		/// <param name="dialect">The dialect </param>
		/// <returns> The appropriate value to bind into the limit clause. </returns>
		internal static int GetMaxOrLimit(Dialect.Dialect dialect, RowSelection selection)
		{
			int firstRow = GetFirstRow(selection);
			int rowCount = selection.MaxRows;

			if (rowCount == RowSelection.NoValue)
				return int.MaxValue;

			return dialect.GetLimitValue(firstRow, rowCount);
		}

		/// <summary>
		/// Fetch a <c>DbCommand</c>, call <c>SetMaxRows</c> and then execute it,
		/// advance to the first result and return an SQL <c>DbDataReader</c>
		/// </summary>
		/// <param name="st">The <see cref="DbCommand" /> to execute.</param>
		/// <param name="selection">The <see cref="RowSelection"/> to apply to the <see cref="DbCommand"/> and <see cref="DbDataReader"/>.</param>
		/// <param name="autoDiscoverTypes">true if result types need to be auto-discovered by the loader; false otherwise.</param>
		/// <param name="session">The <see cref="ISession" /> to load in.</param>
		/// <param name="callable"></param>
		/// <returns>An DbDataReader advanced to the first record in RowSelection.</returns>
		protected DbDataReader GetResultSet(DbCommand st, bool autoDiscoverTypes, bool callable, RowSelection selection, ISessionImplementor session)
		{
			DbDataReader rs = null;
			try
			{
				BeforeGetResultSet(st);
				rs = session.Batcher.ExecuteReader(st);
				return EndGetResultSet(session, selection, autoDiscoverTypes, rs);
			}
			catch (Exception sqle)
			{
				HandleExceptionsGetResultSet(st, session, sqle, rs);
				throw;
			}
		}

		/// <summary>
		/// Fetch a <c>IDbCommand</c>, call <c>SetMaxRows</c> and then execute it,
		/// advance to the first result and return an SQL <c>IDataReader</c> Asynchronously
		/// </summary>
		/// <param name="st">The <see cref="IDbCommand" /> to execute.</param>
		/// <param name="selection">The <see cref="RowSelection"/> to apply to the <see cref="IDbCommand"/> and <see cref="IDataReader"/>.</param>
		/// <param name="autoDiscoverTypes">true if result types need to be auto-discovered by the loader; false otherwise.</param>
		/// <param name="session">The <see cref="ISession" /> to load in.</param>
		/// <param name="callable"></param>
		/// <param name="cancellationToken">Token to cancel the request.</param>
		/// <returns>An IDataReader advanced to the first record in RowSelection.</returns>
		protected Task<DbDataReader> GetResultSetAsync(DbCommand st, CancellationToken cancellationToken, bool autoDiscoverTypes, bool callable, RowSelection selection, ISessionImplementor session)
		{
			BeforeGetResultSet(st);
			return session.Batcher
				.ExecuteReaderAsync(st, cancellationToken)
				.ContinueWith(task =>
				{
					DbDataReader rs = null;
					try
					{
						rs = task.Result;
						return EndGetResultSet(session, selection, autoDiscoverTypes, rs);
					}
					catch (AggregateException aggregateException)
					{
						foreach (var exception in aggregateException.Flatten().InnerExceptions)
						{
							HandleExceptionsGetResultSet(st, session, exception, rs);							
						}
						throw;						
					}
					catch (Exception sqle)
					{
						HandleExceptionsGetResultSet(st, session, sqle, rs);
						throw;
					}
				});
		}

		private static void BeforeGetResultSet(DbCommand st)
		{
			Log.Info(st.CommandText);
			// TODO NH: Callable
		}

		private DbDataReader EndGetResultSet(ISessionImplementor session, RowSelection selection, bool autoDiscoverTypes, DbDataReader reader)
		{
			//NH: this is checked outside the WrapResultSet because we
			// want to avoid the syncronization overhead in the vast majority
			// of cases where IsWrapResultSetsEnabled is set to false
			if (session.Factory.Settings.IsWrapResultSetsEnabled)
				reader = WrapResultSet(reader);

			Dialect.Dialect dialect = session.Factory.Dialect;
			if (!dialect.SupportsLimitOffset || !UseLimit(selection, dialect))
			{
				Advance(reader, selection);
			}

			if (autoDiscoverTypes)
			{
				AutoDiscoverTypes(reader);
			}
			return reader;
		}

		private static void HandleExceptionsGetResultSet(DbCommand st, ISessionImplementor session, Exception sqle, DbDataReader reader)
		{
			ADOExceptionReporter.LogExceptions(sqle);
			session.Batcher.CloseCommand(st, reader);
		}


		protected virtual void AutoDiscoverTypes(DbDataReader rs)
		{
			throw new AssertionFailure("Auto discover types not supported in this loader");
		}

		[MethodImpl(MethodImplOptions.Synchronized)]
		private DbDataReader WrapResultSet(DbDataReader rs)
		{
			// synchronized to avoid multi-thread access issues; defined as method synch to avoid
			// potential deadlock issues due to nature of code.
			try
			{
				Log.Debug("Wrapping result set [" + rs + "]");
				return new ResultSetWrapper(rs, RetreiveColumnNameToIndexCache(rs));
			}
			catch (Exception e)
			{
				Log.Info("Error wrapping result set", e);
				return rs;
			}
		}

		private ColumnNameCache RetreiveColumnNameToIndexCache(DbDataReader rs)
		{
			if (_columnNameCache == null)
			{
				Log.Debug("Building columnName->columnIndex cache");
				_columnNameCache = new ColumnNameCache(rs.GetSchemaTable().Rows.Count);
			}

			return _columnNameCache;
		}

		/// <summary>
		/// Called by subclasses that load entities
		/// </summary>
		protected IList LoadEntity(ISessionImplementor session, object id, IType identifierType, object optionalObject,
								   string optionalEntityName, object optionalIdentifier, IEntityPersister persister)
		{
			if (Log.IsDebugEnabled)
			{
				Log.Debug("loading entity: " + MessageHelper.InfoString(persister, id, identifierType, Factory));
			}

			IList result;

			try
			{
				QueryParameters qp =
					new QueryParameters(new IType[] { identifierType }, new object[] { id }, optionalObject, optionalEntityName,
										optionalIdentifier);
				result = DoQueryAndInitializeNonLazyCollections(session, qp, false);
			}
			catch (HibernateException)
			{
				throw;
			}
			catch (Exception sqle)
			{
				ILoadable[] persisters = EntityPersisters;
				throw ADOExceptionHelper.Convert(Factory.SQLExceptionConverter, sqle,
												 "could not load an entity: "
												 +
												 MessageHelper.InfoString(persisters[persisters.Length - 1], id, identifierType,
																		  Factory), SqlString);
			}

			Log.Debug("done entity load");

			return result;
		}

		protected IList LoadEntity(ISessionImplementor session, object key, object index, IType keyType, IType indexType,
								   IEntityPersister persister)
		{
			Log.Debug("loading collection element by index");

			IList result;
			try
			{
				result =
					DoQueryAndInitializeNonLazyCollections(session,
														   new QueryParameters(new IType[] { keyType, indexType },
																			   new object[] { key, index }), false);
			}
			catch (Exception sqle)
			{
				throw ADOExceptionHelper.Convert(_factory.SQLExceptionConverter, sqle, "could not collection element by index",
												 SqlString);
			}

			Log.Debug("done entity load");

			return result;
		}

		/// <summary>
		/// Called by subclasses that batch load entities
		/// </summary>
		protected internal IList LoadEntityBatch(ISessionImplementor session, object[] ids, IType idType,
												 object optionalObject, string optionalEntityName, object optionalId,
												 IEntityPersister persister)
		{
			if (Log.IsDebugEnabled)
			{
				Log.Debug("batch loading entity: " + MessageHelper.InfoString(persister, ids, Factory));
			}

			IType[] types = new IType[ids.Length];
			ArrayHelper.Fill(types, idType);
			IList result;
			try
			{
				result =
					DoQueryAndInitializeNonLazyCollections(session,
														   new QueryParameters(types, ids, optionalObject, optionalEntityName,
																			   optionalId), false);
			}
			catch (HibernateException)
			{
				throw;
			}
			catch (Exception sqle)
			{
				throw ADOExceptionHelper.Convert(Factory.SQLExceptionConverter, sqle,
												 "could not load an entity batch: "
												 + MessageHelper.InfoString(persister, ids, Factory), SqlString);
				// NH: Hibernate3 passes EntityPersisters[0] instead of persister, I think it's wrong.
			}

			Log.Debug("done entity batch load");
			return result;
		}

		/// <summary>
		/// Called by subclasses that load collections
		/// </summary>
		public void LoadCollection(ISessionImplementor session, object id, IType type)
		{
			if (Log.IsDebugEnabled)
			{
				Log.Debug("loading collection: " + MessageHelper.InfoString(CollectionPersisters[0], id));
			}

			object[] ids = new object[] { id };
			try
			{
				DoQueryAndInitializeNonLazyCollections(session, new QueryParameters(new IType[] { type }, ids, ids), true);
			}
			catch (HibernateException)
			{
				// Do not call Convert on HibernateExceptions
				throw;
			}
			catch (Exception sqle)
			{
				throw ADOExceptionHelper.Convert(Factory.SQLExceptionConverter, sqle,
												 "could not initialize a collection: "
												 + MessageHelper.InfoString(CollectionPersisters[0], id), SqlString);
			}

			Log.Debug("done loading collection");
		}

		/// <summary>
		/// Called by wrappers that batch initialize collections
		/// </summary>
		public void LoadCollectionBatch(ISessionImplementor session, object[] ids, IType type)
		{
			if (Log.IsDebugEnabled)
			{
				Log.Debug("batch loading collection: " + MessageHelper.InfoString(CollectionPersisters[0], ids));
			}

			IType[] idTypes = new IType[ids.Length];
			ArrayHelper.Fill(idTypes, type);
			try
			{
				DoQueryAndInitializeNonLazyCollections(session, new QueryParameters(idTypes, ids, ids), true);
			}
			catch (HibernateException)
			{
				// Do not call Convert on HibernateExceptions
				throw;
			}
			catch (Exception sqle)
			{
				throw ADOExceptionHelper.Convert(Factory.SQLExceptionConverter, sqle,
												 "could not initialize a collection batch: "
												 + MessageHelper.InfoString(CollectionPersisters[0], ids), SqlString);
			}

			Log.Debug("done batch load");
		}

		/// <summary>
		/// Called by subclasses that batch initialize collections
		/// </summary>
		protected void LoadCollectionSubselect(ISessionImplementor session, object[] ids, object[] parameterValues,
											   IType[] parameterTypes, IDictionary<string, TypedValue> namedParameters,
											   IType type)
		{
			try
			{
				DoQueryAndInitializeNonLazyCollections(session,
													   new QueryParameters(parameterTypes, parameterValues, namedParameters, ids),
													   true);
			}
			catch (HibernateException)
			{
				// Do not call Convert on HibernateExceptions
				throw;
			}
			catch (Exception sqle)
			{
				throw ADOExceptionHelper.Convert(Factory.SQLExceptionConverter, sqle,
												 "could not load collection by subselect: "
												 + MessageHelper.InfoString(CollectionPersisters[0], ids), SqlString,
												 parameterValues, namedParameters);
			}
		}

		/// <summary>
		/// Return the query results, using the query cache, called
		/// by subclasses that implement cacheable queries
		/// </summary>
		/// <param name="session"></param>
		/// <param name="queryParameters"></param>
		/// <param name="querySpaces"></param>
		/// <param name="resultTypes"></param>
		/// <returns></returns>
		protected IList List(ISessionImplementor session, QueryParameters queryParameters, ISet<string> querySpaces, IType[] resultTypes)
		{
			bool cacheable = _factory.Settings.IsQueryCacheEnabled && queryParameters.Cacheable;

			if (cacheable)
			{
				return ListUsingQueryCache(session, queryParameters, querySpaces, resultTypes);
			}
			return ListIgnoreQueryCache(session, queryParameters);
		}

		private IList ListIgnoreQueryCache(ISessionImplementor session, QueryParameters queryParameters)
		{
			return GetResultList(DoList(session, queryParameters), queryParameters.ResultTransformer);
		}

		private IList ListUsingQueryCache(ISessionImplementor session, QueryParameters queryParameters, ISet<string> querySpaces, IType[] resultTypes)
		{
			var beforeListUsingQueryCacheParams = BeforeListUsingQueryCache(
				new BeforeListUsingQueryCacheParams(session, queryParameters, querySpaces, resultTypes));

			if (beforeListUsingQueryCacheParams.Result == null)
			{
				beforeListUsingQueryCacheParams.Result = DoList(session, queryParameters);
				PutResultInQueryCache(
					session,
					queryParameters,
					resultTypes,
					beforeListUsingQueryCacheParams.QueryCache,
					beforeListUsingQueryCacheParams.Key,
					beforeListUsingQueryCacheParams.Result);
			}
			return GetResultList(beforeListUsingQueryCacheParams.Result, queryParameters.ResultTransformer);
		}

		/// <summary>
		/// Return the query results asynchronously, using the query cache, called
		/// by subclasses that implement cacheable queries
		/// </summary>
		/// <param name="session"></param>
		/// <param name="queryParameters"></param>
		/// <param name="querySpaces"></param>
		/// <param name="resultTypes"></param>
		/// <returns></returns>
		protected Task<IList> ListAsync(ISessionImplementor session, CancellationToken cancellationToken, QueryParameters queryParameters, ISet<string> querySpaces, IType[] resultTypes)
		{
			bool cacheable = _factory.Settings.IsQueryCacheEnabled && queryParameters.Cacheable;

			if (cacheable)
			{
				return ListUsingQueryCacheAsync(session, cancellationToken, queryParameters, querySpaces, resultTypes);
			}
			return ListIgnoreQueryCacheAsync(session, cancellationToken, queryParameters);
		}

		private Task<IList> ListIgnoreQueryCacheAsync(ISessionImplementor session, CancellationToken cancellationToken, QueryParameters queryParameters)
		{
			return DoListAsync(session, cancellationToken, queryParameters)
				.ContinueWith(task => GetResultList(task.Result, queryParameters.ResultTransformer), cancellationToken);
		}

		private Task<IList> ListUsingQueryCacheAsync(ISessionImplementor session, CancellationToken cancellationToken, QueryParameters queryParameters, ISet<string> querySpaces, IType[] resultTypes)
		{
			var beforeListUsingQueryCacheParams = BeforeListUsingQueryCache(
				new BeforeListUsingQueryCacheParams(session, queryParameters, querySpaces, resultTypes));

			if (beforeListUsingQueryCacheParams.Result == null)
			{
				return DoListAsync(session, cancellationToken, queryParameters)
					.ContinueWith(task =>
					{
						beforeListUsingQueryCacheParams.Result = task.Result;
						PutResultInQueryCache(
							session,
							queryParameters,
							resultTypes,
							beforeListUsingQueryCacheParams.QueryCache,
							beforeListUsingQueryCacheParams.Key,
							beforeListUsingQueryCacheParams.Result);
						return GetResultList(beforeListUsingQueryCacheParams.Result, queryParameters.ResultTransformer);
					}, cancellationToken);
			}
			var taskCompletionSource = new TaskCompletionSource<IList>();
			taskCompletionSource.SetResult(beforeListUsingQueryCacheParams.Result);
			return taskCompletionSource.Task;
		}

		private BeforeListUsingQueryCacheParams BeforeListUsingQueryCache(BeforeListUsingQueryCacheParams beforeListUsingQueryCacheParams)
		{
			beforeListUsingQueryCacheParams.QueryCache = _factory.GetQueryCache(beforeListUsingQueryCacheParams.QueryParameters.CacheRegion);
			ISet<FilterKey> filterKeys = FilterKey.CreateFilterKeys(
				beforeListUsingQueryCacheParams.Session.EnabledFilters,
				beforeListUsingQueryCacheParams.Session.EntityMode);

			beforeListUsingQueryCacheParams.Key = new QueryKey(Factory, SqlString, beforeListUsingQueryCacheParams.QueryParameters, filterKeys);
			beforeListUsingQueryCacheParams.Result = GetResultFromQueryCache(
				beforeListUsingQueryCacheParams.Session,
				beforeListUsingQueryCacheParams.QueryParameters,
				beforeListUsingQueryCacheParams.QuerySpaces,
				beforeListUsingQueryCacheParams.ResultTypes,
				beforeListUsingQueryCacheParams.QueryCache,
				beforeListUsingQueryCacheParams.Key);

			return beforeListUsingQueryCacheParams;
		}

		private IList GetResultFromQueryCache(ISessionImplementor session, QueryParameters queryParameters,
											  ISet<string> querySpaces, IType[] resultTypes, IQueryCache queryCache,
											  QueryKey key)
		{
			IList result = null;

			if ((!queryParameters.ForceCacheRefresh) && (session.CacheMode & CacheMode.Get) == CacheMode.Get)
			{
				IPersistenceContext persistenceContext = session.PersistenceContext;

				bool defaultReadOnlyOrig = persistenceContext.DefaultReadOnly;

				if (queryParameters.IsReadOnlyInitialized)
					persistenceContext.DefaultReadOnly = queryParameters.ReadOnly;
				else
					queryParameters.ReadOnly = persistenceContext.DefaultReadOnly;

				try
				{
					result = queryCache.Get(key, resultTypes, queryParameters.NaturalKeyLookup, querySpaces, session);
					if (_factory.Statistics.IsStatisticsEnabled)
					{
						if (result == null)
						{
							_factory.StatisticsImplementor.QueryCacheMiss(QueryIdentifier, queryCache.RegionName);
						}
						else
						{
							_factory.StatisticsImplementor.QueryCacheHit(QueryIdentifier, queryCache.RegionName);
						}
					}
				}
				finally
				{
					persistenceContext.DefaultReadOnly = defaultReadOnlyOrig;
				}
			}
			return result;
		}

		private void PutResultInQueryCache(ISessionImplementor session, QueryParameters queryParameters, IType[] resultTypes,
										   IQueryCache queryCache, QueryKey key, IList result)
		{
			if ((session.CacheMode & CacheMode.Put) == CacheMode.Put)
			{
				bool put = queryCache.Put(key, resultTypes, result, queryParameters.NaturalKeyLookup, session);
				if (put && _factory.Statistics.IsStatisticsEnabled)
				{
					_factory.StatisticsImplementor.QueryCachePut(QueryIdentifier, queryCache.RegionName);
				}
			}
		}

		/// <summary>
		/// Actually execute a query, ignoring the query cache
		/// </summary>
		/// <param name="session"></param>
		/// <param name="queryParameters"></param>
		/// <returns></returns>
		protected IList DoList(ISessionImplementor session, QueryParameters queryParameters)
		{
			bool statsEnabled = Factory.Statistics.IsStatisticsEnabled;
			var stopWatch = StartStopWatchQueryExecuted(statsEnabled);

			IList result;
			try
			{
				result = DoQueryAndInitializeNonLazyCollections(session, queryParameters, true);
			}
			catch (HibernateException)
			{
				// Do not call Convert on HibernateExceptions
				throw;
			}
			catch (Exception sqle)
			{
				throw ADOExceptionHelper.Convert(Factory.SQLExceptionConverter, sqle, "could not execute query", SqlString,
												 queryParameters.PositionalParameterValues, queryParameters.NamedParameters);
			}
			if (statsEnabled)
			{
				stopWatch.Stop();
				Factory.StatisticsImplementor.QueryExecuted(QueryIdentifier, result.Count, stopWatch.Elapsed);
			}
			return result;
		}

		/// <summary>
		/// Actually execute a query, ignoring the query cache
		/// </summary>
		/// <param name="session"></param>
		/// <param name="queryParameters"></param>
		/// <returns></returns>
		protected Task<IList> DoListAsync(ISessionImplementor session, CancellationToken cancellationToken, QueryParameters queryParameters)
		{
			bool statsEnabled = Factory.Statistics.IsStatisticsEnabled;
			var stopWatch = StartStopWatchQueryExecuted(statsEnabled);

			return DoQueryAndInitializeNonLazyCollectionsAsync(session, cancellationToken, queryParameters, true)
				.ContinueWith(task =>
					EndDoList(statsEnabled, stopWatch, task, queryParameters));
		}

		private static Stopwatch StartStopWatchQueryExecuted(bool statsEnabled)
		{
			var stopWatch = new Stopwatch();
			if (statsEnabled)
			{
				stopWatch.Start();
			}
			return stopWatch;
		}

		private IList EndDoList(bool statsEnabled, Stopwatch stopWatch, Task<IList> task, QueryParameters queryParameters)
		{
			IList result = null;
			try
			{
				result = task.Result;
			}
			catch (AggregateException aggregateException)
			{
				foreach (var exception in aggregateException.Flatten().InnerExceptions)
				{
					if (exception is HibernateException) // This we know how to handle.
					{
						throw;
					}
					throw ADOExceptionHelper.Convert(Factory.SQLExceptionConverter, exception, "could not execute query",
							SqlString,
							queryParameters.PositionalParameterValues, queryParameters.NamedParameters);
				}
			}

			if (statsEnabled)
			{
				stopWatch.Stop();
				Factory.StatisticsImplementor.QueryExecuted(QueryIdentifier, result.Count, stopWatch.Elapsed);
			}

			return result;
		}

		/// <summary>
		/// Calculate and cache select-clause suffixes. Must be
		/// called by subclasses after instantiation.
		/// </summary>
		protected virtual void PostInstantiate() { }

		/// <summary> 
		/// Identifies the query for statistics reporting, if null,
		/// no statistics will be reported
		/// </summary>
		public virtual string QueryIdentifier
		{
			get { return null; }
		}

		public override string ToString()
		{
			return GetType().FullName + '(' + SqlString + ')';
		}

		#region NHibernate specific

		public virtual ISqlCommand CreateSqlCommand(QueryParameters queryParameters, ISessionImplementor session)
		{
			// A distinct-copy of parameter specifications collected during query construction
			var parameterSpecs = new HashSet<IParameterSpecification>(GetParameterSpecifications());
			SqlString sqlString = SqlString.Copy();

			// dynamic-filter parameters: during the createion of the SqlString of allLoader implementation, filters can be added as SQL_TOKEN/string for this reason we have to re-parse the SQL.
			sqlString = ExpandDynamicFilterParameters(sqlString, parameterSpecs, session);
			AdjustQueryParametersForSubSelectFetching(sqlString, parameterSpecs, queryParameters);

			// Add limits
			sqlString = AddLimitsParametersIfNeeded(sqlString, parameterSpecs, queryParameters, session);

			// The PreprocessSQL method can modify the SqlString but should never add parameters (or we have to override it)
			sqlString = PreprocessSQL(sqlString, queryParameters, session.Factory.Dialect);

			// After the last modification to the SqlString we can collect all parameters types (there are cases where we can't infer the type during the creation of the query)
			ResetEffectiveExpectedType(parameterSpecs, queryParameters);

			return new SqlCommandImpl(sqlString, parameterSpecs, queryParameters, session.Factory);
		}

		protected virtual void ResetEffectiveExpectedType(IEnumerable<IParameterSpecification> parameterSpecs, QueryParameters queryParameters)
		{
			// Have to be overridden just by those loaders that can't infer the type during the parse process
		}

		protected abstract IEnumerable<IParameterSpecification> GetParameterSpecifications();

		protected void AdjustQueryParametersForSubSelectFetching(SqlString filteredSqlString, IEnumerable<IParameterSpecification> parameterSpecsWithFilters, QueryParameters queryParameters)
		{
			queryParameters.ProcessedSql = filteredSqlString;
			queryParameters.ProcessedSqlParameters = parameterSpecsWithFilters.ToList();
			if (queryParameters.RowSelection != null)
			{
				queryParameters.ProcessedRowSelection = new RowSelection { FirstRow = queryParameters.RowSelection.FirstRow, MaxRows = queryParameters.RowSelection.MaxRows };
			}
		}

		protected SqlString ExpandDynamicFilterParameters(SqlString sqlString, ICollection<IParameterSpecification> parameterSpecs, ISessionImplementor session)
		{
			var enabledFilters = session.EnabledFilters;
			if (enabledFilters.Count == 0 || sqlString.ToString().IndexOf(ParserHelper.HqlVariablePrefix) < 0)
			{
				return sqlString;
			}

			Dialect.Dialect dialect = session.Factory.Dialect;
			string symbols = ParserHelper.HqlSeparators + dialect.OpenQuote + dialect.CloseQuote;

			var result = new SqlStringBuilder();
			foreach (var sqlPart in sqlString)
			{
				var parameter = sqlPart as Parameter;
				if (parameter != null)
				{
					result.Add(parameter);
					continue;
				}

				var sqlFragment = sqlPart.ToString();
				var tokens = new StringTokenizer(sqlFragment, symbols, true);

				foreach (string token in tokens)
				{
					if (token.StartsWith(ParserHelper.HqlVariablePrefix))
					{
						string filterParameterName = token.Substring(1);
						string[] parts = StringHelper.ParseFilterParameterName(filterParameterName);
						string filterName = parts[0];
						string parameterName = parts[1];
						var filter = (FilterImpl)enabledFilters[filterName];

						object value = filter.GetParameter(parameterName);
						IType type = filter.FilterDefinition.GetParameterType(parameterName);
						int parameterColumnSpan = type.GetColumnSpan(session.Factory);
						var collectionValue = value as ICollection;
						int? collectionSpan = null;

						// Add query chunk
						string typeBindFragment = string.Join(", ", Enumerable.Repeat("?", parameterColumnSpan).ToArray());
						string bindFragment;
						if (collectionValue != null && !type.ReturnedClass.IsArray)
						{
							collectionSpan = collectionValue.Count;
							bindFragment = string.Join(", ", Enumerable.Repeat(typeBindFragment, collectionValue.Count).ToArray());
						}
						else
						{
							bindFragment = typeBindFragment;
						}

						// dynamic-filter parameter tracking
						var filterParameterFragment = SqlString.Parse(bindFragment);
						var dynamicFilterParameterSpecification = new DynamicFilterParameterSpecification(filterName, parameterName, type, collectionSpan);
						var parameters = filterParameterFragment.GetParameters().ToArray();
						var sqlParameterPos = 0;
						var paramTrackers = dynamicFilterParameterSpecification.GetIdsForBackTrack(session.Factory);
						foreach (var paramTracker in paramTrackers)
						{
							parameters[sqlParameterPos++].BackTrack = paramTracker;
						}

						parameterSpecs.Add(dynamicFilterParameterSpecification);
						result.Add(filterParameterFragment);
					}
					else
					{
						result.Add(token);
					}
				}
			}
			return result.ToSqlString();
		}

		protected SqlString AddLimitsParametersIfNeeded(SqlString sqlString, ICollection<IParameterSpecification> parameterSpecs, QueryParameters queryParameters, ISessionImplementor session)
		{
			var sessionFactory = session.Factory;
			Dialect.Dialect dialect = sessionFactory.Dialect;

			RowSelection selection = queryParameters.RowSelection;
			if (UseLimit(selection, dialect))
			{
				bool hasFirstRow = GetFirstRow(selection) > 0;
				bool useOffset = hasFirstRow && dialect.SupportsLimitOffset;
				int max = GetMaxOrLimit(dialect, selection);
				int? skip = useOffset ? (int?)dialect.GetOffsetValue(GetFirstRow(selection)) : null;
				int? take = max != int.MaxValue ? (int?)max : null;

				Parameter skipSqlParameter = null;
				Parameter takeSqlParameter = null;
				if (skip.HasValue)
				{
					var skipParameter = new QuerySkipParameterSpecification();
					skipSqlParameter = Parameter.Placeholder;
					skipSqlParameter.BackTrack = skipParameter.GetIdsForBackTrack(sessionFactory).First();
					parameterSpecs.Add(skipParameter);
				}
				if (take.HasValue)
				{
					var takeParameter = new QueryTakeParameterSpecification();
					takeSqlParameter = Parameter.Placeholder;
					takeSqlParameter.BackTrack = takeParameter.GetIdsForBackTrack(sessionFactory).First();
					parameterSpecs.Add(takeParameter);
				}
				// The dialect can move the given parameters where he need, what it can't do is generates new parameters loosing the BackTrack.
				SqlString result;
				if (TryGetLimitString(dialect, sqlString, skip, take, skipSqlParameter, takeSqlParameter, out result)) return result;
			}
			return sqlString;
		}

		protected bool TryGetLimitString(Dialect.Dialect dialect, SqlString queryString, int? offset, int? limit, Parameter offsetParameter, Parameter limitParameter, out SqlString result)
		{
			result = dialect.GetLimitString(queryString, offset, limit, offsetParameter, limitParameter);
			if (result != null) return true;

			_canUseLimits = false;
			return false;
		}

		#endregion

		private class BeforeListUsingQueryCacheParams
		{
			public ISessionImplementor Session { get; set; }
			public QueryParameters QueryParameters { get; set; }
			public ISet<string> QuerySpaces { get; set; }
			public IType[] ResultTypes { get; set; }
			public IQueryCache QueryCache { get; set; }
			public QueryKey Key { get; set; }
			public IList Result { get; set; }

			public BeforeListUsingQueryCacheParams(ISessionImplementor session, QueryParameters queryParameters, ISet<string> querySpaces, IType[] resultTypes)
			{
				Session = session;
				QueryParameters = queryParameters;
				QuerySpaces = querySpaces;
				ResultTypes = resultTypes;
			}
		}

		private class BeforeDoQueryAndInitializeNonLazyCollectionsParams
		{
			public ISessionImplementor Session { get; set; }
			public QueryParameters QueryParameters { get; set; }
			public IPersistenceContext PersistenceContext { get; set; }
			public bool DefaultReadOnlyOrig { get; set; }

			public BeforeDoQueryAndInitializeNonLazyCollectionsParams(ISessionImplementor session, QueryParameters queryParameters)
			{
				Session = session;
				QueryParameters = queryParameters;
			}
		}

		private class BeforeDoQueryParams
		{
			public ISessionImplementor Session { get; set; }
			public QueryParameters QueryParameters { get; set; }
			public RowSelection Selection { get; set; }
			public int MaxRows { get; set; }
			public int EntitySpan { get; set; }
			public List<object> HydratedObjects { get; set; }
			public DbCommand DbCommand { get; set; }

			public BeforeDoQueryParams(ISessionImplementor session, QueryParameters queryParameters)
			{
				Session = session;
				QueryParameters = queryParameters;
			}
		}
	}
}