use rocket::figment::{Figment, providers::Serialized};
use rocket_db_pools::{Database, Pool};
use rocket::request::{FromRequest, Request, Outcome};
use std::ops::{Deref, DerefMut};
use rocket::{Ignite, Rocket, Sentinel};
use rocket::http::Status;
use rocket::async_trait;

///Internal trait so the FromRequest implementation can match `ReadPool` databases
#[async_trait]
trait PoolRead: Pool{
    ///Gets a connection from the read pool if given else the main pool
    async fn get_read(&self) -> Result<Self::Connection, Self::Error>;
}

///A pool which supports separate read-write and read-only connections.
///Example:
///```rust
/// # #[cfg(feature = "sqlx_sqlite")] mod _inner {
/// # use rocket::get;
/// # type Pool = rocket_db_pools::sqlx::SqlitePool;
/// use rocket_db_pools::Database;
/// use rocket_read_db_pools::ReadPool;
///
/// #[derive(Database)]
/// #[database("db")]
/// struct Db(ReadPool<Pool>);
/// # }
///```
///```toml
///[default.databases.main]
///url = "postgresql://user@host.example/dbname"
///[default.databases.main.read]
///url = "postgresql://user@readreplica.example/dbname"
///max_connections = 10
///```
pub struct ReadPool<P>{
    main: P,
    read: Option<P>,
}
#[rocket::async_trait]
impl<P> Pool for ReadPool<P> where P: Pool
{
    type Error = P::Error;

    type Connection = P::Connection;

    async fn init(figment: &Figment) -> Result<Self, Self::Error> {
        let main_pool = P::init(figment).await?;
        if figment.contains("read"){
            let read_config = figment.focus("read")
                .join(Serialized::default("read.connect_timeout", 5));
            let read_pool = P::init(&read_config).await?;
            Ok(ReadPool{main: main_pool, read: Some(read_pool)})
        } else {
            Ok(ReadPool{main: main_pool, read: None})
        }
    }

    async fn get(&self) -> Result<Self::Connection, Self::Error> {
        self.main.get().await
    }

    async fn close(&self) {
        self.main.close().await;
        if let Some(ref read) = self.read {read.close().await;}
    }
}
#[async_trait]
impl<P> PoolRead for ReadPool<P> where P: Pool{
    async fn get_read(&self) -> Result<<P>::Connection, P::Error> {
        self.read.as_ref().unwrap_or(&self.main).get().await
    }
}

/// A request guard which retrieves a single connection to a [`Database`] using the read_url.
///
/// For a database type of `Db`, a request guard of `ReadConnection<Db>` retrieves a
/// single connection to `Db`.
pub struct ReadConnection<D: Database>(<D::Pool as Pool>::Connection);
impl<D: Database> ReadConnection<D> {
    ///Gets the internal connection value
    pub fn into_inner(self) -> <D::Pool as Pool>::Connection {
        self.0
    }
}
#[rocket::async_trait]
impl<'r, D: Database> FromRequest<'r> for ReadConnection<D> where D::Pool: PoolRead {
    type Error = Option<<D::Pool as Pool>::Error>;

    async fn from_request(req: &'r Request<'_>) -> Outcome<Self, Self::Error> {
        match D::fetch(req.rocket()) {
            Some(db) => match db.get_read().await {
                Ok(conn) => Outcome::Success(ReadConnection(conn)),
                Err(e) => Outcome::Error((Status::ServiceUnavailable, Some(e))),
            },
            None => Outcome::Error((Status::InternalServerError, None)),
        }
    }
}
impl<D: Database> Sentinel for ReadConnection<D> {
    fn abort(rocket: &Rocket<Ignite>) -> bool {
        D::fetch(rocket).is_none()
    }
}
impl<D: Database> Deref for ReadConnection<D> {
    type Target = <D::Pool as Pool>::Connection;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl<D: Database> DerefMut for ReadConnection<D> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// A request guard which retrieves a single connection to a [`Database`] using the main connection url.
/// Can be downgraded into a `ReadConnection`
///
/// For a database type of `Db`, a request guard of `RwConnection<Db>` retrieves a
/// single connection to `Db`.
pub struct RwConnection<D: Database>(ReadConnection<D>);
impl<D: Database> RwConnection<D> {
    ///Gets the internal connection value
    pub fn into_inner(self) -> <D::Pool as Pool>::Connection {
        self.0.0
    }
    ///Dowgrades this into a `ReadConnection`
    pub fn into_read_connection(self) -> ReadConnection<D>{
        self.0
    }
    ///Temporarily downgrades into a `ReadConnection`
    pub fn as_read_connection(&self) -> &ReadConnection<D>{
        &self.0
    }
    ///Temporarily downgrades into a `ReadConnection`
    pub fn as_read_connection_mut(&mut self) -> &mut ReadConnection<D>{
        &mut self.0
    }
}
#[rocket::async_trait]
impl<'r, D: Database> FromRequest<'r> for RwConnection<D> {
    type Error = Option<<D::Pool as Pool>::Error>;

    async fn from_request(req: &'r Request<'_>) -> Outcome<Self, Self::Error> {
        match D::fetch(req.rocket()) {
            Some(db) => match db.get().await {
                Ok(conn) => Outcome::Success(RwConnection(ReadConnection(conn))),
                Err(e) => Outcome::Error((Status::ServiceUnavailable, Some(e))),
            },
            None => Outcome::Error((Status::InternalServerError, None)),
        }
    }
}
impl<D: Database> Sentinel for RwConnection<D> {
    fn abort(rocket: &Rocket<Ignite>) -> bool {
        D::fetch(rocket).is_none()
    }
}
impl<D: Database> Deref for RwConnection<D> {
    type Target = <D::Pool as Pool>::Connection;

    fn deref(&self) -> &Self::Target {
        &self.0.0
    }
}
impl<D: Database> DerefMut for RwConnection<D> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0.0
    }
}
