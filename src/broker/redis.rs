//! Redis broker.
use super::{Broker, BrokerBuilder, DeliveryError, DeliveryStream, IncrementHandle};
use crate::{
    error::{BrokerError, ProtocolError},
    protocol::{self, Message, TryDeserializeMessage},
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::Stream;
use redis::{
    AsyncTypedCommands, Client, ParsingError, RedisError, ToRedisArgs,
    aio::{ConnectionManager, ConnectionManagerConfig},
    from_redis_value_ref,
    streams::{
        StreamId, StreamKey, StreamReadOptions, StreamReadReply, StreamTrimOptions,
        StreamTrimmingMode,
    },
};
use std::{
    clone::Clone,
    collections::{BTreeMap, HashMap},
    pin::Pin,
    sync::Arc,
    task::Poll,
    time::{Duration, Instant},
};
use tokio::sync::{
    Mutex, OwnedSemaphorePermit, RwLock, Semaphore,
    mpsc::{self, UnboundedReceiver, UnboundedSender},
};
use tokio_stream::wrappers::UnboundedReceiverStream;
use uuid::Uuid;

#[cfg(test)]
use std::any::Any;

static GROUP: &str = "_celery";

type ConsumerStream = dyn Stream<Item = Result<Delivery, Box<dyn DeliveryError>>>;

struct Consumer {
    inner: Pin<Box<ConsumerStream>>,
}

impl DeliveryStream for Consumer {}
impl DeliveryError for BrokerError {
    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any + Send + Sync> {
        self
    }
}

impl From<BrokerError> for Box<dyn DeliveryError> {
    fn from(err: BrokerError) -> Self {
        Box::new(err)
    }
}

impl DeliveryError for RedisError {
    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any + Send + Sync> {
        self
    }
}

impl From<RedisError> for Box<dyn DeliveryError> {
    fn from(err: RedisError) -> Self {
        Box::new(err)
    }
}

struct Delivery {
    queue: Arc<dyn RedisQueue>,
    item: StreamItem,
    #[allow(dead_code)]
    permit: Option<OwnedSemaphorePermit>,
}

impl std::fmt::Debug for Delivery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.item.fmt(f)
    }
}

impl std::fmt::Display for Delivery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "queue: {queue}, id: {id}",
            queue = self.item.key,
            id = self.item.id
        )
    }
}

#[async_trait]
impl super::Delivery for Delivery {
    async fn resend(
        &self,
        broker: &dyn Broker,
        eta: Option<DateTime<Utc>>,
    ) -> Result<(), BrokerError> {
        self.ack().await?;
        let mut message = self.try_deserialize_message()?;
        message.headers.eta = eta;
        // Increment the number of retries.
        message.headers.retries = Some(message.headers.retries.map_or(1, |retry| retry + 1));
        broker.send(message, self.item.key.as_ref()).await
    }
    async fn remove(&self) -> Result<(), BrokerError> {
        unimplemented!()
    }
    async fn ack(&self) -> Result<(), BrokerError> {
        self.queue
            .ack(self.item.key.clone(), self.item.id.clone())
            .await;
        Ok(())
    }
    async fn nack(&self) -> Result<(), BrokerError> {
        unimplemented!()
    }
}

impl Stream for Consumer {
    type Item = Result<Box<dyn super::Delivery>, Box<dyn DeliveryError>>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<<Self as futures::Stream>::Item>> {
        use futures_lite::stream::StreamExt;
        use std::task::Poll;
        match self.inner.poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Err(err))) => Poll::Ready(Some(Err(err))),
            Poll::Ready(Some(Ok(item))) => {
                Poll::Ready(Some(Ok(Box::new(item) as Box<dyn super::Delivery>)))
            }
        }
    }
}

struct Config {
    broker_url: String,
    prefetch_count: u16,
    queues: HashMap<String, QueueConfig>,
    heartbeat: Option<u16>,
}

#[derive(Clone, Default)]
struct QueueConfig {
    broadcast: bool,
}

pub struct RedisBrokerBuilder {
    config: Config,
}

#[async_trait]
impl BrokerBuilder for RedisBrokerBuilder {
    /// Create a new `BrokerBuilder`.
    fn new(broker_url: &str) -> Self {
        RedisBrokerBuilder {
            config: Config {
                broker_url: broker_url.into(),
                prefetch_count: 10,
                queues: HashMap::new(),
                heartbeat: Some(60),
            },
        }
    }

    /// Set the prefetch count.
    fn prefetch_count(mut self: Box<Self>, prefetch_count: u16) -> Box<dyn BrokerBuilder> {
        self.config.prefetch_count = prefetch_count;
        self
    }

    /// Declare a queue.
    fn declare_queue(mut self: Box<Self>, name: &str) -> Box<dyn BrokerBuilder> {
        if !self.config.queues.contains_key(name) {
            self.config
                .queues
                .insert(name.into(), QueueConfig { broadcast: false });
        }
        self
    }

    /// Declare a broadcast queue.
    fn declare_broadcast_queue(mut self: Box<Self>, name: &str) -> Box<dyn BrokerBuilder> {
        if !self.config.queues.contains_key(name) {
            self.config
                .queues
                .insert(name.into(), QueueConfig { broadcast: true });
        }
        self
    }

    /// Declare an exclusive queue.
    fn declare_exclusive_queue(self: Box<Self>, name: &str) -> Box<dyn BrokerBuilder> {
        log::warn!("declare_exclusive_queue on redis broker is identical to declare_queue");
        self.declare_queue(name)
    }

    /// Set the heartbeat.
    fn heartbeat(mut self: Box<Self>, heartbeat: Option<u16>) -> Box<dyn BrokerBuilder> {
        if heartbeat.is_some() {
            log::warn!("Setting heartbeat on redis broker has no effect");
        }
        self.config.heartbeat = heartbeat;
        self
    }

    /// Set the per-queue expiry time.
    fn set_queue_expire_time(
        self: Box<Self>,
        _queue_name: &str,
        _queue_expire_time_ms: u32,
    ) -> Box<dyn BrokerBuilder> {
        log::warn!("Setting queue_expire_time on redis broker has no effect");
        self
    }

    /// Set the per-queue message TTL.
    fn set_queue_message_ttl(
        self: Box<Self>,
        _queue_name: &str,
        _queue_message_ttl_ms: u32,
    ) -> Box<dyn BrokerBuilder> {
        log::warn!("Setting queue_message_ttl on redis broker has no effect");
        self
    }

    /// Set the queue type.
    fn set_queue_type(
        self: Box<Self>,
        _queue_name: &str,
        _queue_type: &str,
    ) -> Box<dyn BrokerBuilder> {
        log::warn!("Setting queue_type on redis broker has no effect");
        self
    }

    /// Construct the `Broker` with the given configuration.
    async fn build(&self, connection_timeout: u32) -> Result<Box<dyn Broker>, BrokerError> {
        let url = self.config.broker_url.as_str();
        let consumer = Uuid::new_v4().hyphenated().to_string();
        let timeout = Duration::from_secs(connection_timeout as u64);
        let conn = RedisBroker::connect(url, timeout).await?;

        let prefetch_count = self.config.prefetch_count.clamp(1, u16::MAX);
        log::debug!("Setting global prefetch limit to {prefetch_count}");
        let pending_tasks = Arc::new(Semaphore::new(prefetch_count as usize));

        let queues = declare_queues(url, timeout, &consumer, &self.config.queues).await?;

        Ok(Box::new(RedisBroker {
            uri: self.config.broker_url.clone(),
            conn,
            consumer: consumer.as_str().into(),
            pending_tasks,
            queues: RwLock::new(queues),
            queue_declare_options: self.config.queues.clone(),
        }))
    }
}

pub struct RedisBroker {
    uri: String,
    /// Broker connection.
    conn: ConnectionManager,
    // Unique consumer name for this process
    consumer: Arc<str>,
    /// Mapping of queue name to Queue implementations.
    queues: RwLock<HashMap<String, Arc<dyn RedisQueue>>>,
    queue_declare_options: HashMap<String, QueueConfig>,

    /// Keep track of and enforce global prefetch count.
    pending_tasks: Arc<Semaphore>,
}

impl RedisBroker {
    async fn connect(
        uri: &str,
        connection_timeout: Duration,
    ) -> Result<ConnectionManager, BrokerError> {
        log::debug!("Creating client");
        let client = Client::open(uri).map_err(|_| BrokerError::InvalidBrokerUrl(safe_url(uri)))?;

        log::debug!("Creating connection manager with connection_timeout={connection_timeout:?}");
        let conn = client
            .get_connection_manager_with_config(
                ConnectionManagerConfig::new()
                    .set_connection_timeout(connection_timeout.into())
                    .set_response_timeout(connection_timeout.into()),
            )
            .await?;

        Ok(conn)
    }
}

#[async_trait]
impl Broker for RedisBroker {
    async fn consume(
        &self,
        queue: &str,
        _error_handler: Box<dyn Fn(BrokerError) + Send + Sync + 'static>,
    ) -> Result<(String, Box<dyn DeliveryStream>), BrokerError> {
        let queue = self
            .queues
            .read()
            .await
            .get(queue)
            .ok_or_else::<BrokerError, _>(|| BrokerError::UnknownQueue(queue.into()))?
            .clone();

        // Create unique consumer tag.
        let consumer_tag = Uuid::new_v4().hyphenated().to_string();
        let pending_tasks = self.pending_tasks.clone();

        let consumer = Box::new(Consumer {
            inner: Box::pin(async_stream::stream! {
                use futures_lite::stream::StreamExt;

                let mut last_id = None;

                loop {
                    let item = match queue.subscribe(last_id.clone()).next().await {
                        None => {
                            continue;
                        },
                        Some(Err(err)) => {
                            yield Err(err.into());
                            continue;
                        },
                        Some(Ok(item)) => {
                            last_id.replace(item.id.clone());
                            item
                        },
                    };

                    let delivery = Delivery {
                        item,
                        queue: queue.clone(),
                        permit: pending_tasks.clone().acquire_owned().await.ok(),
                    };

                    if delivery.permit.is_some() {
                        yield Ok(delivery);
                    } else {
                        yield Err(BrokerError::Retry(Box::new(delivery)).into());
                    }
                }
            }),
        });

        Ok((consumer_tag, consumer))
    }

    async fn cancel(&self, _consumer_tag: &str) -> Result<(), BrokerError> {
        Ok(())
    }

    async fn ack(&self, delivery: &dyn super::Delivery) -> Result<(), BrokerError> {
        delivery.ack().await
    }

    async fn nack(&self, _delivery: &dyn super::Delivery) -> Result<(), BrokerError> {
        log::warn!("Negative acknowledges not supported.");
        Ok(())
    }

    /// Retry a delivery.
    async fn retry(
        &self,
        delivery: &dyn super::Delivery,
        eta: Option<DateTime<Utc>>,
    ) -> Result<(), BrokerError> {
        delivery.resend(self, eta).await?;
        Ok(())
    }

    /// Send a [`Message`](protocol/struct.Message.html) into a queue.
    async fn send(&self, message: Message, queue: &str) -> Result<(), BrokerError> {
        if let Some(queue) = self.queues.read().await.get(queue) {
            queue.send(self.conn.clone(), message).await?;
        } else {
            self.conn
                .clone()
                .xadd(
                    queue,
                    "*",
                    &[
                        // TODO: Use redis serialization instead of JSON
                        ("json", message.json_serialized(None)?),
                        // ("properties", &message.properties.to_redis_args()),
                        // ("headers", &message.headers.to_redis_args()),
                        // ("raw_body", &message.raw_body.to_redis_args()),
                    ],
                )
                .await?;
        }
        Ok(())
    }

    /// Increase the `prefetch_count`. This has to be done when a task with a future
    /// ETA is consumed.
    async fn increase_prefetch_count(&self) -> Result<IncrementHandle, BrokerError> {
        Ok(IncrementHandle::new(self.pending_tasks.clone()))
    }

    /// Clone all channels and connection.
    async fn close(&self) -> Result<(), BrokerError> {
        let mut conn = self.conn.clone();
        self.queues.write().await.clear();
        Ok(redis::cmd("QUIT").query_async(&mut conn).await?)
    }

    fn safe_url(&self) -> String {
        safe_url(&self.uri)
    }

    async fn reconnect(&self, connection_timeout: u32) -> Result<(), BrokerError> {
        let mut conn = self.conn.clone();
        let mut queues = self.queues.write().await;
        let connection_timeout = Duration::from_secs(connection_timeout as u64);
        // Stop additional task fetching
        let old_prefetch_count = self.pending_tasks.forget_permits(Semaphore::MAX_PERMITS);

        let start = Instant::now();
        let mut result = Err(BrokerError::NotConnected);

        while result.is_err() && start.elapsed() < connection_timeout {
            // Wait for reconnect or timeout
            result = match tokio::time::timeout(connection_timeout, conn.ping()).await {
                Ok(Ok(res)) if res == "PONG" => declare_queues(
                    &self.uri,
                    connection_timeout,
                    &self.consumer,
                    &self.queue_declare_options,
                )
                .await
                .map(|qs| {
                    *queues = qs;
                    self.pending_tasks.add_permits(old_prefetch_count);
                }),
                Ok(Err(e)) => Err(BrokerError::RedisError(e)),
                Ok(Ok(_)) => Err(BrokerError::NotConnected),
                Err(_) => Err(BrokerError::NotConnected),
            }
        }

        result
    }

    #[cfg(test)]
    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

async fn declare_queues(
    url: &str,
    connection_timeout: Duration,
    consumer: &str,
    queues: &HashMap<String, QueueConfig>,
) -> Result<HashMap<String, Arc<dyn RedisQueue>>, BrokerError> {
    use futures::TryStreamExt;

    log::debug!("Creating streams");

    let broadcast = Arc::new(RedisStreamsBatchReader::broadcast(
        queues.iter().filter(|(_, c)| c.broadcast).count(),
        RedisBroker::connect(url, connection_timeout).await?,
        connection_timeout / 2,
    ));

    let cooperative = Arc::new(RedisStreamsBatchReader::cooperative(
        queues.iter().filter(|(_, c)| !c.broadcast).count(),
        RedisBroker::connect(url, connection_timeout).await?,
        connection_timeout / 2,
        GROUP,
        consumer,
    ));

    futures::stream::iter(queues.iter().map(Ok))
        .and_then(|(queue, config)| async {
            Ok((
                queue.clone(),
                if config.broadcast {
                    BroadcastQueue::new(broadcast.clone(), queue).await?
                } else {
                    CooperativeQueue::new(cooperative.clone(), queue, GROUP, consumer).await?
                },
            ))
        })
        .try_collect()
        .await
}

fn safe_url(broker_url: &str) -> String {
    let parsed_url = redis::parse_redis_url(broker_url);
    match parsed_url {
        Some(url) => format!(
            "{}://{}:***@{}:{}/{}",
            url.scheme(),
            url.username(),
            url.host_str().unwrap(),
            url.port().unwrap(),
            url.path(),
        ),
        None => {
            log::error!("Invalid redis url.");
            String::from("")
        }
    }
}

#[async_trait]
trait RedisQueue: Send + Sync {
    fn name(&self) -> &str;

    async fn ack(&self, key: StreamName, id: String);

    async fn send(&self, mut conn: ConnectionManager, message: Message) -> Result<(), BrokerError> {
        conn.xadd(
            self.name(),
            "*",
            &[
                // TODO: Use redis serialization instead of JSON
                ("json", message.json_serialized(None)?),
                // ("properties", &message.properties.to_redis_args()),
                // ("headers", &message.headers.to_redis_args()),
                // ("raw_body", &message.raw_body.to_redis_args()),
            ],
        )
        .await?;
        Ok(())
    }

    fn subscribe(
        &self,
        last_id: Option<String>,
    ) -> UnboundedReceiverStream<Result<StreamItem, BrokerError>>;
}

struct BroadcastQueue {
    queue: String,
    reader: Arc<RedisStreamsBatchReader>,
}

#[async_trait]
impl RedisQueue for BroadcastQueue {
    async fn ack(&self, key: StreamName, id: String) {
        self.reader.ack(key, id).await
    }

    fn name(&self) -> &str {
        self.queue.as_str()
    }

    fn subscribe(
        &self,
        last_id: Option<String>,
    ) -> UnboundedReceiverStream<Result<StreamItem, BrokerError>> {
        self.reader
            .subscribe(self.name().into(), last_id.or_else(|| Some("$".into())))
    }
}

impl BroadcastQueue {
    #[allow(clippy::new_ret_no_self)]
    pub async fn new(
        reader: Arc<RedisStreamsBatchReader>,
        queue: &str,
    ) -> Result<Arc<dyn RedisQueue>, RedisError> {
        // Push a dummy message to init the stream
        let mut conn = reader.conn.clone();
        if let Some(id) = conn.xadd(queue, "*", &[("init", 0)]).await? {
            tokio::time::sleep(Duration::from_millis(1)).await;
            conn.xdel(queue, &[id]).await?;
        }
        Ok(Arc::new(Self {
            queue: queue.to_string(),
            reader,
        }))
    }
}

struct CooperativeQueue {
    queue: String,
    reader: Arc<RedisStreamsBatchReader>,
}

#[async_trait]
impl RedisQueue for CooperativeQueue {
    async fn ack(&self, key: StreamName, id: String) {
        self.reader.ack(key, id).await
    }

    fn name(&self) -> &str {
        self.queue.as_str()
    }

    fn subscribe(
        &self,
        _last_id: Option<String>,
    ) -> UnboundedReceiverStream<Result<StreamItem, BrokerError>> {
        self.reader.subscribe(self.name().into(), Some(">".into()))
    }
}

impl CooperativeQueue {
    #[allow(clippy::new_ret_no_self)]
    pub async fn new(
        reader: Arc<RedisStreamsBatchReader>,
        queue: &str,
        group: &str,
        consumer: &str,
    ) -> Result<Arc<dyn RedisQueue>, RedisError> {
        let mut conn = reader.conn.clone();
        let _ = conn.xgroup_create_mkstream(queue, group, "$").await;
        let _ = conn.xgroup_createconsumer(queue, group, consumer).await?;
        Ok(Arc::new(Self {
            reader,
            queue: queue.to_string(),
        }))
    }
}

type StreamName = Arc<str>;
#[derive(Debug)]
struct StreamItem {
    key: StreamName,
    id: String,
    json: Result<String, ParsingError>,
}

#[derive(Clone)]
struct RedisStreamsBatchReader {
    conn: ConnectionManager,
    observers: UnboundedSender<StreamObserver>,
    processed: Arc<Mutex<BTreeMap<StreamName, Vec<String>>>>,
    #[allow(dead_code)]
    handle: Arc<tokio_util::task::AbortOnDropHandle<()>>,
}

impl RedisStreamsBatchReader {
    /// Subscribe to Redis stream, start listening since last message id.
    pub fn subscribe<P: Into<Option<String>>>(
        &self,
        key: StreamName,
        last_id: P,
    ) -> UnboundedReceiverStream<Result<StreamItem, BrokerError>> {
        let (sender, receiver) = mpsc::unbounded_channel();
        self.observers
            .send(StreamObserver {
                key,
                // `XREAD {stream-key} "0-0"` means read from the start of the stream
                last_id: last_id.into().unwrap_or_else(|| "0-0".to_string()),
                callback: sender,
            })
            .ok();
        UnboundedReceiverStream::new(receiver)
    }

    pub fn broadcast(size: usize, conn: ConnectionManager, wait_time: Duration) -> Self {
        Self::new(
            size,
            conn,
            StreamReadOptions::default()
                .count(1)
                .block(wait_time.as_millis() as usize),
        )
    }

    pub fn cooperative(
        size: usize,
        conn: ConnectionManager,
        wait_time: Duration,
        group: &str,
        consumer: &str,
    ) -> Self {
        Self::new(
            size,
            conn,
            StreamReadOptions::default()
                .count(1)
                .block(wait_time.as_millis() as usize)
                .group(group, consumer),
        )
    }

    fn new(size: usize, mut conn: ConnectionManager, opts: StreamReadOptions) -> Self {
        let (observers_tx, mut observers_rx) = mpsc::unbounded_channel();
        let processed = Arc::new(Mutex::new(BTreeMap::new()));

        Self {
            conn: conn.clone(),
            observers: observers_tx.clone(),
            processed: processed.clone(),
            handle: Arc::new(tokio_util::task::AbortOnDropHandle::new(tokio::spawn(
                async move {
                    let mut stream_keys = Vec::with_capacity(size);
                    let mut message_ids = Vec::with_capacity(size);
                    let mut callbacks = HashMap::with_capacity(size);
                    loop {
                        // Populate the set of active observers and their streams
                        if !Self::pull_observers(
                            &mut observers_rx,
                            &mut stream_keys,
                            &mut message_ids,
                            &mut callbacks,
                        )
                        .await
                        {
                            break; // The observers channel has been closed
                        }

                        // Acknowledge and delete processed messages
                        if let Err(err) =
                            Self::ack_messages(&mut conn, &opts, processed.as_ref(), &mut callbacks)
                                .await
                        {
                            for obs in callbacks.values() {
                                let _ = obs.on_error(BrokerError::RedisError(err.clone()));
                            }
                            break;
                        }

                        // Read stream values and notify each observer
                        if let Err(err) = Self::read_streams(
                            &mut conn,
                            &opts,
                            &stream_keys,
                            &message_ids,
                            &mut callbacks,
                        )
                        .await
                        {
                            for obs in callbacks.values() {
                                let _ = obs.on_error(BrokerError::RedisError(err.clone()));
                            }
                            break;
                        }

                        Self::push_observers(&observers_tx, &mut stream_keys, &mut callbacks);
                    }
                },
            ))),
        }
    }

    pub async fn ack(&self, key: StreamName, id: String) {
        self.processed.lock().await.entry(key).or_default().push(id)
    }

    async fn pull_observers(
        observers_rx: &mut UnboundedReceiver<StreamObserver>,
        stream_keys: &mut Vec<StreamName>,
        message_ids: &mut Vec<String>,
        callbacks: &mut HashMap<StreamName, StreamObserver>,
    ) -> bool {
        message_ids.clear();
        let mut idx = 0;
        let len = callbacks.capacity();
        loop {
            if idx >= len {
                return idx > 0;
            }
            let obs = if idx == 0 {
                // Wait for the first observer
                observers_rx.recv().await
            } else {
                // Eagerly pull more observers to fill the whole senders map
                observers_rx.try_recv().ok()
            };
            if let Some(obs) = obs {
                idx += 1;
                stream_keys.push(obs.key.clone());
                message_ids.push(obs.last_id.clone());
                callbacks.insert(obs.key.clone(), obs);
            } else {
                // We filled up to the batch size, or no more pending tasks
                idx = len;
            }
        }
    }

    async fn ack_messages(
        conn: &mut ConnectionManager,
        opts: &StreamReadOptions,
        processed: &Mutex<BTreeMap<StreamName, Vec<String>>>,
        callbacks: &mut HashMap<StreamName, StreamObserver>,
    ) -> Result<(), RedisError> {
        let mut processed = processed.lock().await;
        while let Some((key, ids)) = processed.pop_first() {
            if opts.read_only() {
                let one_minute_ago = ids
                    .iter()
                    .filter_map(|id| id.split('-').next())
                    .filter_map(|id| id.parse::<i64>().ok())
                    .min()
                    .unwrap_or_else(|| chrono::Local::now().timestamp_millis())
                    .saturating_sub(Duration::from_secs(60).as_millis() as i64)
                    .to_string();

                conn.xtrim_options(
                    key.as_ref(),
                    &StreamTrimOptions::minid(StreamTrimmingMode::Approx, one_minute_ago),
                )
                .await?;
            } else {
                conn.xack(key.as_ref(), GROUP, &ids).await?;
                #[allow(clippy::collapsible_if)]
                if let Some(obs) = callbacks.get_mut(key.as_ref()) {
                    if obs.last_id != ">" && ids.contains(&obs.last_id) {
                        obs.last_id = ">".into();
                    }
                }
                conn.xdel(key.as_ref(), &ids).await?;
            }
        }
        Ok(())
    }

    async fn read_streams(
        conn: &mut ConnectionManager,
        opts: &StreamReadOptions,
        stream_keys: &[StreamName],
        message_ids: &[String],
        callbacks: &mut HashMap<StreamName, StreamObserver>,
    ) -> Result<(), RedisError> {
        // perform Redis XREAD for streams given their last message ids
        let res: Result<Option<StreamReadReply>, RedisError> = conn
            .xread_options(&[StreamKeysToRedisArgs(stream_keys)], message_ids, opts)
            .await;

        let res = match res {
            Ok(result) => result,
            Err(err) => {
                if !err.is_timeout() {
                    return Err(err);
                } else {
                    None
                }
            }
        };

        if let Some(res) = res {
            for StreamKey { key, ids } in res.keys {
                // Match received Redis messages with their subscriber's channel
                if let Some(obs) = callbacks.get_mut(key.as_str()) {
                    for id in ids {
                        // update latest message id for a given stream
                        obs.last_id = id.id.clone();
                        // forward message to subscriber
                        if obs.on_next(id).is_err() {
                            // sender is closed, remove it
                            callbacks.remove(key.as_str());
                            break;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn push_observers(
        observers_tx: &UnboundedSender<StreamObserver>,
        stream_keys: &mut Vec<StreamName>,
        callbacks: &mut HashMap<StreamName, StreamObserver>,
    ) {
        for key in stream_keys.drain(..) {
            if let Some(obs) = callbacks.remove(key.as_ref()) {
                if obs.is_closed() {
                    continue; // skip rescheduling of closed senders
                }
                if let Err(err) = observers_tx.send(obs) {
                    log::warn!("Failed to reschedule: {err}");
                    break;
                }
            }
        }

        callbacks.clear();
    }
}

struct StreamObserver {
    /// Redis stream key
    key: StreamName,
    /// Last message id read from given Redis stream (default: "0-0")
    last_id: String,
    /// Channel where messages should be forwarded.
    callback: UnboundedSender<Result<StreamItem, BrokerError>>,
}

impl StreamObserver {
    pub fn is_closed(&self) -> bool {
        self.callback.is_closed()
    }

    pub fn on_next(
        &self,
        id: StreamId,
    ) -> Result<(), tokio::sync::mpsc::error::SendError<Result<StreamItem, BrokerError>>> {
        if let Some(json) = id.map.get("json") {
            self.callback.send(Ok(StreamItem {
                key: self.key.clone(),
                id: id.id,
                json: from_redis_value_ref::<String>(json),
            }))
        } else {
            Ok(())
        }
    }

    pub fn on_error(
        &self,
        err: BrokerError,
    ) -> Result<(), tokio::sync::mpsc::error::SendError<Result<StreamItem, BrokerError>>> {
        self.callback.send(Err(err))
    }
}

struct StreamKeysToRedisArgs<'a>(&'a [StreamName]);

impl<'a> StreamKeysToRedisArgs<'a> {
    pub fn iter(&self) -> impl Iterator<Item = &'a str> {
        self.0.iter().map(|key| key.as_ref())
    }
}

impl<'a> ToRedisArgs for StreamKeysToRedisArgs<'a> {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + redis::RedisWrite,
    {
        for key in self.iter() {
            out.write_arg(key.as_bytes());
        }
    }
}

impl TryDeserializeMessage for Delivery {
    fn try_deserialize_message(&self) -> Result<Message, ProtocolError> {
        serde_json::from_str::<protocol::Delivery>(
            self.item
                .json
                .as_ref()
                .map(|s| s.as_str())
                .unwrap_or_default(),
        )?
        .try_deserialize_message()
    }
}

// impl TryDeserializeMessage for HashMap<String, Value> {
//     fn try_deserialize_message(&self) -> Result<Message, ProtocolError> {
//         Ok(Message {
//             properties: from_redis_value_ref(self.get("properties").map(Ok).unwrap_or_else(
//                 || Err(ProtocolError::MissingRequiredProperty("properties".into())),
//             )?)
//             .map_err(|err| ProtocolError::InvalidProperty(format!("properties: {err}")))?,

//             headers: from_redis_value_ref(
//                 self.get("headers")
//                     .map(Ok)
//                     .unwrap_or_else(|| Err(ProtocolError::MissingHeaders))?,
//             )
//             .map_err(|err| ProtocolError::InvalidProperty(format!("headers: {err}")))?,

//             raw_body: from_redis_value_ref(self.get("raw_body").map(Ok).unwrap_or_else(|| {
//                 Err(ProtocolError::MissingRequiredProperty("raw_body".into()))
//             })?)
//             .map_err(|err| ProtocolError::InvalidProperty(format!("raw_body: {err}")))?,
//         })
//     }
// }

// impl ToRedisArgs for MessageProperties {
//     fn write_redis_args<W>(&self, out: &mut W)
//     where
//         W: ?Sized + redis::RedisWrite,
//     {
//         out.write_arg(b"correlation_id");
//         out.write_arg(self.correlation_id.as_bytes());

//         out.write_arg(b"content_type");
//         out.write_arg(self.content_type.as_bytes());

//         out.write_arg(b"content_encoding");
//         out.write_arg(self.content_encoding.as_bytes());

//         if let Some(reply_to) = self.reply_to.as_ref() {
//             out.write_arg(b"reply_to");
//             out.write_arg(reply_to.as_bytes());
//         }

//         if let Some(delivery_info) = self.delivery_info.as_ref() {
//             out.write_arg(b"delivery_info_exchange");
//             out.write_arg(delivery_info.exchange.as_bytes());
//             out.write_arg(b"delivery_info_routing_key");
//             out.write_arg(delivery_info.routing_key.as_bytes());
//         }
//     }
// }

// impl FromRedisValue for MessageProperties {
//     fn from_redis_value(v: Value) -> Result<Self, ParsingError> {
//         v.as_map_iter()
//             .map(|mut entries| {
//                 entries.try_fold(Self::default(), |mut props, (key, val)| {
//                     let key: String = from_redis_value_ref(key)?;
//                     match key.as_str() {
//                         "correlation_id" => {
//                             props.correlation_id = from_redis_value_ref(val)?;
//                         }
//                         "content_type" => {
//                             props.content_type = from_redis_value_ref(val)?;
//                         }
//                         "content_encoding" => {
//                             props.content_encoding = from_redis_value_ref(val)?;
//                         }
//                         "reply_to" => {
//                             props.reply_to = from_redis_value_ref(val)?;
//                         }
//                         "delivery_info_exchange" => {
//                             let mut info = props.delivery_info.unwrap_or_default();
//                             info.exchange = from_redis_value_ref(val)?;
//                             props.delivery_info = Some(info);
//                         }
//                         "delivery_info_routing_key" => {
//                             let mut info = props.delivery_info.unwrap_or_default();
//                             info.routing_key = from_redis_value_ref(val)?;
//                             props.delivery_info = Some(info);
//                         }
//                         _ => {}
//                     }
//                     Ok(props)
//                 })
//             })
//             .unwrap_or_else(|| Err("Failed to deserialize MessageProperties".into()))
//     }
// }

// impl ToRedisArgs for MessageHeaders {
//     fn write_redis_args<W>(&self, out: &mut W)
//     where
//         W: ?Sized + redis::RedisWrite,
//     {
//         out.write_arg(b"id");
//         out.write_arg(self.id.as_bytes());

//         out.write_arg(b"task");
//         out.write_arg(self.task.as_bytes());

//         out.write_arg(b"lang");
//         self.lang.write_redis_args(out);

//         out.write_arg(b"root_id");
//         self.root_id.write_redis_args(out);

//         out.write_arg(b"parent_id");
//         self.parent_id.write_redis_args(out);

//         out.write_arg(b"group");
//         self.group.write_redis_args(out);

//         out.write_arg(b"meth");
//         self.meth.write_redis_args(out);

//         out.write_arg(b"shadow");
//         self.shadow.write_redis_args(out);

//         out.write_arg(b"eta");
//         self.eta.map(|t| t.to_rfc3339()).write_redis_args(out);

//         out.write_arg(b"expires");
//         self.expires.map(|t| t.to_rfc3339()).write_redis_args(out);

//         out.write_arg(b"retries");
//         self.retries.write_redis_args(out);

//         out.write_arg(b"timelimit");
//         self.timelimit.write_redis_args(out);

//         out.write_arg(b"argsrepr");
//         self.argsrepr.write_redis_args(out);

//         out.write_arg(b"kwargsrepr");
//         self.kwargsrepr.write_redis_args(out);

//         out.write_arg(b"origin");
//         self.origin.write_redis_args(out);
//     }
// }

// impl FromRedisValue for MessageHeaders {
//     fn from_redis_value(v: Value) -> Result<Self, ParsingError> {
//         v.as_map_iter()
//             .map(|mut entries| {
//                 entries.try_fold(Self::default(), |mut props, (key, val)| {
//                     let key: String = from_redis_value_ref(key)?;
//                     match key.as_str() {
//                         "id" => {
//                             props.id = from_redis_value_ref(val)?;
//                         }
//                         "task" => {
//                             props.task = from_redis_value_ref(val)?;
//                         }
//                         "lang" => {
//                             props.lang = from_redis_value_ref(val)?;
//                         }
//                         "root_id" => {
//                             props.root_id = from_redis_value_ref(val)?;
//                         }
//                         "parent_id" => {
//                             props.parent_id = from_redis_value_ref(val)?;
//                         }
//                         "group" => {
//                             props.group = from_redis_value_ref(val)?;
//                         }
//                         "meth" => {
//                             props.meth = from_redis_value_ref(val)?;
//                         }
//                         "shadow" => {
//                             props.shadow = from_redis_value_ref(val)?;
//                         }
//                         "eta" => {
//                             let eta: String = from_redis_value_ref(val)?;
//                             let eta = DateTime::parse_from_rfc3339(&eta).map_err(|err| {
//                                 format!("Failed to deserialize MessageHeaders.eta: {err}")
//                             })?;
//                             props.eta = Some(eta.to_utc());
//                         }
//                         "expires" => {
//                             let expires: String = from_redis_value_ref(val)?;
//                             let expires =
//                                 DateTime::parse_from_rfc3339(&expires).map_err(|err| {
//                                     format!("Failed to deserialize MessageHeaders.expires: {err}")
//                                 })?;
//                             props.expires = Some(expires.to_utc());
//                         }
//                         "retries" => {
//                             props.retries = from_redis_value_ref(val)?;
//                         }
//                         "timelimit" => {
//                             props.timelimit = from_redis_value_ref(val)?;
//                         }
//                         "argsrepr" => {
//                             props.argsrepr = from_redis_value_ref(val)?;
//                         }
//                         "kwargsrepr" => {
//                             props.kwargsrepr = from_redis_value_ref(val)?;
//                         }
//                         "origin" => {
//                             props.origin = from_redis_value_ref(val)?;
//                         }
//                         _ => {}
//                     }
//                     Ok(props)
//                 })
//             })
//             .unwrap_or_else(|| Err("Failed to deserialize MessageHeaders".into()))
//     }
// }
