//! AMQP broker.

use async_trait::async_trait;
use chrono::{DateTime, SecondsFormat, Utc};
use futures::{Stream, TryStreamExt};

use lapin::{
    options::{
        BasicAckOptions, BasicCancelOptions, BasicConsumeOptions, BasicNackOptions,
        BasicPublishOptions, ExchangeDeclareOptions, QueueDeclareOptions,
    },
    types::{AMQPValue, FieldArray, FieldTable},
    uri::{self, AMQPUri},
    {BasicProperties, Channel, Connection, ConnectionProperties, ExchangeKind, Queue},
};

use log::debug;

use std::{collections::HashMap, pin::Pin, str::FromStr, sync::Arc};

use tokio::sync::{Mutex, OwnedSemaphorePermit, RwLock, Semaphore};

use super::{Broker, BrokerBuilder, DeliveryError, DeliveryStream, IncrementHandle};
use crate::error::{BrokerError, ProtocolError};
use crate::protocol::{Message, MessageHeaders, MessageProperties, TryDeserializeMessage};
use tokio_executor_trait::Tokio as TokioExecutor;

#[cfg(test)]
use std::any::Any;

type ConsumerStream = dyn Stream<Item = Result<Delivery, Box<dyn DeliveryError>>>;

struct Consumer {
    inner: Pin<Box<ConsumerStream>>,
}
impl DeliveryStream for Consumer {}

impl DeliveryError for lapin::Error {
    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any + Send + Sync> {
        self
    }
}

impl From<lapin::Error> for Box<dyn DeliveryError> {
    fn from(err: lapin::Error) -> Self {
        Box::new(err)
    }
}

struct Delivery {
    item: lapin::message::Delivery,
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
            "exchange: {exchange}, queue: {queue}, id: {id}",
            exchange = self.item.exchange,
            queue = self.item.routing_key,
            id = self.item.delivery_tag
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
        let mut message = self.try_deserialize_message()?;
        message.headers.eta = eta;
        // Increment the number of retries.
        message.headers.retries = Some(message.headers.retries.map_or(1, |retry| retry + 1));
        broker.send(message, self.item.routing_key.as_str()).await
    }
    async fn remove(&self) -> Result<(), BrokerError> {
        todo!()
    }
    async fn ack(&self) -> Result<(), BrokerError> {
        lapin::acker::Acker::ack(&self.item, BasicAckOptions::default()).await?;
        Ok(())
    }
    async fn nack(&self) -> Result<(), BrokerError> {
        lapin::acker::Acker::nack(&self.item, BasicNackOptions::default()).await?;
        Ok(())
    }
}

impl Stream for Consumer {
    type Item = Result<Box<dyn super::Delivery>, Box<dyn DeliveryError>>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::option::Option<<Self as futures::Stream>::Item>> {
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

#[derive(Clone, Default)]
struct QueueConfig {
    options: QueueDeclareOptions,
    broadcast: bool,
    expire_time_ms: Option<u32>,
    message_ttl_ms: Option<u32>,
    queue_type: Option<String>,
}

impl From<&QueueConfig> for ExchangeDeclareOptions {
    fn from(config: &QueueConfig) -> Self {
        ExchangeDeclareOptions {
            passive: config.options.passive,
            durable: config.options.durable,
            auto_delete: config.options.auto_delete,
            nowait: config.options.nowait,
            ..Default::default()
        }
    }
}

impl From<&QueueConfig> for QueueDeclareOptions {
    fn from(config: &QueueConfig) -> Self {
        let mut options = config.options;
        if let Some("quorum") = config.queue_type.as_deref() {
            options.durable = true;
            options.exclusive = false;
        }
        options
    }
}

impl From<&QueueConfig> for FieldTable {
    fn from(config: &QueueConfig) -> FieldTable {
        let mut fields = FieldTable::default();
        if let Some(expire_time_ms) = config.expire_time_ms {
            fields.insert("x-expires".into(), AMQPValue::LongUInt(expire_time_ms));
        }
        if let Some(message_ttl_ms) = config.message_ttl_ms {
            fields.insert("x-message-ttl".into(), AMQPValue::LongUInt(message_ttl_ms));
        }
        fields.insert(
            "x-queue-type".into(),
            AMQPValue::LongString(config.queue_type.as_deref().unwrap_or("classic").into()),
        );
        fields
    }
}

struct Config {
    broker_url: String,
    prefetch_count: u16,
    queues: HashMap<String, QueueConfig>,
    heartbeat: Option<u16>,
}

/// Builds an [`AMQPBroker`] with a custom configuration.
pub struct AMQPBrokerBuilder {
    config: Config,
}

fn create_base_connection_properties() -> ConnectionProperties {
    // See https://github.com/amqp-rs/reactor-trait/issues/1#issuecomment-1033473197
    ConnectionProperties::default().with_executor(TokioExecutor::current())
}

#[cfg(unix)]
fn create_connection_properties() -> ConnectionProperties {
    create_base_connection_properties().with_reactor(tokio_reactor_trait::Tokio)
}
#[cfg(windows)]
fn create_connection_properties() -> ConnectionProperties {
    create_base_connection_properties()
}

#[async_trait]
impl BrokerBuilder for AMQPBrokerBuilder {
    /// Create a new `AMQPBrokerBuilder`.
    fn new(broker_url: &str) -> Self {
        Self {
            config: Config {
                broker_url: broker_url.into(),
                prefetch_count: 10,
                queues: HashMap::new(),
                heartbeat: Some(60),
            },
        }
    }

    /// Set the worker [prefetch
    /// count](https://www.rabbitmq.com/confirms.html#channel-qos-prefetch).
    fn prefetch_count(mut self: Box<Self>, prefetch_count: u16) -> Box<dyn BrokerBuilder> {
        self.config.prefetch_count = prefetch_count;
        self
    }

    /// Declare a queue.
    fn declare_queue(mut self: Box<Self>, name: &str) -> Box<dyn BrokerBuilder> {
        if !self.config.queues.contains_key(name) {
            self.config.queues.insert(
                name.into(),
                QueueConfig {
                    options: QueueDeclareOptions {
                        passive: false,
                        durable: true,
                        exclusive: false,
                        auto_delete: false,
                        nowait: false,
                    },
                    ..Default::default()
                },
            );
        }
        self
    }

    /// Declare a broadcast queue.
    fn declare_broadcast_queue(mut self: Box<Self>, name: &str) -> Box<dyn BrokerBuilder> {
        if !self.config.queues.contains_key(name) {
            self.config.queues.insert(
                name.into(),
                QueueConfig {
                    options: QueueDeclareOptions {
                        passive: false,
                        durable: false,
                        exclusive: true,
                        auto_delete: false,
                        nowait: false,
                    },
                    broadcast: true,
                    ..Default::default()
                },
            );
        }
        self
    }

    /// Declare a exclusive queue.
    fn declare_exclusive_queue(mut self: Box<Self>, name: &str) -> Box<dyn BrokerBuilder> {
        if !self.config.queues.contains_key(name) {
            self.config.queues.insert(
                name.into(),
                QueueConfig {
                    options: QueueDeclareOptions {
                        passive: false,
                        durable: false,
                        exclusive: true,
                        auto_delete: false,
                        nowait: false,
                    },
                    ..Default::default()
                },
            );
        }
        self
    }

    /// Set the heartbeat.
    fn heartbeat(mut self: Box<Self>, heartbeat: Option<u16>) -> Box<dyn BrokerBuilder> {
        self.config.heartbeat = heartbeat;
        self
    }

    /// Set the per-queue expiry time.
    fn set_queue_expire_time(
        mut self: Box<Self>,
        queue_name: &str,
        queue_expire_time_ms: u32,
    ) -> Box<dyn BrokerBuilder> {
        if let Some(config) = self.config.queues.get_mut(queue_name) {
            config.expire_time_ms = Some(queue_expire_time_ms);
            config.options.durable = queue_expire_time_ms != 0;
            config.options.auto_delete = queue_expire_time_ms != 0;
        }
        self
    }

    /// Set the per-queue message TTL.
    fn set_queue_message_ttl(
        mut self: Box<Self>,
        queue_name: &str,
        queue_message_ttl_ms: u32,
    ) -> Box<dyn BrokerBuilder> {
        if let Some(config) = self.config.queues.get_mut(queue_name) {
            config.message_ttl_ms = Some(queue_message_ttl_ms);
        }
        self
    }

    /// Set the queue type.
    fn set_queue_type(
        mut self: Box<Self>,
        queue_name: &str,
        queue_type: &str,
    ) -> Box<dyn BrokerBuilder> {
        if let Some(config) = self.config.queues.get_mut(queue_name) {
            config.queue_type = Some(queue_type.to_string());
        }
        self
    }

    /// Build an `AMQPBroker`.
    async fn build(&self, connection_timeout: u32) -> Result<Box<dyn Broker>, BrokerError> {
        let mut uri = AMQPUri::from_str(&self.config.broker_url)
            .map_err(|_| BrokerError::InvalidBrokerUrl(self.config.broker_url.clone()))?;
        uri.query.heartbeat = self.config.heartbeat;
        uri.query.connection_timeout = Some((connection_timeout as u64) * 1000);

        let conn = Connection::connect_uri(uri.clone(), create_connection_properties()).await?;

        let prefetch_count = self.config.prefetch_count.clamp(1, u16::MAX);
        debug!("Setting global prefetch limit to {prefetch_count}");
        let pending_tasks = Arc::new(Semaphore::new(prefetch_count as usize));

        let consume_channel = conn.create_channel().await?;
        let produce_channel = conn.create_channel().await?;
        let queues = declare_queues(&consume_channel, &self.config.queues).await?;

        Ok(Box::new(AMQPBroker {
            uri,
            conn: Mutex::new(conn),
            consume_channel: RwLock::new(consume_channel),
            produce_channel: RwLock::new(produce_channel),
            pending_tasks,
            queues: RwLock::new(queues),
            queue_declare_options: self.config.queues.clone(),
        }))
    }
}

/// An AMQP broker.
pub struct AMQPBroker {
    uri: AMQPUri,

    /// Broker connection.
    ///
    /// This is only wrapped in a Mutex for interior mutability.
    conn: Mutex<Connection>,

    /// Channel to consume messages from.
    consume_channel: RwLock<Channel>,

    /// Channel to produce messages from.
    ///
    /// This is only wrapped in RwLock for interior mutability.
    produce_channel: RwLock<Channel>,

    /// Mapping of queue name to Queue struct.
    ///
    /// This is only wrapped in RwLock for interior mutability.
    queues: RwLock<HashMap<String, Arc<dyn AMQPQueue>>>,

    queue_declare_options: HashMap<String, QueueConfig>,

    /// Keep track of and enforce global prefetch count.
    pending_tasks: Arc<Semaphore>,
}

impl AMQPBroker {}

#[async_trait]
impl Broker for AMQPBroker {
    fn safe_url(&self) -> String {
        safe_url(&self.uri)
    }

    async fn consume(
        &self,
        queue: &str,
        error_handler: Box<dyn Fn(BrokerError) + Send + Sync + 'static>,
    ) -> Result<(String, Box<dyn DeliveryStream>), BrokerError> {
        self.conn
            .lock()
            .await
            .on_error(move |e| error_handler(BrokerError::from(e)));

        let queue = self
            .queues
            .read()
            .await
            .get(queue)
            .ok_or_else::<BrokerError, _>(|| BrokerError::UnknownQueue(queue.into()))?
            .clone();

        let consumer = self
            .consume_channel
            .read()
            .await
            .basic_consume(
                queue.name(),
                "",
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await?;

        let consumer_tag = consumer.tag().to_string();
        let pending_tasks = self.pending_tasks.clone();

        let consumer = Box::new(Consumer {
            inner: Box::pin(async_stream::stream! {
                for await item in consumer {
                    let item = match item {
                        Ok(item) => item,
                        Err(err) => {
                            yield Err(err.into());
                            continue;
                        }
                    };

                    let delivery = Delivery {
                        item,
                        permit: pending_tasks.clone().acquire_owned().await.ok()
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

    async fn cancel(&self, consumer_tag: &str) -> Result<(), BrokerError> {
        let consume_channel = self.consume_channel.write().await;
        consume_channel
            .basic_cancel(consumer_tag, BasicCancelOptions::default())
            .await?;
        Ok(())
    }

    async fn ack(&self, delivery: &dyn super::Delivery) -> Result<(), BrokerError> {
        delivery.ack().await
    }

    async fn nack(&self, delivery: &dyn super::Delivery) -> Result<(), BrokerError> {
        delivery.nack().await
    }

    async fn retry(
        &self,
        delivery: &dyn super::Delivery,
        eta: Option<DateTime<Utc>>,
    ) -> Result<(), BrokerError> {
        delivery.resend(self, eta).await?;
        Ok(())
    }

    async fn send(&self, message: Message, queue: &str) -> Result<(), BrokerError> {
        let produce_channel = self.produce_channel.read().await;
        if let Some(queue) = self.queues.read().await.get(queue) {
            queue.send(&produce_channel, message).await?;
        } else {
            let properties = message.delivery_properties();
            debug!("Sending AMQP message with: {properties:?}");
            produce_channel
                .basic_publish(
                    "",
                    queue,
                    BasicPublishOptions::default(),
                    &message.raw_body[..],
                    properties,
                )
                .await?;
        }
        Ok(())
    }

    async fn increase_prefetch_count(&self) -> Result<IncrementHandle, BrokerError> {
        Ok(IncrementHandle::new(self.pending_tasks.clone()))
    }

    async fn close(&self) -> Result<(), BrokerError> {
        let consume_channel = self.consume_channel.write().await;
        let produce_channel = self.produce_channel.write().await;
        let conn = self.conn.lock().await;

        if consume_channel.status().connected() {
            debug!("Closing consumer channel...");
            consume_channel.close(200, "OK").await?;
        }

        if produce_channel.status().connected() {
            debug!("Closing producer channel...");
            produce_channel.close(200, "OK").await?;
        }

        if conn.status().connected() {
            debug!("Closing connection...");
            conn.close(200, "OK").await?;
        }

        Ok(())
    }

    /// Try reconnecting in the event of some sort of connection error.
    async fn reconnect(&self, connection_timeout: u32) -> Result<(), BrokerError> {
        let mut conn = self.conn.lock().await;
        if !conn.status().connected() {
            debug!("Attempting to reconnect to broker");
            let mut uri = self.uri.clone();
            uri.query.connection_timeout = Some(connection_timeout as u64);
            *conn = Connection::connect_uri(uri, create_connection_properties()).await?;

            let mut consume_channel = self.consume_channel.write().await;
            let mut produce_channel = self.produce_channel.write().await;
            let mut queues = self.queues.write().await;

            *consume_channel = conn.create_channel().await?;
            *produce_channel = conn.create_channel().await?;
            *queues = declare_queues(&consume_channel, &self.queue_declare_options).await?;
        }

        Ok(())
    }

    #[cfg(test)]
    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

async fn declare_queues(
    consume_channel: &Channel,
    queues: &HashMap<String, QueueConfig>,
) -> Result<HashMap<String, Arc<dyn AMQPQueue>>, BrokerError> {
    futures::stream::iter(queues.iter().map(Ok))
        .and_then(|(queue, config)| async {
            Ok((
                queue.clone(),
                if config.broadcast {
                    BroadcastQueue::new(consume_channel, queue, config).await?
                } else {
                    CooperativeQueue::new(consume_channel, queue, config).await?
                },
            ))
        })
        .try_collect()
        .await
}

fn safe_url(uri: &AMQPUri) -> String {
    format!(
        "{}://{}:***@{}:{}/{}",
        match uri.scheme {
            uri::AMQPScheme::AMQP => "amqp",
            _ => "amqps",
        },
        uri.authority.userinfo.username,
        uri.authority.host,
        uri.authority.port,
        uri.vhost,
    )
}

#[async_trait]
trait AMQPQueue: Send + Sync {
    fn inner(&self) -> &Queue;

    fn name(&self) -> &str {
        self.inner().name().as_str()
    }

    async fn send(&self, produce_channel: &Channel, message: Message) -> Result<(), BrokerError>;
}

struct CooperativeQueue {
    inner: Queue,
}

impl CooperativeQueue {
    #[allow(clippy::new_ret_no_self)]
    async fn new(
        consume_channel: &Channel,
        queue: &str,
        config: &QueueConfig,
    ) -> Result<Arc<dyn AMQPQueue>, BrokerError> {
        Ok(Arc::new(Self {
            inner: consume_channel
                .queue_declare(queue, config.into(), config.into())
                .await?,
        }))
    }
}

#[async_trait]
impl AMQPQueue for CooperativeQueue {
    fn inner(&self) -> &Queue {
        &self.inner
    }

    async fn send(&self, produce_channel: &Channel, message: Message) -> Result<(), BrokerError> {
        let properties = message.delivery_properties();
        debug!("Sending AMQP message with: {properties:?}");
        produce_channel
            .basic_publish(
                "",
                self.name(),
                BasicPublishOptions::default(),
                &message.raw_body[..],
                properties,
            )
            .await?;
        Ok(())
    }
}

struct BroadcastQueue {
    inner: Queue,
    queue: String,
}

impl BroadcastQueue {
    #[allow(clippy::new_ret_no_self)]
    async fn new(
        consume_channel: &Channel,
        queue: &str,
        config: &QueueConfig,
    ) -> Result<Arc<dyn AMQPQueue>, BrokerError> {
        // Declare the fanout exchange
        consume_channel
            .exchange_declare(
                queue, // use the queue name as the exchange name
                ExchangeKind::Fanout,
                config.into(),
                Default::default(),
            )
            .await?;

        // Declare an anonymous exclusive queue to receive broadcasts
        let inner = consume_channel
            .queue_declare(
                "",
                QueueDeclareOptions {
                    exclusive: true,
                    ..Default::default()
                },
                Default::default(),
            )
            .await?;

        // Bind the anonymous queue to the fanout exchange
        consume_channel
            .queue_bind(
                inner.name().as_str(),
                queue,
                "",
                Default::default(),
                Default::default(),
            )
            .await?;

        Ok(Arc::new(Self {
            inner,
            queue: queue.to_owned(),
        }))
    }
}

#[async_trait]
impl AMQPQueue for BroadcastQueue {
    fn inner(&self) -> &Queue {
        &self.inner
    }

    async fn send(&self, produce_channel: &Channel, message: Message) -> Result<(), BrokerError> {
        let properties = message.delivery_properties();
        debug!(
            "Broadcasting AMQP message to {name:?} with: {properties:?}",
            name = self.queue
        );
        produce_channel
            .basic_publish(
                self.queue.as_str(),
                "",
                BasicPublishOptions::default(),
                &message.raw_body[..],
                properties,
            )
            .await?;
        Ok(())
    }
}

impl Message {
    fn delivery_properties(&self) -> BasicProperties {
        let mut properties = BasicProperties::default()
            .with_correlation_id(self.properties.correlation_id.clone().into())
            .with_content_type(self.properties.content_type.clone().into())
            .with_content_encoding(self.properties.content_encoding.clone().into())
            .with_headers(self.delivery_headers())
            .with_priority(0)
            .with_delivery_mode(2);
        if let Some(ref reply_to) = self.properties.reply_to {
            properties = properties.with_reply_to(reply_to.clone().into());
        }
        properties
    }

    fn delivery_headers(&self) -> FieldTable {
        let mut headers = FieldTable::default();
        headers.insert(
            "id".into(),
            AMQPValue::LongString(self.headers.id.clone().into()),
        );
        headers.insert(
            "task".into(),
            AMQPValue::LongString(self.headers.task.clone().into()),
        );
        if let Some(ref lang) = self.headers.lang {
            headers.insert("lang".into(), AMQPValue::LongString(lang.clone().into()));
        }
        if let Some(ref root_id) = self.headers.root_id {
            headers.insert(
                "root_id".into(),
                AMQPValue::LongString(root_id.clone().into()),
            );
        }
        if let Some(ref parent_id) = self.headers.parent_id {
            headers.insert(
                "parent_id".into(),
                AMQPValue::LongString(parent_id.clone().into()),
            );
        }
        if let Some(ref group) = self.headers.group {
            headers.insert("group".into(), AMQPValue::LongString(group.clone().into()));
        }
        if let Some(ref meth) = self.headers.meth {
            headers.insert("meth".into(), AMQPValue::LongString(meth.clone().into()));
        }
        if let Some(ref shadow) = self.headers.shadow {
            headers.insert(
                "shadow".into(),
                AMQPValue::LongString(shadow.clone().into()),
            );
        }
        if let Some(ref eta) = self.headers.eta {
            headers.insert(
                "eta".into(),
                AMQPValue::LongString(eta.to_rfc3339_opts(SecondsFormat::Millis, false).into()),
            );
        }
        if let Some(ref expires) = self.headers.expires {
            headers.insert(
                "expires".into(),
                AMQPValue::LongString(expires.to_rfc3339_opts(SecondsFormat::Millis, false).into()),
            );
        }
        if let Some(retries) = self.headers.retries {
            headers.insert("retries".into(), AMQPValue::LongUInt(retries));
        }
        let mut timelimit = FieldArray::default();
        if let Some(t) = self.headers.timelimit.0 {
            timelimit.push(AMQPValue::LongUInt(t));
        } else {
            timelimit.push(AMQPValue::Void);
        }
        if let Some(t) = self.headers.timelimit.1 {
            timelimit.push(AMQPValue::LongUInt(t));
        } else {
            timelimit.push(AMQPValue::Void);
        }
        headers.insert("timelimit".into(), AMQPValue::FieldArray(timelimit));
        if let Some(ref argsrepr) = self.headers.argsrepr {
            headers.insert(
                "argsrepr".into(),
                AMQPValue::LongString(argsrepr.clone().into()),
            );
        }
        if let Some(ref kwargsrepr) = self.headers.kwargsrepr {
            headers.insert(
                "kwargsrepr".into(),
                AMQPValue::LongString(kwargsrepr.clone().into()),
            );
        }
        if let Some(ref origin) = self.headers.origin {
            headers.insert(
                "origin".into(),
                AMQPValue::LongString(origin.clone().into()),
            );
        }
        headers
    }
}

impl TryDeserializeMessage for (Channel, Delivery) {
    fn try_deserialize_message(&self) -> Result<Message, ProtocolError> {
        self.1.try_deserialize_message()
    }
}

impl TryDeserializeMessage for Delivery {
    fn try_deserialize_message(&self) -> Result<Message, ProtocolError> {
        let headers = self
            .item
            .properties
            .headers()
            .as_ref()
            .ok_or(ProtocolError::MissingHeaders)?;
        Ok(Message {
            properties: MessageProperties {
                correlation_id: self
                    .item
                    .properties
                    .correlation_id()
                    .as_ref()
                    .map(|v| v.to_string())
                    .ok_or_else(|| {
                        ProtocolError::MissingRequiredProperty("correlation_id".into())
                    })?,
                content_type: self
                    .item
                    .properties
                    .content_type()
                    .as_ref()
                    .map(|v| v.to_string())
                    .ok_or_else(|| ProtocolError::MissingRequiredProperty("content_type".into()))?,
                content_encoding: self
                    .item
                    .properties
                    .content_encoding()
                    .as_ref()
                    .map(|v| v.to_string())
                    .ok_or_else(|| {
                        ProtocolError::MissingRequiredProperty("content_encoding".into())
                    })?,
                reply_to: self
                    .item
                    .properties
                    .reply_to()
                    .as_ref()
                    .map(|v| v.to_string()),
                delivery_info: None,
            },
            headers: MessageHeaders {
                id: get_header_str_required(headers, "id")?,
                task: get_header_str_required(headers, "task")?,
                lang: get_header_str(headers, "lang"),
                root_id: get_header_str(headers, "root_id"),
                parent_id: get_header_str(headers, "parent_id"),
                group: get_header_str(headers, "group"),
                meth: get_header_str(headers, "meth"),
                shadow: get_header_str(headers, "shadow"),
                eta: get_header_dt(headers, "eta"),
                expires: get_header_dt(headers, "expires"),
                retries: get_header_u32(headers, "retries"),
                timelimit: headers
                    .inner()
                    .get("timelimit")
                    .and_then(|v| match v {
                        AMQPValue::FieldArray(a) => {
                            let a = a.as_slice().to_vec();
                            if a.len() == 2 {
                                let soft = amqp_value_to_u32(&a[0]);
                                let hard = amqp_value_to_u32(&a[1]);
                                Some((soft, hard))
                            } else {
                                None
                            }
                        }
                        _ => None,
                    })
                    .unwrap_or((None, None)),
                argsrepr: get_header_str(headers, "argsrepr"),
                kwargsrepr: get_header_str(headers, "kwargsrepr"),
                origin: get_header_str(headers, "origin"),
            },
            raw_body: self.item.data.clone(),
        })
    }
}

fn get_header_str(headers: &FieldTable, key: &str) -> Option<String> {
    headers.inner().get(key).and_then(|v| match v {
        AMQPValue::ShortString(s) => Some(s.to_string()),
        AMQPValue::LongString(s) => Some(s.to_string()),
        _ => None,
    })
}

fn get_header_str_required(headers: &FieldTable, key: &str) -> Result<String, ProtocolError> {
    get_header_str(headers, key).ok_or_else(|| ProtocolError::MissingRequiredHeader(key.into()))
}

fn get_header_dt(headers: &FieldTable, key: &str) -> Option<DateTime<Utc>> {
    if let Some(s) = get_header_str(headers, key) {
        match DateTime::parse_from_rfc3339(&s) {
            Ok(dt) => Some(DateTime::<Utc>::from(dt)),
            _ => None,
        }
    } else {
        None
    }
}

fn get_header_u32(headers: &FieldTable, key: &str) -> Option<u32> {
    headers.inner().get(key).and_then(amqp_value_to_u32)
}

fn amqp_value_to_u32(v: &AMQPValue) -> Option<u32> {
    match v {
        AMQPValue::ShortShortInt(n) => Some(*n as u32),
        AMQPValue::ShortShortUInt(n) => Some(*n as u32),
        AMQPValue::ShortInt(n) => Some(*n as u32),
        AMQPValue::ShortUInt(n) => Some(*n as u32),
        AMQPValue::LongInt(n) => Some(*n as u32),
        AMQPValue::LongUInt(n) => Some(*n),
        AMQPValue::LongLongInt(n) => Some(*n as u32),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lapin::types::ShortString;
    use std::time::SystemTime;

    #[test]
    /// Tests conversion between Message -> Delivery -> Message.
    fn test_conversion() {
        let now = DateTime::<Utc>::from(SystemTime::now());

        // HACK: round this to milliseconds because that will happen during conversion
        // from message -> delivery.
        let now_str = now.to_rfc3339_opts(SecondsFormat::Millis, false);
        let now = DateTime::<Utc>::from(DateTime::parse_from_rfc3339(&now_str).unwrap());

        let message = Message {
            properties: MessageProperties {
                correlation_id: "aaa".into(),
                content_type: "application/json".into(),
                content_encoding: "utf-8".into(),
                reply_to: Some("bbb".into()),
                delivery_info: None,
            },
            headers: MessageHeaders {
                id: "aaa".into(),
                task: "add".into(),
                lang: Some("rust".into()),
                root_id: Some("aaa".into()),
                parent_id: Some("000".into()),
                group: Some("A".into()),
                meth: Some("method_name".into()),
                shadow: Some("add-these".into()),
                eta: Some(now),
                expires: Some(now),
                retries: Some(1),
                timelimit: (Some(30), Some(60)),
                argsrepr: Some("(1)".into()),
                kwargsrepr: Some("{'y': 2}".into()),
                origin: Some("gen123@piper".into()),
            },
            raw_body: vec![],
        };

        let delivery = Delivery {
            item: lapin::message::Delivery {
                delivery_tag: 0,
                exchange: ShortString::from(""),
                routing_key: ShortString::from("celery"),
                redelivered: false,
                properties: message.delivery_properties(),
                data: vec![],
                acker: Default::default(),
            },
            permit: None,
        };

        let message2 = delivery.try_deserialize_message();
        assert!(message2.is_ok());

        let message2 = message2.unwrap();
        assert_eq!(message, message2);
    }
}
