# Architecture

This document describes the architecture of the tt-signalr-client library.

## Overview

The library is structured into several layers, each with specific responsibilities:

```
┌─────────────────────────────────────────┐
│           Public API Layer              │
│    (SignalRClient, InvocationContext)   │
└─────────────────────────────────────────┘
                  │
┌─────────────────────────────────────────┐
│          Execution Layer                │
│  (Actions, Storage, Callbacks)          │
└─────────────────────────────────────────┘
                  │
┌─────────────────────────────────────────┐
│        Communication Layer              │
│   (WebSocket, HTTP Negotiation)         │
└─────────────────────────────────────────┘
                  │
┌─────────────────────────────────────────┐
│          Protocol Layer                 │
│    (Messages, Serialization)            │
└─────────────────────────────────────────┘
                  │
┌─────────────────────────────────────────┐
│         Completer Layer                 │
│   (Futures, Streams, Async)             │
└─────────────────────────────────────────┘
```

## Layer Details

### 1. Public API Layer (`src/client/`)

**Purpose**: Provides the main interface for users to interact with SignalR hubs.

**Key Components**:
- `SignalRClient`: Main client for hub connections
- `InvocationContext`: Context for callback handlers
- `ConnectionConfiguration`: Configuration builder

**Responsibilities**:
- Connect to SignalR servers
- Invoke hub methods
- Register callbacks
- Manage connection lifecycle

### 2. Execution Layer (`src/execution/`)

**Purpose**: Manages the execution of invocations, callbacks, and streaming operations.

**Key Components**:
- `UpdatableActionStorage`: Thread-safe storage for actions
- `InvocationAction`: Handles method invocations
- `EnumerableAction`: Handles streaming results
- `CallbackAction`: Handles hub-to-client callbacks
- `ArgumentConfiguration`: Builder for method arguments

**Responsibilities**:
- Store and manage pending invocations
- Match responses to invocations
- Execute callbacks when hub calls client
- Handle streaming data

### 3. Communication Layer (`src/communication/`)

**Purpose**: Handles low-level WebSocket and HTTP communication.

**Key Components**:
- `CommunicationClient`: WebSocket client using fastwebsockets
- `HttpClient`: HTTP client for negotiation
- `ConnectionData`: Connection information

**Responsibilities**:
- Negotiate connection with SignalR server
- Establish WebSocket connection
- Send and receive messages
- Handle connection lifecycle

**Technology**: 
- **fastwebsockets** with `unstable-split` feature for WebSocket
- **hyper** for HTTP negotiation

### 4. Protocol Layer (`src/protocol/`)

**Purpose**: Implements SignalR protocol message types and serialization.

**Key Components**:
- `MessageParser`: JSON serialization/deserialization
- `Invocation`: Method invocation messages
- `Completion`: Result messages
- `StreamItem`: Streaming data items
- `HandshakeRequest`: Connection handshake
- `Ping`: Keep-alive messages

**Responsibilities**:
- Serialize/deserialize SignalR messages
- Define message types
- Handle record separators

**Protocol**: SignalR JSON protocol with `\u{001E}` record separator

### 5. Completer Layer (`src/completer/`)

**Purpose**: Provides async primitives for manual completion of futures and streams.

**Key Components**:
- `ManualFuture`: Manually completable future
- `ManualFutureCompleter`: Completer for manual futures
- `ManualStream`: Manually controlled stream
- `ManualStreamCompleter`: Completer for manual streams
- `CompletedFuture`: Immediately ready future

**Responsibilities**:
- Bridge sync-to-async boundaries
- Allow manual completion of async operations
- Support streaming data patterns

## Data Flow

### Method Invocation

```
User Code
  │
  ├─→ SignalRClient.invoke()
  │     │
  │     ├─→ Create InvocationAction
  │     │     │
  │     │     └─→ Store in UpdatableActionStorage
  │     │
  │     ├─→ Serialize Invocation message
  │     │
  │     └─→ CommunicationClient.send()
  │           │
  │           └─→ WebSocket write_frame()
  │
  └─→ await ManualFuture
        │
        └─→ Completed when Completion message received
```

### Receiving Messages

```
WebSocket read_frame()
  │
  ├─→ Parse message type
  │
  ├─→ UpdatableActionStorage.process_message()
  │     │
  │     ├─→ Match invocation_id
  │     │
  │     └─→ InvocationAction.update_with()
  │           │
  │           └─→ ManualFutureCompleter.complete()
  │                 │
  │                 └─→ Wakes awaiting future
  │
  └─→ User receives result
```

### Streaming

```
User Code
  │
  ├─→ SignalRClient.enumerate()
  │     │
  │     ├─→ Create EnumerableAction
  │     │     │
  │     │     └─→ Store in UpdatableActionStorage
  │     │
  │     ├─→ Serialize StreamInvocation
  │     │
  │     └─→ CommunicationClient.send()
  │
  └─→ Stream items arrive
        │
        ├─→ StreamItem messages
        │     │
        │     └─→ ManualStreamCompleter.push()
        │
        ├─→ Completion message
        │     │
        │     └─→ ManualStreamCompleter.close()
        │
        └─→ User iterates stream
```

## Thread Safety

The library uses a mix of strategies for thread safety:

- **UpdatableActionStorage**: Uses `Arc<Mutex<HashMap>>` for thread-safe storage of pending invocations and callbacks
- **CommunicationConnection**: Uses channels (`UnboundedSender`/`UnboundedReceiver`) for message passing between tasks
- **WebSocket Splitting**: Uses fastwebsockets' `unstable-split` feature to split the WebSocket into independent reader and writer halves
  - Reader task: Handles incoming frames independently
  - Writer task: Handles outgoing messages via channels
- **ManualFuture/ManualStream**: Use `Arc<Mutex<State>>` for internal state management

## Key Design Decisions

1. **No WASM Support**: Focused on native targets only, using tokio and fastwebsockets
2. **Manual Futures**: Custom future implementation for sync-to-async bridges
3. **FastWebSockets**: Uses fastwebsockets for high-performance WebSocket communication
4. **Unstable Split**: Uses the `unstable-split` feature to split WebSocket into separate reader/writer
   - Reader and writer run in independent tokio tasks for true concurrency
   - Communication between tasks via unbounded channels
   - No mutex on WebSocket itself - each half is owned by its task
5. **Channel-Based Communication**: Uses tokio channels for passing messages between components
6. **Thread-Safe Storage**: Action storage wrapped in Arc<Mutex<>> for cloneable, thread-safe access
7. **Message Matching**: Uses invocation_id to match requests with responses

## Concurrency Model

The WebSocket communication layer uses a split architecture:

```
┌────────────────────────────────────────────────┐
│         CommunicationConnection                │
├────────────────────────────────────────────────┤
│                                                │
│  ┌──────────────┐         ┌─────────────────┐ │
│  │ Reader Task  │         │  Writer Task    │ │
│  │              │         │                 │ │
│  │ ws_reader    │         │  ws_writer      │ │
│  │ read_frame() │         │  write_frame()  │ │
│  └──────┬───────┘         └────────▲────────┘ │
│         │                          │          │
│         │                          │          │
│         │ (incoming)    (outgoing) │          │
│         │                          │          │
│         ▼                          │          │
│   ┌──────────┐         ┌───────────┴────┐    │
│   │ Storage  │         │ Channel (tx)   │    │
│   │ Process  │         │ UnboundedSender│    │
│   └──────────┘         └────────────────┘    │
│                                                │
│  Automatic Frames (pongs) via channel         │
│  Reader → Writer                               │
└────────────────────────────────────────────────┘
```

Benefits:
- **True Concurrency**: Reader and writer don't block each other
- **No Contention**: No mutex locks on the WebSocket
- **Automatic Responses**: Reader can send pongs through writer via channel
- **Clean Separation**: Each task has clear responsibilities

## Performance Considerations

- **FastWebSockets**: Chosen for its performance characteristics
- **Minimal Copying**: Uses borrowed payloads where possible
- **Async I/O**: All I/O is async using tokio
- **Shared State**: Cloning clients shares underlying connection via Arc
- **Stream Buffering**: ManualStream uses VecDeque for buffering
- **Lock-Free WebSocket**: Reader and writer run concurrently without mutex contention
- **Channel-Based**: Uses unbounded channels for efficient message passing between tasks

## Extension Points

To extend the library:

1. **Add Message Types**: Implement new message types in `src/protocol/`
2. **Add Actions**: Create new action types in `src/execution/`
3. **Custom Transports**: Implement `Communication` trait for new transports
4. **Custom Serialization**: Modify `MessageParser` for different formats
