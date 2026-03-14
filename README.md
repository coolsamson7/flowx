# flowx

A lightweight, Kotlin-native **Saga orchestration engine** for Spring Boot applications — built for developers who want distributed transaction management without the operational overhead of a dedicated workflow platform.

---

## What is flowx?

flowx lets you define long-running, multi-step business processes as **code** — not XML, not YAML, not a visual designer. Each saga coordinates a sequence of steps, handles asynchronous events, compensates on failure, and survives JVM restarts.

```kotlin
val definition = saga<OrderSaga> {

    on<PlaceOrderCommand> { cmd ->
        customerName = cmd.customerName
        amount       = cmd.amount
    }

    branch {
        on({ amount > 1000.0 }) {
            step("manualApproval") {
                execute   { approvalService.requestApproval(customerName) }
                compensate { println("approval rolled back") }
                onSuccess<ApprovalGranted> { _ -> println("approved!") }
                onFailure<ApprovalDenied>  { e -> println("denied: ${e.reason}") }
                timeout(60_000)
            }
        }
        otherwise {
            step("autoApprove") {
                execute { println("auto-approved") }
            }
        }
    }

    parallel(join = Join.ALL) {
        step("payment") {
            execute   { paymentService.pay(amount) }
            compensate { paymentService.compensate() }
            onSuccess<PaymentConfirmed> { e -> txId = e.txId }
            onFailure<PaymentFailed>    { e -> println("payment failed: ${e.reason}") }
            timeout(30_000)
        }
        step("inventory") {
            execute   { inventoryService.reserve() }
            compensate { inventoryService.compensate() }
            condition { amount > 0.0 }
        }
    }

    step("shipping") {
        execute { println("shipping for $customerName (txId=$txId)") }
    }
}
```

---

## Features

- **Kotlin DSL** — sagas are pure Kotlin code, with full IDE support, refactoring, and type safety
- **Async/await steps** — steps can pause and wait for external events (webhooks, Kafka messages, manual approvals)
- **Automatic compensation** — failed sagas run compensation in strict reverse order
- **Parallel execution** — run steps concurrently with `Join.ALL` or `Join.ANY` semantics
- **Branching** — conditional paths evaluated at runtime against saga state
- **Durable** — saga state is persisted to a JPA database; sagas survive JVM restarts and resume where they left off
- **Cluster-safe** — pessimistic locking in the event store prevents duplicate delivery across multiple nodes
- **Spring Boot integration** — just `@Import(FlowxConfiguration::class)` and your sagas are Spring beans with full `@Autowired` support
- **Pluggable storage** — swap between in-memory (tests), JPA (default), or bring your own
- **Pluggable locking** — in-process `LocalSagaLock` out of the box, Redis-backed `RedisSagaLock` in the `flowx-redis` module
- **Timeout support** — async steps can declare a timeout; the engine compensates automatically on expiry

---

## How it compares

| | **flowx** | Temporal | Axon Framework | Spring State Machine | Conductor |
|---|---|---|---|---|---|
| **Definition style** | Kotlin DSL (code) | Code (Java/Go/TS) | Annotations + code | Java DSL / annotations | JSON / YAML |
| **External server required** | ❌ No | ✅ Yes | ❌ No | ❌ No | ✅ Yes |
| **Spring Boot native** | ✅ Yes | ⚠️ Integration needed | ✅ Yes | ✅ Yes | ⚠️ Integration needed |
| **Async event waiting** | ✅ Built-in | ✅ Built-in | ✅ Built-in | ⚠️ Limited | ✅ Built-in |
| **Automatic compensation** | ✅ Built-in | ✅ Built-in | ✅ Built-in | ❌ Manual | ⚠️ Limited |
| **Parallel steps** | ✅ Built-in | ✅ Built-in | ⚠️ Complex | ⚠️ Limited | ✅ Built-in |
| **Survives restarts** | ✅ JPA-backed | ✅ Yes | ✅ Event-sourced | ❌ No | ✅ Yes |
| **Operational overhead** | 🟢 None | 🔴 High | 🟡 Medium | 🟢 None | 🔴 High |
| **Learning curve** | 🟢 Low | 🟡 Medium | 🔴 High | 🟡 Medium | 🟡 Medium |
| **Library size** | 🟢 Minimal | 🔴 Large | 🔴 Large | 🟢 Small | 🔴 Large |

### Key advantages over alternatives

**vs Temporal / Conductor** — No separate server to deploy, operate, and monitor. flowx runs inside your Spring Boot application using your existing database. Zero new infrastructure.

**vs Axon Framework** — Axon is a full CQRS/Event Sourcing framework with significant architectural commitments. flowx is a single-purpose library you drop into any existing Spring Boot app in minutes.

**vs Spring State Machine** — State machines model states and transitions well but struggle with long-running async processes, compensation, and parallel execution. flowx is designed specifically for distributed transactions.

**vs Roll-your-own** — Managing saga state, compensation order, crash recovery, event deduplication, and cluster-safe locking yourself is weeks of work and a constant source of bugs. flowx handles all of it.

---

## Getting started

### 1. Add the dependency

```kotlin
// build.gradle.kts
implementation(project(":flowx:core"))
```

### 2. Import the configuration

```kotlin
@SpringBootApplication
@Import(FlowxConfiguration::class)
class MyApplication
```

### 3. Define a saga

```kotlin
@SagaType("my-saga")
@StartedBy(MyCommand::class)
class MySaga : AbstractSaga<MySaga>() {

    @Persisted var someField: String = ""

    @Autowired lateinit var myService: MyService

    override fun definition() = saga<MySaga> {
        on<MyCommand> { cmd -> someField = cmd.value }
        step("doWork") {
            execute   { myService.doSomething(someField) }
            compensate { myService.undo(someField) }
        }
    }
}
```

### 4. Start a saga

```kotlin
val sagaId = engine.send(MyCommand(value = "hello"))
engine.onComplete(sagaId) { success ->
    println(if (success) "Done!" else "Failed and compensated")
}
```

---

## Modules

| Module | Description |
|---|---|
| `common` | Shared interfaces: `Event`, `Command`, `Saga`, `SagaLock`, `TimeoutQueue`, DSL |
| `flowx/core` | Engine, JPA storage, Spring auto-configuration |
| `flowx/redis` | Redis-backed `SagaLock` and `TimeoutQueue` for multi-node deployments |

---

## Requirements

- Kotlin 2.1+
- Spring Boot 3.4+
- JDK 21+
- Any JPA-compatible database (H2, PostgreSQL, MySQL)

---

## License

Copyright © 2023 Andreas Ernst. All rights reserved.
